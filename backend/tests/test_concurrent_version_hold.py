"""concurrent_version_hold 双架构核对。

全局只贯彻一种口径，但仍分别按两架构核对正确性，故用 settings.conflict_mode
在同一份实现里切换：hold = 甲·冲突入搁置；rollback = 乙·冲突整票回滚。

四个必查场景（甲乙同口径处 + 分叉处）：
  1. 同键交错：一次成功一次冲突；正式链仅一段当前。
  2. 甲有搁置行；乙无搁置行但有冲突回执。
  3. 邻键可并行（不被本键租约误伤）。
  4. 先到完成后第三者才能写；且先到完成必须释放租约。

另外核证：先到未释租时第三者不得写入；无锁并行写 hist 被禁；后到不覆盖先到。
"""

import json

import pytest
from fastapi.testclient import TestClient

from app.db import connect
from app.main import app
from app.seed import row_hash


@pytest.fixture()
def client(tmp_path, monkeypatch):
    import app.db as db

    data = tmp_path / "data"
    monkeypatch.setattr(db, "DATA_DIR", data)
    monkeypatch.setattr(db, "DB_PATH", data / "hourspan.db")
    monkeypatch.setattr(db, "FIXTURES_DIR", __import__("pathlib").Path("/workspace/fixtures"))
    monkeypatch.setenv("DATA_DIR", str(data))
    # 每个用例一份干净库；startup 会 init_db 建表 + 播种。
    with TestClient(app) as c:
        yield c


def _mode(client, mode):
    from app.modules.concurrent_version_hold import service as hold

    hold.set_mode(mode)


def _ingest(client, snaps):
    """直接落快照（走业务服务以外的入档语义同形），返回 fixture 批号。"""
    conn = connect()
    try:
        from uuid import uuid4

        from app.seed import now_iso

        batch_id = f"B-{uuid4().hex[:8]}"
        conn.execute(
            "INSERT INTO ingest_batches(id, created_at, report_json) VALUES (?,?,?)",
            (batch_id, now_iso(), json.dumps({"t": True})),
        )
        for s in snaps:
            rh = row_hash(s.get("open_time"), s.get("close_time"),
                          s.get("weekday_mask", "1111100"), s.get("biz_status", "open"))
            conn.execute(
                """INSERT INTO source_snapshots(
                  id, batch_id, store_id, as_of_ts, open_time, close_time,
                  weekday_mask, biz_status, payload_json, row_hash
                ) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (s["id"], batch_id, s["store_id"], s["as_of_ts"], s.get("open_time"),
                 s.get("close_time"), s.get("weekday_mask", "1111100"),
                 s.get("biz_status", "open"), "{}", rh),
            )
        conn.commit()
        return batch_id
    finally:
        conn.close()


def _snap(sid, store, as_of, ot, ct, status="open", mask="1111100"):
    return {"id": sid, "store_id": store, "as_of_ts": as_of, "open_time": ot,
            "close_time": ct, "biz_status": status, "weekday_mask": mask}


def _detect(client, batch_id):
    r = client.post("/api/detect/run", json={"batch_id": batch_id})
    assert r.status_code == 200, r.text
    return r.json()


def _apply(client, run_id):
    r = client.post("/api/scd/apply", json={"detect_run_id": run_id})
    assert r.status_code == 200, r.text
    return r.json()


def _proof(client, store_id=None):
    q = f"?store_id={store_id}" if store_id else ""
    return client.get(f"/api/concurrency/proof{q}").json()


def _key(client, store_id):
    p = _proof(client, store_id)
    assert p["keys"], f"no key proof for {store_id}"
    return p["keys"][0]


def _hist(store):
    conn = connect()
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM dim_store_hours_hist WHERE store_id=? ORDER BY valid_from, id",
            (store,)).fetchall()]
    finally:
        conn.close()


def _current_count(store):
    conn = connect()
    try:
        return conn.execute(
            "SELECT COUNT(*) AS c FROM dim_store_hours_hist WHERE store_id=? AND is_current=1",
            (store,)).fetchone()["c"]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 甲：冲突入搁置
# ---------------------------------------------------------------------------
def test_jia_same_key_interleave_one_wins_one_held(client):
    _mode(client, "hold")
    from app.modules.concurrent_version_hold import service as hold

    b1 = _ingest(client, [_snap("S1-A", "ST-EAST-01", "2026-11-20T00:00:00Z", "10:00", "22:00")])
    d1 = _detect(client, b1)                      # 先到（未应用 = 未提交草稿）
    b2 = _ingest(client, [_snap("S1-B", "ST-EAST-01", "2026-11-21T00:00:00Z", "11:00", "23:00")])
    d2 = _detect(client, b2)                      # 后到

    before_hash = _key(client, "ST-EAST-01")["is_current"]["row_hash"]

    # 先到未释租：后到必须被挡、落搁置、不得写正式维（"先到未释租时第三者仍写入"必须禁止）
    conn = hold.connect_immediate()
    try:
        conn.execute("BEGIN IMMEDIATE")
        hold.claim_lease(conn, "ST-EAST-01", d1["detect_run_id"])
        conn.commit()
    finally:
        conn.close()
    blocked = _apply(client, d2["detect_run_id"])
    assert blocked["applied_count"] == 0
    assert blocked["held_count"] == 1
    assert {h["reason"] for h in blocked["held"]} & {"pending_earlier_run", "lease_held_by_other"}
    assert _key(client, "ST-EAST-01")["is_current"]["row_hash"] == before_hash

    # 先到完成（应用成功），正式链推进且只一段当前
    r1 = _apply(client, d1["detect_run_id"])
    assert r1["outcome"] == "applied"
    assert r1["applied_count"] == 1
    assert r1["held_count"] == 0
    after_first = _key(client, "ST-EAST-01")["is_current"]["row_hash"]
    assert after_first != before_hash
    assert _current_count("ST-EAST-01") == 1
    assert len(_hist("ST-EAST-01")) == 2  # seed + 先到一段


def test_jia_later_pending_blocked_and_run_closed(client):
    """甲：后到在"先到的未提交跑次仍 open"时落搁置并终结该跑次；
    后续要写须重跑检测（新跑次、新基准）。"""
    _mode(client, "hold")
    b1 = _ingest(client, [_snap("S1b-A", "ST-EAST-01", "2026-11-20T00:00:00Z", "10:00", "22:00")])
    d1 = _detect(client, b1)
    b2 = _ingest(client, [_snap("S1b-B", "ST-EAST-01", "2026-11-21T00:00:00Z", "11:00", "23:00")])
    d2 = _detect(client, b2)

    r2 = _apply(client, d2["detect_run_id"])      # 先到仍 open
    assert r2["outcome"] == "conflict_hold"
    assert r2["held"][0]["reason"] == "pending_earlier_run"
    assert r2["held"][0]["blocking_run"] == d1["detect_run_id"]
    assert _current_count("ST-EAST-01") == 1

    # 搁置是终态：该跑次已关闭，重放返回原回执（不再写正式维）
    replay = _apply(client, d2["detect_run_id"])
    assert replay["outcome"] == "conflict_hold"
    assert replay["replayed"] is True
    assert _current_count("ST-EAST-01") == 1

    r1 = _apply(client, d1["detect_run_id"])      # 先到完成
    assert r1["outcome"] == "applied"
    # 先到完成后租约已释放
    assert client.get("/api/concurrency/leases").json() == []
    assert _current_count("ST-EAST-01") == 1

    # 第三者（新跑次、新基准）在先到完成后可写
    b3 = _ingest(client, [_snap("S1b-C", "ST-EAST-01", "2026-11-22T00:00:00Z", "11:00", "23:00")])
    d3 = _detect(client, b3)
    r3 = _apply(client, d3["detect_run_id"])
    assert r3["outcome"] == "applied"
    assert _current_count("ST-EAST-01") == 1


def test_jia_hold_row_has_key_run_pair_and_snapshot(client):
    _mode(client, "hold")
    b1 = _ingest(client, [_snap("S2-A", "ST-WEST-01", "2026-11-20T00:00:00Z", "08:00", "20:00")])
    d1 = _detect(client, b1)
    b2 = _ingest(client, [_snap("S2-B", "ST-WEST-01", "2026-11-21T00:00:00Z", "07:00", "19:00")])
    d2 = _detect(client, b2)
    before = _key(client, "ST-WEST-01")["is_current"]

    r2 = _apply(client, d2["detect_run_id"])      # 先到仍 open：后到落搁置
    assert r2["outcome"] == "conflict_hold"

    holds = client.get("/api/concurrency/holds?store_id=ST-WEST-01").json()
    assert len(holds) == 1
    h = holds[0]
    assert h["store_id"] == "ST-WEST-01"
    assert h["conflict_run"] == d2["detect_run_id"]
    assert h["blocking_run"] == d1["detect_run_id"]
    snap = h["snapshot_at_conflict"]
    # 当时 is_current 快照：键 + 冲突 run 对 + 当时 is_current
    assert snap["is_current_count"] == 1
    assert snap["hist_id"] == before["hist_ids"][0]
    assert snap["row_hash"] == before["row_hash"]

    p = _proof(client, "ST-WEST-01")
    key = p["keys"][0]
    assert key["conflicts"] and key["conflict_evidence_complete"] is True
    assert key["official_double_open_with_hold"] is False
    assert p["ok"] is True


def test_jia_adjacent_key_parallel_not_hurt(client):
    _mode(client, "hold")
    b1 = _ingest(client, [_snap("S3-A", "ST-EAST-01", "2026-11-20T00:00:00Z", "10:00", "22:00")])
    d1 = _detect(client, b1)
    b2 = _ingest(client, [_snap("S3-B", "ST-EAST-02", "2026-11-20T00:00:00Z", "09:00", "21:00")])
    d2 = _detect(client, b2)

    # 两个不同键并行：各自独立租约，互不误伤
    r1 = _apply(client, d1["detect_run_id"])
    r2 = _apply(client, d2["detect_run_id"])
    assert r1["outcome"] == "applied"
    assert r2["outcome"] == "applied"
    assert _current_count("ST-EAST-01") == 1
    assert _current_count("ST-EAST-02") == 1
    p = _proof(client)
    assert p["ok"] is True


def test_jia_third_writer_blocked_by_unreleased_lease(client):
    _mode(client, "hold")
    from app.modules.concurrent_version_hold import service as hold

    b1 = _ingest(client, [_snap("S4-A", "ST-SOUTH-01", "2026-11-20T00:00:00Z", "12:00", "23:00")])
    d1 = _detect(client, b1)
    # 人为把先到租约挂住（模拟先到写到一半未释租）
    conn = hold.connect_immediate()
    try:
        conn.execute("BEGIN IMMEDIATE")
        hold.claim_lease(conn, "ST-SOUTH-01", d1["detect_run_id"])
        conn.commit()
    finally:
        conn.close()
    b2 = _ingest(client, [_snap("S4-B", "ST-SOUTH-01", "2026-11-21T00:00:00Z", "13:00", "23:30")])
    d2 = _detect(client, b2)
    r2 = _apply(client, d2["detect_run_id"])
    assert r2["outcome"] == "conflict_hold"
    assert {h["reason"] for h in r2["held"]} & {"lease_held_by_other", "pending_earlier_run"}
    assert _current_count("ST-SOUTH-01") == 1


def test_jia_third_writer_after_first_completes(client):
    _mode(client, "hold")
    b1 = _ingest(client, [_snap("S5-A", "ST-NORTH-01", "2026-11-20T00:00:00Z", "09:30", "18:30")])
    d1 = _detect(client, b1)
    _apply(client, d1["detect_run_id"])
    # 先到完成后必须释放租约
    assert client.get("/api/concurrency/leases").json() == []

    b2 = _ingest(client, [_snap("S5-B", "ST-NORTH-01", "2026-11-21T00:00:00Z", "08:30", "17:30")])
    d2 = _detect(client, b2)
    r2 = _apply(client, d2["detect_run_id"])
    assert r2["outcome"] == "applied"
    assert _current_count("ST-NORTH-01") == 1
    hists = _hist("ST-NORTH-01")
    # 正式链：seed -> 第三段（先到已让位），无覆盖
    assert len(hists) == 3
    assert sum(1 for h in hists if h["is_current"] == 1) == 1


def test_jia_late_snapshot_cannot_cover_earlier_segment(client):
    _mode(client, "hold")
    b1 = _ingest(client, [_snap("S6-A", "ST-EAST-01", "2026-11-20T00:00:00Z", "10:00", "22:00")])
    d1 = _detect(client, b1)
    _apply(client, d1["detect_run_id"])
    before = _key(client, "ST-EAST-01")["is_current"]["row_hash"]

    # 后到晚到但 as_of 更早：不得覆盖先到正式段
    b2 = _ingest(client, [_snap("S6-B", "ST-EAST-01", "2026-11-10T00:00:00Z", "11:00", "23:00")])
    d2 = _detect(client, b2)
    r2 = _apply(client, d2["detect_run_id"])
    assert r2["applied_count"] == 0
    assert r2["held_count"] == 1
    assert r2["held"][0]["reason"] == "late_snapshot_not_forward"
    assert _key(client, "ST-EAST-01")["is_current"]["row_hash"] == before


# ---------------------------------------------------------------------------
# 乙：冲突整票回滚
# ---------------------------------------------------------------------------
def test_yi_same_key_interleave_one_wins_one_rollback(client):
    _mode(client, "rollback")
    from app.modules.concurrent_version_hold import service as hold

    b1 = _ingest(client, [_snap("Y1-A", "ST-EAST-01", "2026-11-20T00:00:00Z", "10:00", "22:00")])
    d1 = _detect(client, b1)
    b2 = _ingest(client, [_snap("Y1-B", "ST-EAST-01", "2026-11-21T00:00:00Z", "11:00", "23:00")])
    d2 = _detect(client, b2)
    before_hash = _key(client, "ST-EAST-01")["is_current"]["row_hash"]

    # 先到未释租：后到整票回滚，不出正式段
    conn = hold.connect_immediate()
    try:
        conn.execute("BEGIN IMMEDIATE")
        hold.claim_lease(conn, "ST-EAST-01", d1["detect_run_id"])
        conn.commit()
    finally:
        conn.close()
    r2 = _apply(client, d2["detect_run_id"])
    assert r2["outcome"] == "conflict_rollback"
    assert r2["rolled_back"] is True
    assert r2["applied_count"] == 0
    assert r2["held_count"] == 0
    assert r2["conflict_run_pairs"]
    pair = r2["conflict_run_pairs"][0]
    assert pair["conflict_run"] == d2["detect_run_id"]
    assert pair["blocking_run"] == d1["detect_run_id"]
    assert r2["conflicts"][0]["snapshot_at_conflict"]["is_current_count"] == 1
    assert _key(client, "ST-EAST-01")["is_current"]["row_hash"] == before_hash

    # 先到成功；正式链仅一段当前
    r1 = _apply(client, d1["detect_run_id"])
    assert r1["outcome"] == "applied"
    assert _current_count("ST-EAST-01") == 1


def test_yi_no_hold_rows_but_conflict_receipt(client):
    _mode(client, "rollback")
    b1 = _ingest(client, [_snap("Y2-A", "ST-WEST-01", "2026-11-20T00:00:00Z", "08:00", "20:00")])
    d1 = _detect(client, b1)
    b2 = _ingest(client, [_snap("Y2-B", "ST-WEST-01", "2026-11-21T00:00:00Z", "07:00", "19:00")])
    d2 = _detect(client, b2)

    r2 = _apply(client, d2["detect_run_id"])      # 先到仍 open：后到整票回滚
    assert r2["outcome"] == "conflict_rollback"

    # 乙口径：核证无搁置行
    holds = client.get("/api/concurrency/holds?store_id=ST-WEST-01").json()
    assert holds == []

    p = _proof(client, "ST-WEST-01")
    assert p["architecture"] == "rollback"
    assert p["holds"] == []
    assert p["conflict_evidence"]["source"] == "apply_receipts"
    assert p["conflict_evidence"]["rows"]
    key = p["keys"][0]
    assert key["ok"] is True
    # 冲突证据来自回执
    assert any(c["conflict_run"] == d2["detect_run_id"] for c in key["conflicts"])


def test_yi_adjacent_key_parallel(client):
    _mode(client, "rollback")
    b1 = _ingest(client, [_snap("Y3-A", "ST-EAST-01", "2026-11-20T00:00:00Z", "10:00", "22:00")])
    d1 = _detect(client, b1)
    b2 = _ingest(client, [_snap("Y3-B", "ST-EAST-02", "2026-11-20T00:00:00Z", "09:00", "21:00")])
    d2 = _detect(client, b2)
    r1 = _apply(client, d1["detect_run_id"])
    r2 = _apply(client, d2["detect_run_id"])
    assert r1["outcome"] == "applied"
    assert r2["outcome"] == "applied"
    assert _proof(client)["ok"] is True


def test_yi_third_writer_after_first_completes(client):
    _mode(client, "rollback")
    b1 = _ingest(client, [_snap("Y4-A", "ST-NORTH-01", "2026-11-20T00:00:00Z", "09:30", "18:30")])
    d1 = _detect(client, b1)
    _apply(client, d1["detect_run_id"])
    assert client.get("/api/concurrency/leases").json() == []
    b2 = _ingest(client, [_snap("Y4-B", "ST-NORTH-01", "2026-11-21T00:00:00Z", "08:30", "17:30")])
    d2 = _detect(client, b2)
    r2 = _apply(client, d2["detect_run_id"])
    assert r2["outcome"] == "applied"
    assert _current_count("ST-NORTH-01") == 1


def test_yi_rollback_run_is_terminal_and_fresh_run_can_write(client):
    _mode(client, "rollback")
    b1 = _ingest(client, [_snap("Y5-A", "ST-SOUTH-01", "2026-11-20T00:00:00Z", "12:00", "23:00")])
    d1 = _detect(client, b1)
    b2 = _ingest(client, [_snap("Y5-B", "ST-SOUTH-01", "2026-11-21T00:00:00Z", "13:00", "23:30")])
    d2 = _detect(client, b2)
    blocked = _apply(client, d2["detect_run_id"])     # 先到仍 open -> 整票回滚
    assert blocked["outcome"] == "conflict_rollback"

    # 乙口径：冲突是终态，跑次关闭、无搁置行；不得卡住后续写入
    runs = client.get("/api/detect-runs").json()
    row = next(r for r in runs if r["id"] == d2["detect_run_id"])
    assert row["status"] == "conflict_rollback"
    assert client.get("/api/concurrency/holds?store_id=ST-SOUTH-01").json() == []
    assert client.get("/api/concurrency/leases").json() == []

    r1 = _apply(client, d1["detect_run_id"])          # 先到完成
    assert r1["outcome"] == "applied"
    assert _current_count("ST-SOUTH-01") == 1

    # 第三者（新跑次、新基准）在先到完成后可写
    b3 = _ingest(client, [_snap("Y5-C", "ST-SOUTH-01", "2026-11-22T00:00:00Z", "13:00", "23:30")])
    d3 = _detect(client, b3)
    assert _apply(client, d3["detect_run_id"])["outcome"] == "applied"
    assert _current_count("ST-SOUTH-01") == 1


# ---------------------------------------------------------------------------
# 通用不变量
# ---------------------------------------------------------------------------
def test_unique_current_index_enforced(client):
    conn = connect()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key='hist_current_unique_index'"
        ).fetchone()
        assert row["value"] == "ok"
        import sqlite3

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO dim_store_hours_hist(
                  id, store_id, open_time, close_time, weekday_mask, biz_status,
                  valid_from, valid_to, is_current, source_snapshot_id, row_hash, closed_by_run
                ) VALUES ('H-DUP','ST-EAST-01','09:00','21:00','1111100','open',
                          '2030-01-01T00:00:00Z',NULL,1,NULL,'deadbeef',NULL)"""
            )
            conn.commit()
    finally:
        conn.close()


def test_proof_endpoint_read_only(client):
    _mode(client, "hold")
    p = _proof(client)
    assert p["read_only"] is True
    assert set(["holder_run", "is_current", "lease", "violations"]).issubset(
        set(p["keys"][0].keys())
    )
    assert p["ok"] is True


# ---------------------------------------------------------------------------
# 真并发：多线程同时写同键 / 邻键
# ---------------------------------------------------------------------------
def test_concurrent_threads_same_key_single_current(client):
    """两个线程同时 apply 同键的两个跑次：一个成功、一个冲突/回滚；
    无论交错顺序，正式链恒为单段当前。"""
    import threading
    from app.modules.concurrent_version_hold import service as hold

    _mode(client, "hold")
    b1 = _ingest(client, [_snap("C1-A", "ST-EAST-01", "2026-11-20T00:00:00Z", "10:00", "22:00")])
    d1 = _detect(client, b1)
    b2 = _ingest(client, [_snap("C1-B", "ST-EAST-01", "2026-11-21T00:00:00Z", "11:00", "23:00")])
    d2 = _detect(client, b2)

    results = {}

    def worker(tag, run_id):
        results[tag] = _apply(client, run_id)

    # 两台"检测"同时提交；lease + 闸门应保证只有一段正式当前段。
    t1 = threading.Thread(target=worker, args=("a", d1["detect_run_id"]))
    t2 = threading.Thread(target=worker, args=("b", d2["detect_run_id"]))
    t1.start(); t2.start(); t1.join(); t2.join()

    outcomes = sorted([results["a"]["outcome"], results["b"]["outcome"]])
    # 由于先到草稿闸门与租约，至少一个不为 applied；不允许两个都 applied 且都写正式
    applied_total = results["a"]["applied_count"] + results["b"]["applied_count"]
    assert applied_total <= 1, outcomes
    assert _current_count("ST-EAST-01") == 1
    assert hold.build_proof(connect())["ok"] is True


def test_concurrent_threads_adjacent_keys_both_proceed(client):
    _mode(client, "hold")
    b1 = _ingest(client, [_snap("C2-A", "ST-EAST-01", "2026-11-20T00:00:00Z", "10:00", "22:00")])
    d1 = _detect(client, b1)
    b2 = _ingest(client, [_snap("C2-B", "ST-EAST-02", "2026-11-20T00:00:00Z", "09:00", "21:00")])
    d2 = _detect(client, b2)

    results = {}
    import threading

    def worker(tag, run_id):
        results[tag] = _apply(client, run_id)

    t1 = threading.Thread(target=worker, args=("a", d1["detect_run_id"]))
    t2 = threading.Thread(target=worker, args=("b", d2["detect_run_id"]))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert results["a"]["outcome"] == "applied"
    assert results["b"]["outcome"] == "applied"
    assert _current_count("ST-EAST-01") == 1
    assert _current_count("ST-EAST-02") == 1


def test_proof_exposes_holder_run_while_lease_held(client):
    """租约持有时，只读核证必须可见 holder_run 与租约态（供冲突页判读）。"""
    _mode(client, "hold")
    from app.modules.concurrent_version_hold import service as hold

    b1 = _ingest(client, [_snap("P1-A", "ST-EAST-01", "2026-11-20T00:00:00Z", "10:00", "22:00")])
    d1 = _detect(client, b1)
    conn = hold.connect_immediate()
    try:
        conn.execute("BEGIN IMMEDIATE")
        hold.claim_lease(conn, "ST-EAST-01", d1["detect_run_id"])
        conn.commit()
    finally:
        conn.close()

    key = _key(client, "ST-EAST-01")
    assert key["holder_run"] == d1["detect_run_id"]
    assert key["lease_state"] == "held"
    assert key["lease"]["holder_run"] == d1["detect_run_id"]
    # is_current 唯一性、租约态、冲突证据均在
    assert key["is_current_unique"] is True
    assert "conflict_evidence_complete" in key
