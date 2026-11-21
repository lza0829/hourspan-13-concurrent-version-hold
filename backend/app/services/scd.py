import json
from uuid import uuid4

from app.db import connect, connect_immediate
from app.modules.concurrent_version_hold import service as hold
from app.seed import now_iso

# 全局唯一架构：甲 —— 冲突入搁置（见 concurrent_version_hold.service.POLICY）。
# 正式维写入只在"已持租 + 无更早未提交跑次 + 基准未漂移"时发生；
# 任一不满足则落 version_holds 搁置行，正式维与当前镜不动。


def _store_ids_in_changes(changes) -> list[str]:
    seen, order = set(), []
    for ch in changes:
        sid = ch.get("store_id")
        if sid and sid not in seen:
            seen.add(sid)
            order.append(sid)
    return order


def _load_snapshot(conn, snapshot_id: str):
    if not snapshot_id:
        return None
    return conn.execute(
        "SELECT * FROM source_snapshots WHERE id=?", (snapshot_id,)
    ).fetchone()


def _write_official(conn, run_id: str, store_id: str, snap, snapshot_at_write: dict) -> dict:
    """在持租事务内开闭正式维；调用方保证无更早未提交跑次且基准未漂移。"""
    cur_hist = conn.execute(
        "SELECT * FROM dim_store_hours_hist WHERE store_id=? AND is_current=1",
        (store_id,),
    ).fetchone()
    if cur_hist and cur_hist["valid_from"] and cur_hist["valid_from"] >= snap["as_of_ts"]:
        # 区间互盖防护：新段必须严格晚于当前段起点
        raise ValueError("late_snapshot_not_forward")
    valid_from = snap["as_of_ts"]
    if cur_hist:
        conn.execute(
            """UPDATE dim_store_hours_hist
               SET valid_to=?, is_current=0, closed_by_run=?
               WHERE id=?""",
            (valid_from, run_id, cur_hist["id"]),
        )
    new_id = f"HIST-{uuid4().hex[:10]}"
    conn.execute(
        """INSERT INTO dim_store_hours_hist(
          id, store_id, open_time, close_time, weekday_mask, biz_status,
          valid_from, valid_to, is_current, source_snapshot_id, row_hash, closed_by_run
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            new_id,
            store_id,
            snap["open_time"],
            snap["close_time"],
            snap["weekday_mask"],
            snap["biz_status"],
            valid_from,
            None,
            1,
            snap["id"],
            snap["row_hash"],
            None,
        ),
    )
    conn.execute(
        """INSERT INTO dim_store_hours_current(
          store_id, open_time, close_time, weekday_mask, biz_status,
          hist_id, as_of_ts, row_hash
        ) VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(store_id) DO UPDATE SET
          open_time=excluded.open_time,
          close_time=excluded.close_time,
          weekday_mask=excluded.weekday_mask,
          biz_status=excluded.biz_status,
          hist_id=excluded.hist_id,
          as_of_ts=excluded.as_of_ts,
          row_hash=excluded.row_hash
        """,
        (
            store_id,
            snap["open_time"],
            snap["close_time"],
            snap["weekday_mask"],
            snap["biz_status"],
            new_id,
            valid_from,
            snap["row_hash"],
        ),
    )
    return {
        "store_id": store_id,
        "hist_id": new_id,
        "closed": cur_hist["id"] if cur_hist else None,
        "snapshot_id": snap["id"],
        "row_hash": snap["row_hash"],
        "valid_from": valid_from,
        "based_on": {
            "hist_id": snapshot_at_write.get("hist_id"),
            "row_hash": snapshot_at_write.get("row_hash"),
        },
    }


def _hold_and_commit(
    conn, store_id, run_id, reason, blocking_run, draft, snap, release_lease=False
):
    snap_before = hold.conflict_snapshot(conn, store_id)
    if reason == "lease_held_by_other":
        holder = blocking_run
    elif reason == "pending_earlier_run":
        # 先到是草稿挡路，未必持租；holder 以实际租约态为准。
        lease = hold.lease_state(conn, store_id)
        holder = lease["holder_run"] if lease and lease["active"] else None
    else:
        holder = run_id
    hold_id, _ = hold.record_hold(
        conn,
        store_id=store_id,
        conflict_run=run_id,
        reason=reason,
        blocking_run=blocking_run,
        holder_run=holder,
        base={
            "hist_id": draft["base_hist_id"] if draft else None,
            "row_hash": draft["base_hash"] if draft else None,
            "as_of_ts": draft["base_as_of_ts"] if draft else None,
        },
        snapshot=snap_before,
        attempt={"snapshot_id": snap["id"], "to_hash": snap["row_hash"]},
    )
    # 若本跑次已持该键租约，必须在同一事务内释放，避免留下悬挂租约。
    if release_lease:
        hold.release_lease(conn, store_id, run_id)
    conn.commit()
    return {
        "store_id": store_id,
        "hold_id": hold_id,
        "run_id": run_id,
        "blocking_run": blocking_run,
        "holder_run": holder,
        "reason": reason,
        "snapshot_at_conflict": snap_before,
        "official_unchanged": True,
    }


def _is_replayable(receipt: dict | None) -> bool:
    """已终结的跑次重放原回执（甲搁置 / 乙回滚都是终态）；
    乙的回滚回执不重放，但随后会被 run_closed 拦截。"""
    if not receipt:
        return False
    if receipt.get("outcome") == "conflict_rollback":
        return False
    return True


class _RaceConflict(Exception):
    """持锁阶段复查发现的冲突；携带完整冲突证据供乙口径回执。"""

    def __init__(self, store_id, reason, blocking_run, draft, snap, snapshot):
        super().__init__(f"{reason}:{store_id}")
        self.store_id = store_id
        self.reason = reason
        self.blocking_run = blocking_run
        self.draft = draft
        self.snap = snap
        self.snapshot = snapshot


def apply_type2(detect_run_id: str):
    """按当前全局口径应用检测跑次；全局只允许一种口径生效。"""
    conn = connect_immediate()
    try:
        existing = hold.load_receipt(conn, detect_run_id)
        if _is_replayable(existing):
            existing = dict(existing)
            existing["replayed"] = True
            return existing

        mode = hold.active_mode(conn)
        if mode == hold.MODE_ROLLBACK:
            return _apply_rollback(conn, detect_run_id)

        conn.execute("BEGIN IMMEDIATE")
        run = conn.execute(
            "SELECT * FROM detect_runs WHERE id=?", (detect_run_id,)
        ).fetchone()
        if not run:
            conn.rollback()
            return {"ok": False, "outcome": "not_found", "message": "检测跑次不存在"}
        if run["status"] != "open":
            conn.rollback()
            return {
                "ok": False,
                "outcome": "run_closed",
                "detect_run_id": detect_run_id,
                "status": run["status"],
                "message": "跑次已关闭（已应用或已放弃），不可重复应用",
            }
        report = json.loads(run["report_json"] or "{}")
        changes = report.get("changes", [])
        change_store_ids = _store_ids_in_changes(changes)
        run_seq = run["seq"]
        draft_rows = {
            r["store_id"]: dict(r)
            for r in conn.execute(
                "SELECT * FROM version_drafts WHERE run_id=?", (detect_run_id,)
            ).fetchall()
        }
        missing = [sid for sid in change_store_ids if sid not in draft_rows]
        conn.commit()
        if missing:
            return {
                "ok": False,
                "outcome": "draft_missing",
                "detect_run_id": detect_run_id,
                "missing_store_ids": missing,
                "message": "检测跑次缺少草稿基准，请重跑检测后再应用",
            }
        if change_store_ids and run_seq is None:
            # 兜底：老库无 seq 时补一个单调序号，保证先到/后到可比
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS m FROM detect_runs"
            ).fetchone()
            run_seq = int(row["m"]) + 1
            conn.execute(
                "UPDATE detect_runs SET seq=? WHERE id=? AND seq IS NULL",
                (run_seq, detect_run_id),
            )
            conn.commit()

        applied: list[dict] = []
        held: list[dict] = []
        for ch in changes:
            store_id = ch.get("store_id")
            if not store_id:
                continue
            snap = _load_snapshot(conn, ch.get("snapshot_id"))
            if not snap:
                continue
            draft = conn.execute(
                "SELECT * FROM version_drafts WHERE run_id=? AND store_id=?",
                (detect_run_id, store_id),
            ).fetchone()

            conn.execute("BEGIN IMMEDIATE")
            pending = hold.earlier_pending_run(conn, store_id, detect_run_id, run_seq)
            if pending:
                held.append(
                    _hold_and_commit(
                        conn,
                        store_id,
                        detect_run_id,
                        "pending_earlier_run",
                        pending["run_id"],
                        draft,
                        snap,
                    )
                )
                continue

            claim = hold.claim_lease(conn, store_id, detect_run_id)
            if not claim["ok"]:
                held.append(
                    _hold_and_commit(
                        conn,
                        store_id,
                        detect_run_id,
                        "lease_held_by_other",
                        claim["holder_run"],
                        draft,
                        snap,
                    )
                )
                continue

            # 持租（同一事务）内做基准漂移检查，再写正式维。
            snap_before = hold.conflict_snapshot(conn, store_id)
            base_hash = draft["base_hash"] if draft else None
            base_hist_id = draft["base_hist_id"] if draft else None
            if snap_before["row_hash"] != base_hash or snap_before["hist_id"] != base_hist_id:
                held.append(
                    _hold_and_commit(
                        conn,
                        store_id,
                        detect_run_id,
                        "base_version_changed",
                        snap_before.get("hist_id"),
                        draft,
                        snap,
                        release_lease=True,
                    )
                )
                continue

            try:
                written = _write_official(conn, detect_run_id, store_id, snap, snap_before)
            except ValueError as exc:
                held.append(
                    _hold_and_commit(
                        conn,
                        store_id,
                        detect_run_id,
                        str(exc),
                        snap_before.get("hist_id"),
                        draft,
                        snap,
                        release_lease=True,
                    )
                )
                continue
            hold.drop_draft(conn, detect_run_id, store_id)
            conn.commit()
            applied.append(written)

        # 收尾：释放租约、关闭跑次、落回执 —— 一个事务完成。
        conn.execute("BEGIN IMMEDIATE")
        for store_id in change_store_ids:
            hold.release_lease(conn, store_id, detect_run_id)
        closed_at = now_iso()
        hold.close_run(
            conn,
            detect_run_id,
            "applied" if applied else "conflict_hold",
            closed_at,
        )
        if applied and held:
            outcome = "partial_hold"
        elif held:
            outcome = "conflict_hold"
        elif applied:
            outcome = "applied"
        else:
            outcome = "noop"
        receipt = {
            "ok": outcome != "conflict_hold",
            "architecture": hold.active_mode(conn),
            "architecture_label": hold.mode_label(hold.active_mode(conn)),
            "detect_run_id": detect_run_id,
            "outcome": outcome,
            "applied_count": len(applied),
            "held_count": len(held),
            "applied": applied,
            "held": held,
            "lease_released": True,
            "created_at": closed_at,
        }
        hold.save_receipt(conn, receipt)
        conn.commit()
        return receipt
    finally:
        conn.close()


def _apply_rollback(conn, detect_run_id: str):
    """乙·冲突整票回滚：先探测冲突；任一冲突则整票回滚、不写正式维、不入搁置，
    只返回冲突回执（含冲突 run 对与当时 is_current 快照）。"""
    # 先拿离线的候选，探测阶段不持锁、不脏写。
    probe = connect()
    try:
        run = probe.execute(
            "SELECT * FROM detect_runs WHERE id=?", (detect_run_id,)
        ).fetchone()
        if not run:
            return {"ok": False, "outcome": "not_found", "message": "检测跑次不存在"}
        if run["status"] != "open":
            return {
                "ok": False,
                "outcome": "run_closed",
                "detect_run_id": detect_run_id,
                "status": run["status"],
                "message": "跑次已关闭（已应用或已放弃），不可重复应用",
            }
        report = json.loads(run["report_json"] or "{}")
        changes = report.get("changes", [])
        change_store_ids = _store_ids_in_changes(changes)
        run_seq = run["seq"]
        draft_rows = {
            r["store_id"]: dict(r)
            for r in probe.execute(
                "SELECT * FROM version_drafts WHERE run_id=?", (detect_run_id,)
            ).fetchall()
        }
        missing = [sid for sid in change_store_ids if sid not in draft_rows]
        if missing:
            return {
                "ok": False,
                "outcome": "draft_missing",
                "detect_run_id": detect_run_id,
                "missing_store_ids": missing,
                "message": "检测跑次缺少草稿基准，请重跑检测后再应用",
            }
        now = hold.now_iso()
        conflicts = []
        for ch in changes:
            store_id = ch.get("store_id")
            if not store_id:
                continue
            pending = hold.earlier_pending_run(probe, store_id, detect_run_id, run_seq)
            lease = hold.lease_state(probe, store_id, now)
            draft = draft_rows.get(store_id)
            snap = hold.conflict_snapshot(probe, store_id)
            if pending:
                conflicts.append(
                    {
                        "store_id": store_id,
                        "reason": "pending_earlier_run",
                        "blocking_run": pending["run_id"],
                        "holder_run": pending["run_id"],
                        "base": {
                            "hist_id": draft["base_hist_id"],
                            "row_hash": draft["base_hash"],
                        },
                        "snapshot_at_conflict": snap,
                    }
                )
            elif lease and lease["active"] and lease["holder_run"] != detect_run_id:
                conflicts.append(
                    {
                        "store_id": store_id,
                        "reason": "lease_held_by_other",
                        "blocking_run": lease["holder_run"],
                        "holder_run": lease["holder_run"],
                        "base": {
                            "hist_id": draft["base_hist_id"],
                            "row_hash": draft["base_hash"],
                        },
                        "snapshot_at_conflict": snap,
                    }
                )
            elif snap["hist_id"] != draft["base_hist_id"] or snap["row_hash"] != draft["base_hash"]:
                conflicts.append(
                    {
                        "store_id": store_id,
                        "reason": "base_version_changed",
                        "blocking_run": snap.get("hist_id"),
                        "holder_run": None,
                        "base": {
                            "hist_id": draft["base_hist_id"],
                            "row_hash": draft["base_hash"],
                        },
                        "snapshot_at_conflict": snap,
                    }
                )
    finally:
        probe.close()

    if conflicts:
        # 整票回滚：不写正式维、不入搁置；跑次终结为 conflict_rollback，
        # 回执即冲突证据（乙口径：核证无搁置行，只认冲突回执/租约 holder）。
        rconn = connect_immediate()
        try:
            rconn.execute("BEGIN IMMEDIATE")
            closed_at = hold.now_iso()
            hold.close_run(rconn, detect_run_id, "conflict_rollback", closed_at)
            receipt = {
                "ok": False,
                "architecture": hold.MODE_ROLLBACK,
                "architecture_label": hold.MODE_LABELS[hold.MODE_ROLLBACK],
                "detect_run_id": detect_run_id,
                "outcome": "conflict_rollback",
                "rolled_back": True,
                "applied_count": 0,
                "held_count": 0,
                "applied": [],
                "held": [],
                "conflicts": conflicts,
                "conflict_run_pairs": [
                    {
                        "conflict_run": detect_run_id,
                        "blocking_run": c["blocking_run"],
                        "store_id": c["store_id"],
                        "reason": c["reason"],
                    }
                    for c in conflicts
                ],
                "lease_released": True,
                "message": "整票回滚：未写正式维、未入搁置；冲突见回执，跑次已终结。",
                "created_at": closed_at,
            }
            hold.save_receipt(rconn, receipt)
            rconn.commit()
            return receipt
        finally:
            rconn.close()

    # 无冲突：在单事务内持租写全部键，异常则整票回滚（含正式维与租约）。
    conn.execute("BEGIN IMMEDIATE")
    try:
        run_row = conn.execute(
            "SELECT status, seq FROM detect_runs WHERE id=?", (detect_run_id,)
        ).fetchone()
        if not run_row or run_row["status"] != "open":
            conn.rollback()
            return {
                "ok": False,
                "outcome": "run_closed",
                "detect_run_id": detect_run_id,
                "message": "跑次已关闭，不可重复应用",
            }
        applied = []
        for ch in changes:
            store_id = ch.get("store_id")
            if not store_id:
                continue
            snap = _load_snapshot(conn, ch.get("snapshot_id"))
            if not snap:
                continue
            draft = conn.execute(
                "SELECT * FROM version_drafts WHERE run_id=? AND store_id=?",
                (detect_run_id, store_id),
            ).fetchone()
            pending = hold.earlier_pending_run(conn, store_id, detect_run_id, run_row["seq"])
            if pending:
                raise _RaceConflict(
                    store_id,
                    "pending_earlier_run",
                    pending["run_id"],
                    draft,
                    snap,
                    hold.conflict_snapshot(conn, store_id),
                )
            claim = hold.claim_lease(conn, store_id, detect_run_id)
            if not claim["ok"]:
                raise _RaceConflict(
                    store_id,
                    "lease_held_by_other",
                    claim["holder_run"],
                    draft,
                    snap,
                    hold.conflict_snapshot(conn, store_id),
                )
            # 持租后在同一写事务内复核基准：探测阶段的"无冲突"不能替代它。
            snap_before = hold.conflict_snapshot(conn, store_id)
            base_hash = draft["base_hash"] if draft else None
            base_hist_id = draft["base_hist_id"] if draft else None
            if snap_before["row_hash"] != base_hash or snap_before["hist_id"] != base_hist_id:
                raise _RaceConflict(
                    store_id,
                    "base_version_changed",
                    snap_before.get("hist_id"),
                    draft,
                    snap,
                    snap_before,
                )
            written = _write_official(conn, detect_run_id, store_id, snap, snap_before)
            hold.drop_draft(conn, detect_run_id, store_id)
            applied.append(written)
        for store_id in change_store_ids:
            hold.release_lease(conn, store_id, detect_run_id)
        closed_at = hold.now_iso()
        hold.close_run(conn, detect_run_id, "applied", closed_at)
        receipt = {
            "ok": True,
            "architecture": hold.MODE_ROLLBACK,
            "architecture_label": hold.MODE_LABELS[hold.MODE_ROLLBACK],
            "detect_run_id": detect_run_id,
            "outcome": "applied",
            "rolled_back": False,
            "applied_count": len(applied),
            "held_count": 0,
            "applied": applied,
            "held": [],
            "conflicts": [],
            "lease_released": True,
            "created_at": closed_at,
        }
        hold.save_receipt(conn, receipt)
        conn.commit()
        return receipt
    except _RaceConflict as exc:
        conn.rollback()
        # 竞态在持锁阶段出现：按乙口径整票回滚并出冲突回执（含冲突 run 对 + 当时快照）。
        conflict = {
            "store_id": exc.store_id,
            "reason": exc.reason,
            "blocking_run": exc.blocking_run,
            "holder_run": exc.blocking_run,
            "base": {
                "hist_id": exc.draft["base_hist_id"] if exc.draft else None,
                "row_hash": exc.draft["base_hash"] if exc.draft else None,
            },
            "snapshot_at_conflict": exc.snapshot,
            "attempt": {"snapshot_id": exc.snap["id"], "to_hash": exc.snap["row_hash"]},
        }
        rconn = connect_immediate()
        try:
            rconn.execute("BEGIN IMMEDIATE")
            closed_at = hold.now_iso()
            hold.close_run(rconn, detect_run_id, "conflict_rollback", closed_at)
            receipt = {
                "ok": False,
                "architecture": hold.MODE_ROLLBACK,
                "architecture_label": hold.MODE_LABELS[hold.MODE_ROLLBACK],
                "detect_run_id": detect_run_id,
                "outcome": "conflict_rollback",
                "rolled_back": True,
                "applied_count": 0,
                "held_count": 0,
                "applied": [],
                "held": [],
                "conflicts": [conflict],
                "conflict_run_pairs": [
                    {
                        "conflict_run": detect_run_id,
                        "blocking_run": exc.blocking_run,
                        "store_id": exc.store_id,
                        "reason": exc.reason,
                    }
                ],
                "snapshot_at_conflict": exc.snapshot,
                "lease_released": True,
                "message": f"整票回滚：{exc.reason}；正式维未写。",
                "created_at": closed_at,
            }
            hold.save_receipt(rconn, receipt)
            rconn.commit()
            return receipt
        finally:
            rconn.close()
    finally:
        conn.close()


def rebuild_proofs():
    """Attest: each current mirror hist_id must point to is_current=1 hist with same hash."""
    conn = connect()
    now = now_iso()
    try:
        mirrors = conn.execute("SELECT * FROM dim_store_hours_current ORDER BY store_id").fetchall()
        results = []
        for m in mirrors:
            hist = conn.execute(
                "SELECT * FROM dim_store_hours_hist WHERE id=?", (m["hist_id"],)
            ).fetchone()
            valid = 1
            detail = {}
            if not hist:
                valid = 0
                detail["error"] = "mirror_hist_missing"
            elif hist["is_current"] != 1:
                valid = 0
                detail["error"] = "mirror_not_current_hist"
            elif hist["row_hash"] != m["row_hash"]:
                valid = 0
                detail["error"] = "hash_mismatch"
            elif hist["store_id"] != m["store_id"]:
                valid = 0
                detail["error"] = "store_mismatch"
            else:
                detail["ok"] = True
            conn.execute(
                """INSERT INTO scd_proofs(
                  store_id, hist_id, mirror_hist_id, row_hash, valid, detail_json, created_at
                ) VALUES (?,?,?,?,?,?,?)""",
                (
                    m["store_id"],
                    hist["id"] if hist else "",
                    m["hist_id"],
                    m["row_hash"],
                    valid,
                    json.dumps(detail, ensure_ascii=False),
                    now,
                ),
            )
            results.append({"store_id": m["store_id"], "valid": bool(valid), "detail": detail})
        conn.commit()
        proof = hold.build_proof(conn)
        summary = {
            "checked": len(results),
            "failed": sum(1 for r in results if not r["valid"]),
            "results": results,
            "concurrency": {
                "ok": not proof["violations"],
                "architecture": proof["architecture"],
                "architecture_label": proof["architecture_label"],
                "holds": proof["summary"]["holds"],
                "violations": proof["violations"],
            },
        }
        return summary
    finally:
        conn.close()
