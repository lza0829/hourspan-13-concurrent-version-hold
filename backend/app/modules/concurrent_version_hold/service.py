"""并发版本持锁 · concurrent_version_hold

全局架构（本项目唯一贯彻）：**甲 —— 冲突入搁置**（默认）。
两种可行架构的分叉口径集中在此，用 ``settings.conflict_mode`` 全局切一，
同一时刻只有一种生效；实现按"两种架构分别核对"保留可切换能力。

口径
----
* 同一 ``store_id`` 任一时刻只允许一个持租写者写正式维；持租期间第三个
  写入者不得绕过租约。
* 同键存在未关闭检测跑次 / 未提交草稿时，后到者不得写正式维；先到者仍可
  完成（先到 = 更早创建的待提交跑次，按 ``detect_runs.seq`` 排序）。
* 甲：后到冲突不写正式维、不覆盖先到，而是落 ``version_holds`` 搁置行：
  键（store_id）+ 冲突 run 对（conflict_run / blocking_run / holder_run）
  + 当时 is_current 快照（含 hist_id、row_hash、is_current 计数）。
* 乙：后到冲突整票回滚，不入搁置；返回 ``apply_receipts`` 冲突回执
  （含冲突 run 对 + 当时 is_current 快照），正式维不变。
* 先到完成后必须释放租约，第三者才能再写。
* 邻键并行不受本键租约影响（租约按 store_id 粒度）。
* 核证：``GET /api/concurrency/proof`` 只读暴露 holder_run、冲突证据、
  is_current 唯一性、租约态。甲看 version_holds，乙只认冲突回执/租约 holder。

本模块只做闸门 / 搁置 / 核证，不直接写正式维；正式写由
``app.services.scd`` 在已持有租约的事务内完成。
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone

from app.db import connect, connect_immediate

ARCHITECTURE = "hold"  # 甲（默认，全局唯一贯彻）
ARCHITECTURE_LABEL = "甲·冲突入搁置"

MODE_HOLD = "hold"
MODE_ROLLBACK = "rollback"
MODE_LABELS = {
    MODE_HOLD: "甲·冲突入搁置",
    MODE_ROLLBACK: "乙·冲突整票回滚",
}
MODE_POLICY = {
    MODE_HOLD: (
        "同键单持租；后到冲突落 version_holds 搁置行（键+冲突 run 对+当时 is_current 快照），"
        "正式维与当前镜不变，先到未冲突键照常完成；核证以搁置行证明冲突，正式链恒为单段当前。"
    ),
    MODE_ROLLBACK: (
        "同键单持租；后到冲突整票回滚，不入搁置行，返回冲突回执（含冲突 run 对与当时 "
        "is_current 快照）；正式维与当前镜不变；核证无搁置行，只认冲突回执与租约 holder。"
    ),
}
POLICY = MODE_POLICY[MODE_HOLD]

DEFAULT_LEASE_TTL = 60


SCHEMA = """
CREATE TABLE IF NOT EXISTS version_leases (
  store_id TEXT PRIMARY KEY,
  holder_run TEXT NOT NULL,
  acquired_at TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS version_drafts (
  run_id TEXT NOT NULL,
  store_id TEXT NOT NULL,
  snapshot_id TEXT,
  base_hist_id TEXT,
  base_hash TEXT,
  base_as_of_ts TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY (run_id, store_id)
);

CREATE TABLE IF NOT EXISTS version_holds (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  store_id TEXT NOT NULL,
  conflict_run TEXT NOT NULL,
  blocking_run TEXT,
  holder_run TEXT,
  reason TEXT NOT NULL,
  base_json TEXT NOT NULL DEFAULT '{}',
  snapshot_json TEXT NOT NULL DEFAULT '{}',
  attempt_json TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'held',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS apply_receipts (
  run_id TEXT PRIMARY KEY,
  outcome TEXT NOT NULL,
  architecture TEXT NOT NULL DEFAULT 'hold',
  created_at TEXT NOT NULL,
  applied_count INTEGER NOT NULL DEFAULT 0,
  held_count INTEGER NOT NULL DEFAULT 0,
  receipt_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_holds_store
  ON version_holds(store_id, id);
CREATE INDEX IF NOT EXISTS idx_drafts_store
  ON version_drafts(store_id, run_id);
"""


# --------------------------------------------------------------------------
# 时间工具
# --------------------------------------------------------------------------
def _fmt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_iso() -> str:
    return _fmt(datetime.now(timezone.utc))


def _plus(ts: str, seconds: int) -> str:
    base = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return _fmt(base + timedelta(seconds=seconds))


def lease_ttl_seconds() -> int:
    try:
        return max(5, int(os.environ.get("VERSION_LEASE_TTL", DEFAULT_LEASE_TTL)))
    except (TypeError, ValueError):
        return DEFAULT_LEASE_TTL


def configured_mode() -> str:
    """环境变量 VERSION_CONFLICT_MODE=hold|rollback；非法值回落到 hold。"""
    raw = (os.environ.get("VERSION_CONFLICT_MODE") or "").strip().lower()
    if raw in (MODE_HOLD, MODE_ROLLBACK):
        return raw
    if raw in ("甲", "jia", "a"):
        return MODE_HOLD
    if raw in ("乙", "yi", "b"):
        return MODE_ROLLBACK
    return MODE_HOLD


def active_mode(conn: sqlite3.Connection | None = None) -> str:
    """库内 settings 覆盖环境变量；全局只允许一种口径生效。"""
    if conn is not None:
        row = conn.execute(
            "SELECT value FROM settings WHERE key='conflict_mode'"
        ).fetchone()
        if row and row["value"] in (MODE_HOLD, MODE_ROLLBACK):
            return row["value"]
    return configured_mode()


def mode_label(mode: str | None = None) -> str:
    return MODE_LABELS.get(mode or configured_mode(), MODE_LABELS[MODE_HOLD])


def mode_policy(mode: str | None = None) -> str:
    return MODE_POLICY.get(mode or configured_mode(), MODE_POLICY[MODE_HOLD])


def set_mode(value: str) -> str:
    """切换全局冲突口径。已落搁置行不清除，避免抹掉历史冲突证据。"""
    v = (value or "").strip().lower()
    aliases = {"甲": MODE_HOLD, "jia": MODE_HOLD, "a": MODE_HOLD,
               "乙": MODE_ROLLBACK, "yi": MODE_ROLLBACK, "b": MODE_ROLLBACK}
    mode = aliases.get(v, v)
    if mode not in (MODE_HOLD, MODE_ROLLBACK):
        raise ValueError("conflict_mode 只能是 hold(甲) 或 rollback(乙)")
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES ('conflict_mode', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (mode,),
        )
        conn.commit()
        return mode
    finally:
        conn.close()


# --------------------------------------------------------------------------
# 建表 / 迁移
# --------------------------------------------------------------------------
def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}


def _add_column(conn: sqlite3.Connection, table: str, name: str, decl: str) -> None:
    if name not in _columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")


def migrate(conn: sqlite3.Connection) -> None:
    """幂等建表；给既有库补字段；尝试加"单店单当前"唯一闸。"""
    conn.executescript(SCHEMA)
    _add_column(conn, "detect_runs", "status", "TEXT NOT NULL DEFAULT 'closed'")
    _add_column(conn, "detect_runs", "seq", "INTEGER")
    _add_column(conn, "detect_runs", "closed_at", "TEXT")
    if not conn.execute(
        "SELECT 1 FROM settings WHERE key='conflict_mode'"
    ).fetchone():
        conn.execute(
            "INSERT INTO settings(key, value) VALUES ('conflict_mode', ?)",
            (configured_mode(),),
        )
    try:
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_hist_current_store "
            "ON dim_store_hours_hist(store_id) WHERE is_current=1"
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES ('hist_current_unique_index', 'ok') "
            "ON CONFLICT(key) DO UPDATE SET value='ok'"
        )
    except sqlite3.IntegrityError:
        # 既有脏数据已双开：不阻断启动，但登记为失败，核证页会暴露。
        conn.execute(
            "INSERT INTO settings(key, value) VALUES ('hist_current_unique_index', 'failed') "
            "ON CONFLICT(key) DO UPDATE SET value='failed'"
        )
    conn.commit()


# --------------------------------------------------------------------------
# 租约
# --------------------------------------------------------------------------
def lease_state(conn: sqlite3.Connection, store_id: str, now: str | None = None) -> dict | None:
    now = now or now_iso()
    row = conn.execute(
        "SELECT * FROM version_leases WHERE store_id=?", (store_id,)
    ).fetchone()
    if not row:
        return None
    data = dict(row)
    data["expired"] = data["expires_at"] <= now
    data["active"] = not data["expired"]
    return data


def claim_lease(
    conn: sqlite3.Connection, store_id: str, run_id: str, now: str | None = None
) -> dict:
    """必须在 BEGIN IMMEDIATE 事务内调用。返回 ok=False 表示被他人持有。"""
    now = now or now_iso()
    expires = _plus(now, lease_ttl_seconds())
    row = conn.execute(
        "SELECT * FROM version_leases WHERE store_id=?", (store_id,)
    ).fetchone()
    if row and row["holder_run"] != run_id and row["expires_at"] > now:
        return {
            "ok": False,
            "holder_run": row["holder_run"],
            "acquired_at": row["acquired_at"],
            "expires_at": row["expires_at"],
        }
    if row and row["holder_run"] == run_id:
        conn.execute(
            "UPDATE version_leases SET heartbeat_at=?, expires_at=? WHERE store_id=?",
            (now, expires, store_id),
        )
        return {
            "ok": True,
            "holder_run": run_id,
            "acquired_at": row["acquired_at"],
            "expires_at": expires,
            "takeover": False,
        }
    takeover = row is not None
    conn.execute(
        """INSERT INTO version_leases(store_id, holder_run, acquired_at, heartbeat_at, expires_at)
           VALUES (?,?,?,?,?)
           ON CONFLICT(store_id) DO UPDATE SET
             holder_run=excluded.holder_run,
             acquired_at=excluded.acquired_at,
             heartbeat_at=excluded.heartbeat_at,
             expires_at=excluded.expires_at""",
        (store_id, run_id, now, now, expires),
    )
    return {
        "ok": True,
        "holder_run": run_id,
        "acquired_at": now,
        "expires_at": expires,
        "takeover": takeover,
    }


def release_lease(conn: sqlite3.Connection, store_id: str, run_id: str) -> bool:
    cur = conn.execute(
        "DELETE FROM version_leases WHERE store_id=? AND holder_run=?",
        (store_id, run_id),
    )
    return cur.rowcount > 0


# --------------------------------------------------------------------------
# 草稿（未提交待写）
# --------------------------------------------------------------------------
def earlier_pending_run(
    conn: sqlite3.Connection, store_id: str, run_id: str, run_seq: int | None
) -> dict | None:
    """同键上更早创建且仍未关闭的待提交跑次；先到者以此压住后到。"""
    rows = conn.execute(
        """SELECT d.run_id, d.snapshot_id, d.base_hist_id, d.created_at AS draft_created_at,
                  r.created_at AS run_created_at, r.seq
           FROM version_drafts d
           JOIN detect_runs r ON r.id = d.run_id
           WHERE d.store_id=? AND d.run_id<>? AND r.status='open'
           ORDER BY COALESCE(r.seq, 0) ASC, r.created_at ASC, d.run_id ASC""",
        (store_id, run_id),
    ).fetchall()
    for row in rows:
        if run_seq is None or row["seq"] is None or row["seq"] < run_seq:
            return dict(row)
    return None


def add_draft(
    conn: sqlite3.Connection,
    run_id: str,
    store_id: str,
    snapshot_id: str | None,
    base_hist_id: str | None,
    base_hash: str | None,
    base_as_of_ts: str | None,
    created_at: str | None = None,
) -> None:
    conn.execute(
        """INSERT INTO version_drafts(
             run_id, store_id, snapshot_id, base_hist_id, base_hash, base_as_of_ts, created_at
           ) VALUES (?,?,?,?,?,?,?)
           ON CONFLICT(run_id, store_id) DO UPDATE SET
             snapshot_id=excluded.snapshot_id,
             base_hist_id=excluded.base_hist_id,
             base_hash=excluded.base_hash,
             base_as_of_ts=excluded.base_as_of_ts""",
        (
            run_id,
            store_id,
            snapshot_id,
            base_hist_id,
            base_hash,
            base_as_of_ts,
            created_at or now_iso(),
        ),
    )


def drop_draft(conn: sqlite3.Connection, run_id: str, store_id: str) -> None:
    conn.execute(
        "DELETE FROM version_drafts WHERE run_id=? AND store_id=?", (run_id, store_id)
    )


# --------------------------------------------------------------------------
# 键状态 / 冲突快照
# --------------------------------------------------------------------------
def store_state(conn: sqlite3.Connection, store_id: str) -> dict:
    hists = conn.execute(
        "SELECT * FROM dim_store_hours_hist WHERE store_id=? ORDER BY valid_from, id",
        (store_id,),
    ).fetchall()
    current = [h for h in hists if h["is_current"] == 1]
    mirror = conn.execute(
        "SELECT * FROM dim_store_hours_current WHERE store_id=?", (store_id,)
    ).fetchone()
    return {
        "hists": [dict(h) for h in hists],
        "current": [dict(h) for h in current],
        "mirror": dict(mirror) if mirror else None,
    }


def conflict_snapshot(conn: sqlite3.Connection, store_id: str) -> dict:
    """落搁置行时冻结"当时 is_current 快照"。"""
    state = store_state(conn, store_id)
    current = state["current"]
    lead = current[-1] if current else None
    return {
        "store_id": store_id,
        "is_current_count": len(current),
        "is_current_hist_ids": [h["id"] for h in current],
        "hist_id": lead["id"] if lead else None,
        "row_hash": lead["row_hash"] if lead else None,
        "open_time": lead["open_time"] if lead else None,
        "close_time": lead["close_time"] if lead else None,
        "weekday_mask": lead["weekday_mask"] if lead else None,
        "biz_status": lead["biz_status"] if lead else None,
        "valid_from": lead["valid_from"] if lead else None,
        "valid_to": lead["valid_to"] if lead else None,
        "mirror_hist_id": state["mirror"]["hist_id"] if state["mirror"] else None,
        "mirror_row_hash": state["mirror"]["row_hash"] if state["mirror"] else None,
        "captured_at": now_iso(),
    }


def record_hold(
    conn: sqlite3.Connection,
    store_id: str,
    conflict_run: str,
    reason: str,
    blocking_run: str | None = None,
    holder_run: str | None = None,
    base: dict | None = None,
    snapshot: dict | None = None,
    attempt: dict | None = None,
    created_at: str | None = None,
) -> tuple[int, dict]:
    snap = snapshot or conflict_snapshot(conn, store_id)
    cur = conn.execute(
        """INSERT INTO version_holds(
             store_id, conflict_run, blocking_run, holder_run, reason,
             base_json, snapshot_json, attempt_json, status, created_at
           ) VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (
            store_id,
            conflict_run,
            blocking_run,
            holder_run,
            reason,
            json.dumps(base or {}, ensure_ascii=False),
            json.dumps(snap, ensure_ascii=False),
            json.dumps(attempt or {}, ensure_ascii=False),
            "held",
            created_at or now_iso(),
        ),
    )
    return int(cur.lastrowid), snap


# --------------------------------------------------------------------------
# 跑次关闭 / 放弃
# --------------------------------------------------------------------------
def close_run(
    conn: sqlite3.Connection, run_id: str, status: str, closed_at: str | None = None
) -> None:
    conn.execute(
        "UPDATE detect_runs SET status=?, closed_at=? WHERE id=?",
        (status, closed_at or now_iso(), run_id),
    )
    conn.execute("DELETE FROM version_drafts WHERE run_id=?", (run_id,))


def save_receipt(conn: sqlite3.Connection, receipt: dict) -> None:
    conn.execute(
        """INSERT INTO apply_receipts(
             run_id, outcome, architecture, created_at, applied_count, held_count, receipt_json
           ) VALUES (?,?,?,?,?,?,?)
           ON CONFLICT(run_id) DO UPDATE SET
             outcome=excluded.outcome,
             created_at=excluded.created_at,
             applied_count=excluded.applied_count,
             held_count=excluded.held_count,
             receipt_json=excluded.receipt_json""",
        (
            receipt["detect_run_id"],
            receipt["outcome"],
            ARCHITECTURE,
            receipt.get("created_at") or now_iso(),
            receipt.get("applied_count", 0),
            receipt.get("held_count", 0),
            json.dumps(receipt, ensure_ascii=False),
        ),
    )


def load_receipt(conn: sqlite3.Connection, run_id: str) -> dict | None:
    row = conn.execute(
        "SELECT receipt_json FROM apply_receipts WHERE run_id=?", (run_id,)
    ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row["receipt_json"])
    except json.JSONDecodeError:
        return None


def abandon_run(run_id: str, reason: str = "abandoned") -> dict | None:
    """主动关闭未提交跑次，释放其未提交草稿（先到者可让位）。"""
    conn = connect_immediate()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT * FROM detect_runs WHERE id=?", (run_id,)).fetchone()
        if not row:
            conn.rollback()
            return None
        if row["status"] != "open":
            conn.rollback()
            return {
                "ok": False,
                "detect_run_id": run_id,
                "outcome": "run_closed",
                "status": row["status"],
                "reason": reason,
            }
        closed = now_iso()
        close_run(conn, run_id, "abandoned", closed)
        receipt = {
            "ok": True,
            "architecture": ARCHITECTURE,
            "detect_run_id": run_id,
            "outcome": "abandoned",
            "applied_count": 0,
            "held_count": 0,
            "applied": [],
            "held": [],
            "message": "跑次已放弃，未提交草稿已撤下，先到让位。",
            "reason": reason,
            "created_at": closed,
        }
        save_receipt(conn, receipt)
        conn.commit()
        return receipt
    finally:
        conn.close()


# --------------------------------------------------------------------------
# 只读核证
# --------------------------------------------------------------------------
def _safe_json(raw: str | None, default):
    if not raw:
        return default
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return default


def _history_diag(hists: list[dict]) -> dict:
    overlap = False
    overlap_pairs = []
    orphan_open = []
    for i, row in enumerate(hists):
        if row["is_current"] != 1 and row["valid_to"] is None:
            orphan_open.append(row["id"])
        if i + 1 < len(hists):
            nxt = hists[i + 1]
            if row["valid_to"] is None:
                # 非末行未闭合 = 区间互盖
                overlap = True
                overlap_pairs.append([row["id"], nxt["id"]])
            elif row["valid_to"] > nxt["valid_from"]:
                overlap = True
                overlap_pairs.append([row["id"], nxt["id"]])
    return {
        "rows": len(hists),
        "segments": [
            {
                "id": h["id"],
                "valid_from": h["valid_from"],
                "valid_to": h["valid_to"],
                "is_current": bool(h["is_current"]),
                "row_hash": h["row_hash"],
            }
            for h in hists
        ],
        "overlap": overlap,
        "overlap_pairs": overlap_pairs,
        "orphan_open_rows": orphan_open,
        "current_row_has_valid_to": [
            h["id"] for h in hists if h["is_current"] == 1 and h["valid_to"] is not None
        ],
    }


def _key_proof(
    conn: sqlite3.Connection, store_id: str, now: str, mode: str = MODE_HOLD
) -> dict:
    state = store_state(conn, store_id)
    hists, current, mirror = state["hists"], state["current"], state["mirror"]
    lease = lease_state(conn, store_id, now)
    drafts = [
        dict(r)
        for r in conn.execute(
            """SELECT d.*, r.status AS run_status, r.seq AS run_seq
               FROM version_drafts d
               LEFT JOIN detect_runs r ON r.id=d.run_id
               WHERE d.store_id=?
               ORDER BY COALESCE(r.seq, 0), d.run_id""",
            (store_id,),
        ).fetchall()
    ]
    hold_rows = conn.execute(
        "SELECT * FROM version_holds WHERE store_id=? ORDER BY id DESC LIMIT 20",
        (store_id,),
    ).fetchall()
    conflicts = []
    evidence_ok = True
    if mode == MODE_HOLD:
        for row in hold_rows:
            snap = _safe_json(row["snapshot_json"], {})
            base = _safe_json(row["base_json"], {})
            attempt = _safe_json(row["attempt_json"], {})
            run_exists = (
                conn.execute(
                    "SELECT 1 FROM detect_runs WHERE id=?", (row["conflict_run"],)
                ).fetchone()
                is not None
            )
            pair_ok = bool(row["conflict_run"]) and bool(
                row["blocking_run"] or row["holder_run"]
            )
            snap_ok = bool(snap) and snap.get("is_current_count") is not None
            this_ok = bool(run_exists and pair_ok and snap_ok)
            evidence_ok = evidence_ok and this_ok
            conflicts.append(
                {
                    "id": row["id"],
                    "conflict_run": row["conflict_run"],
                    "blocking_run": row["blocking_run"],
                    "holder_run": row["holder_run"],
                    "reason": row["reason"],
                    "status": row["status"],
                    "created_at": row["created_at"],
                    "snapshot_at_conflict": snap,
                    "base": base,
                    "attempt": attempt,
                    "conflict_run_exists": run_exists,
                    "evidence_complete": this_ok,
                }
            )
    else:
        # 乙口径：本键不出搁置证据，冲突证据在 apply_receipts。
        for rec in _receipt_conflicts(conn):
            for c in rec["conflicts"]:
                if c.get("store_id") != store_id:
                    continue
                conflicts.append(
                    {
                        "receipt_run": rec["receipt_run"],
                        "conflict_run": rec["receipt_run"],
                        "blocking_run": c.get("blocking_run"),
                        "holder_run": c.get("holder_run"),
                        "reason": c.get("reason"),
                        "status": rec["outcome"],
                        "created_at": rec["created_at"],
                        "snapshot_at_conflict": c.get("snapshot_at_conflict", {}),
                        "base": c.get("base", {}),
                        "attempt": {},
                        "conflict_run_exists": True,
                        "evidence_complete": bool(
                            c.get("blocking_run") or c.get("holder_run")
                        ),
                    }
                )

    current_ids = {h["id"] for h in current}
    history = _history_diag(hists)
    current_unique = len(current) == 1
    mirror_consistent = False
    if mirror and len(current) == 1:
        lead = current[0]
        mirror_consistent = (
            mirror["hist_id"] == lead["id"]
            and mirror["row_hash"] == lead["row_hash"]
            and mirror["store_id"] == store_id
        )
    elif not mirror and not current:
        mirror_consistent = True

    violations = []
    if len(current) > 1:
        violations.append(
            {
                "type": "double_current",
                "store_id": store_id,
                "detail": {"count": len(current), "hist_ids": sorted(current_ids)},
            }
        )
    if hists and len(current) == 0:
        violations.append(
            {"type": "no_current_with_history", "store_id": store_id, "detail": {}}
        )
    if history["overlap"]:
        violations.append(
            {
                "type": "interval_overlap",
                "store_id": store_id,
                "detail": {"pairs": history["overlap_pairs"]},
            }
        )
    if history["orphan_open_rows"]:
        violations.append(
            {
                "type": "open_segment_not_current",
                "store_id": store_id,
                "detail": {"hist_ids": history["orphan_open_rows"]},
            }
        )
    if history["current_row_has_valid_to"]:
        violations.append(
            {
                "type": "current_row_closed",
                "store_id": store_id,
                "detail": {"hist_ids": history["current_row_has_valid_to"]},
            }
        )
    if mirror is not None and len(current) == 1 and not mirror_consistent:
        violations.append(
            {
                "type": "mirror_mismatch",
                "store_id": store_id,
                "detail": {
                    "mirror_hist_id": mirror["hist_id"],
                    "current_hist_id": current[0]["id"],
                },
            }
        )
    if mirror is not None and not current:
        violations.append(
            {"type": "mirror_without_current", "store_id": store_id, "detail": {}}
        )
    if current and mirror is None:
        violations.append(
            {"type": "mirror_missing", "store_id": store_id, "detail": {}}
        )
    warnings = []
    if lease and lease["expired"]:
        # 过期租约不破坏历史不变量，但会阻塞后续写入：作为告警暴露。
        warnings.append(
            {
                "type": "stale_lease",
                "store_id": store_id,
                "detail": {
                    "holder_run": lease["holder_run"],
                    "expires_at": lease["expires_at"],
                },
            }
        )
    if hold_rows and not evidence_ok:
        violations.append(
            {
                "type": "hold_evidence_incomplete",
                "store_id": store_id,
                "detail": {"hold_ids": [h["id"] for h in hold_rows]},
            }
        )
    if hold_rows and len(current) > 1:
        # 甲架构建成后不应出现：搁置已记但正式仍双开
        violations.append(
            {
                "type": "hold_recorded_but_official_double_open",
                "store_id": store_id,
                "detail": {"hold_ids": [h["id"] for h in hold_rows]},
            }
        )

    return {
        "store_id": store_id,
        "holder_run": (
            lease["holder_run"] if lease and lease["active"] else None
        ),
        "lease": lease,
        "lease_state": (
            "free"
            if not lease
            else ("expired" if lease["expired"] else "held")
        ),
        "is_current": {
            "count": len(current),
            "unique": current_unique,
            "hist_ids": [h["id"] for h in current],
            "row_hash": current[0]["row_hash"] if len(current) == 1 else None,
            "valid_from": current[0]["valid_from"] if len(current) == 1 else None,
        },
        "is_current_unique": current_unique,
        "mirror": mirror,
        "mirror_consistent": mirror_consistent,
        "history": history,
        "pending_drafts": drafts,
        "conflicts": conflicts,
        "conflict_evidence_complete": evidence_ok if hold_rows else True,
        "official_double_open_with_hold": bool(hold_rows) and len(current) > 1,
        "violations": violations,
        "warnings": warnings,
        "ok": not violations,
    }


def build_proof(conn: sqlite3.Connection, store_id: str | None = None) -> dict:
    """只读：holder_run / 冲突证据 / is_current 唯一性 / 租约态。"""
    now = now_iso()
    mode = active_mode(conn)
    if store_id:
        exists = conn.execute(
            "SELECT 1 FROM stores WHERE id=?", (store_id,)
        ).fetchone()
        store_ids = [store_id] if exists else []
    else:
        store_ids = [
            r["id"]
            for r in conn.execute("SELECT id FROM stores ORDER BY id").fetchall()
        ]

    keys = [_key_proof(conn, sid, now, mode) for sid in store_ids]
    violations = [v for k in keys for v in k["violations"]]
    warnings = [w for k in keys for w in k.get("warnings", [])]

    holds = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM version_holds ORDER BY id DESC LIMIT 200"
        ).fetchall()
    ]
    for h in holds:
        h["snapshot_at_conflict"] = _safe_json(h.pop("snapshot_json", None), {})
        h["base"] = _safe_json(h.pop("base_json", None), {})
        h["attempt"] = _safe_json(h.pop("attempt_json", None), {})
    receipt_conflicts = _receipt_conflicts(conn)
    leases = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM version_leases ORDER BY store_id"
        ).fetchall()
    ]
    for l in leases:
        l["expired"] = l["expires_at"] <= now
        l["active"] = not l["expired"]

    open_drafts = conn.execute(
        "SELECT COUNT(*) AS c FROM version_drafts"
    ).fetchone()["c"]
    open_runs = conn.execute(
        "SELECT COUNT(*) AS c FROM detect_runs WHERE status='open'"
    ).fetchone()["c"]

    if mode == MODE_ROLLBACK:
        # 乙口径：核证无搁置行，只认冲突回执 / 租约 holder。
        if holds:
            violations.append(
                {
                    "type": "hold_rows_in_rollback_mode",
                    "store_id": None,
                    "detail": {"hold_ids": [h["id"] for h in holds]},
                }
            )
        conflict_view = receipt_conflicts
    else:
        conflict_view = holds

    summary = {
        "keys": len(keys),
        "leases_active": sum(1 for l in leases if l["active"]),
        "leases_expired": sum(1 for l in leases if l["expired"]),
        "holds": len(holds),
        "conflict_receipts": len(receipt_conflicts),
        "open_drafts": open_drafts,
        "open_runs": open_runs,
        "double_current_keys": sum(1 for k in keys if k["is_current"]["count"] > 1),
        "violations": len(violations),
        "warnings": len(warnings),
    }

    return {
        "architecture": mode,
        "architecture_label": MODE_LABELS[mode],
        "policy": MODE_POLICY[mode],
        "read_only": True,
        "generated_at": now,
        "lease_ttl_seconds": lease_ttl_seconds(),
        "ok": not violations,
        "summary": summary,
        "violations": violations,
        "warnings": warnings,
        "keys": keys,
        "leases": leases,
        "holds": holds,
        "conflict_evidence": {
            "mode": mode,
            "source": "version_holds" if mode == MODE_HOLD else "apply_receipts",
            "rows": conflict_view,
        },
    }


def _receipt_conflicts(conn: sqlite3.Connection) -> list[dict]:
    """从 apply_receipts 抽冲突回执（乙口径的证据面）。"""
    out = []
    rows = conn.execute(
        "SELECT run_id, receipt_json, created_at FROM apply_receipts "
        "WHERE outcome IN ('conflict_rollback','partial_hold') ORDER BY created_at DESC LIMIT 200"
    ).fetchall()
    for r in rows:
        receipt = _safe_json(r["receipt_json"], {})
        conflicts = receipt.get("conflicts") or [
            h for h in receipt.get("held", []) if h.get("reason")
        ]
        if not conflicts:
            continue
        out.append(
            {
                "receipt_run": r["run_id"],
                "outcome": receipt.get("outcome"),
                "architecture": receipt.get("architecture"),
                "created_at": r["created_at"],
                "conflict_run_pairs": receipt.get("conflict_run_pairs", []),
                "conflicts": conflicts,
                "rolled_back": receipt.get("rolled_back", False),
            }
        )
    return out


def list_leases() -> list[dict]:
    conn = connect()
    try:
        now = now_iso()
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM version_leases ORDER BY store_id"
        ).fetchall()]
        for r in rows:
            r["expired"] = r["expires_at"] <= now
            r["active"] = not r["expired"]
        return rows
    finally:
        conn.close()


def list_holds(store_id: str | None = None, limit: int = 200) -> list[dict]:
    conn = connect()
    try:
        if store_id:
            rows = conn.execute(
                "SELECT * FROM version_holds WHERE store_id=? ORDER BY id DESC LIMIT ?",
                (store_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM version_holds ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["snapshot_at_conflict"] = _safe_json(d.pop("snapshot_json", None), {})
            d["base"] = _safe_json(d.pop("base_json", None), {})
            d["attempt"] = _safe_json(d.pop("attempt_json", None), {})
            out.append(d)
        return out
    finally:
        conn.close()
