import json
from uuid import uuid4

from app.db import connect, connect_immediate
from app.modules.concurrent_version_hold import service as hold
from app.seed import now_iso


def _collect_snapshots(conn, batch_id: str | None):
    if batch_id:
        return conn.execute(
            "SELECT * FROM source_snapshots WHERE batch_id=? ORDER BY store_id, as_of_ts",
            (batch_id,),
        ).fetchall()
    return conn.execute(
        """
        SELECT s.* FROM source_snapshots s
        JOIN (
          SELECT store_id, MAX(as_of_ts) AS mx FROM source_snapshots GROUP BY store_id
        ) t ON s.store_id=t.store_id AND s.as_of_ts=t.mx
        ORDER BY s.store_id
        """
    ).fetchall()


def run_detect(batch_id: str | None = None):
    """对照当前镜与快照 row_hash 产出变更；跑次以 open 状态落库并生成逐键草稿。

    跑次不关闭 = 存在未提交草稿；同键后到写入者将因此落搁置（甲架构）。
    """
    conn = connect_immediate()
    run_id = f"DET-{uuid4().hex[:8]}"
    now = now_iso()
    try:
        conn.execute("BEGIN IMMEDIATE")
        seq = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS m FROM detect_runs"
        ).fetchone()["m"] + 1
        snaps = _collect_snapshots(conn, batch_id)

        changes = []
        drafts = []
        for snap in snaps:
            cur = conn.execute(
                "SELECT * FROM dim_store_hours_current WHERE store_id=?",
                (snap["store_id"],),
            ).fetchone()
            cur_hist = conn.execute(
                "SELECT * FROM dim_store_hours_hist WHERE store_id=? AND is_current=1",
                (snap["store_id"],),
            ).fetchone()
            if not cur:
                changes.append(
                    {
                        "store_id": snap["store_id"],
                        "kind": "new",
                        "snapshot_id": snap["id"],
                        "from_hash": None,
                        "to_hash": snap["row_hash"],
                    }
                )
            elif cur["row_hash"] != snap["row_hash"]:
                changes.append(
                    {
                        "store_id": snap["store_id"],
                        "kind": "attr_change",
                        "snapshot_id": snap["id"],
                        "from_hash": cur["row_hash"],
                        "to_hash": snap["row_hash"],
                        "from": {
                            "open_time": cur["open_time"],
                            "close_time": cur["close_time"],
                            "weekday_mask": cur["weekday_mask"],
                            "biz_status": cur["biz_status"],
                        },
                        "to": {
                            "open_time": snap["open_time"],
                            "close_time": snap["close_time"],
                            "weekday_mask": snap["weekday_mask"],
                            "biz_status": snap["biz_status"],
                        },
                    }
                )
            else:
                continue
            # 仅变更键才需要草稿基准（未提交待写）；无变更键不占后到闸门。
            base_hist_id = cur_hist["id"] if cur_hist else (cur["hist_id"] if cur else None)
            base_hash = cur_hist["row_hash"] if cur_hist else (cur["row_hash"] if cur else None)
            base_as_of = (
                cur_hist["valid_from"] if cur_hist else (cur["as_of_ts"] if cur else None)
            )
            drafts.append(
                {
                    "store_id": snap["store_id"],
                    "snapshot_id": snap["id"],
                    "base_hist_id": base_hist_id,
                    "base_hash": base_hash,
                    "base_as_of_ts": base_as_of,
                }
            )
        # 同一次检测里同一键可能命中多条快照；草稿只取该键最后一个快照为准，
        # 变更清单同样去重为每键一条（保持先到/后到口径一致）。
        by_store: dict[str, dict] = {}
        for ch in changes:
            by_store[ch["store_id"]] = ch
        draft_by_store: dict[str, dict] = {}
        for d in drafts:
            draft_by_store[d["store_id"]] = d

        report = {
            "changes": list(by_store.values()),
            "scanned": len(snaps),
            "drafts": draft_by_store,
            "architecture": hold.ARCHITECTURE,
            "note": "跑次 open 即未提交草稿；同键后到写入者将落搁置，正式维不变。",
        }
        conn.execute(
            """INSERT INTO detect_runs(id, batch_id, created_at, changed_count, report_json, status, seq)
               VALUES (?,?,?,?,?,?,?)""",
            (
                run_id,
                batch_id,
                now,
                len(by_store),
                json.dumps(report, ensure_ascii=False),
                "open",
                seq,
            ),
        )
        for d in draft_by_store.values():
            hold.add_draft(
                conn,
                run_id=run_id,
                store_id=d["store_id"],
                snapshot_id=d["snapshot_id"],
                base_hist_id=d["base_hist_id"],
                base_hash=d["base_hash"],
                base_as_of_ts=d["base_as_of_ts"],
                created_at=now,
            )
        conn.commit()
        return {
            "detect_run_id": run_id,
            "seq": seq,
            "status": "open",
            "changed_count": len(by_store),
            "changes": list(by_store.values()),
            "architecture": hold.ARCHITECTURE,
        }
    finally:
        conn.close()
