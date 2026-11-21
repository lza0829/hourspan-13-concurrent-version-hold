from fastapi import APIRouter, Body, HTTPException

from app.db import connect
from app.modules import STUBS
from app.modules.concurrent_version_hold import service as hold
from app.services import catalog, detect, ingest, scd

router = APIRouter(prefix="/api")


@router.get("/overview")
def overview():
    conn = connect()
    try:
        data = catalog.overview(conn)
        data["空桩"] = STUBS
        return data
    finally:
        conn.close()


@router.get("/stores")
def stores():
    conn = connect()
    try:
        return catalog.store_board(conn)
    finally:
        conn.close()


@router.get("/snapshots")
def snapshots(batch_id: str | None = None):
    conn = connect()
    try:
        if batch_id:
            rows = conn.execute(
                "SELECT * FROM source_snapshots WHERE batch_id=? ORDER BY as_of_ts, store_id",
                (batch_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM source_snapshots ORDER BY as_of_ts DESC, store_id LIMIT 200"
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.get("/batches")
def batches():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT * FROM ingest_batches ORDER BY created_at DESC LIMIT 50"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.get("/history")
def history(store_id: str | None = None):
    conn = connect()
    try:
        if store_id:
            rows = conn.execute(
                "SELECT * FROM dim_store_hours_hist WHERE store_id=? ORDER BY valid_from, id",
                (store_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM dim_store_hours_hist ORDER BY store_id, valid_from, id"
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.get("/current")
def current_mirror():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT * FROM dim_store_hours_current ORDER BY store_id"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.get("/detect-runs")
def detect_runs():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT * FROM detect_runs ORDER BY COALESCE(seq, 0) DESC, created_at DESC LIMIT 50"
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["pending_stores"] = [
                x["store_id"]
                for x in conn.execute(
                    "SELECT store_id FROM version_drafts WHERE run_id=? ORDER BY store_id",
                    (r["id"],),
                ).fetchall()
            ]
            d["held"] = [
                dict(x)
                for x in conn.execute(
                    "SELECT id, store_id, blocking_run, reason, status, created_at "
                    "FROM version_holds WHERE conflict_run=? ORDER BY id",
                    (r["id"],),
                ).fetchall()
            ]
            out.append(d)
        return out
    finally:
        conn.close()


@router.get("/proofs")
def proofs():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT * FROM scd_proofs ORDER BY id DESC LIMIT 80"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.get("/quarantine")
def quarantine():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT * FROM quarantine ORDER BY id DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@router.post("/ingest/run")
def run_ingest(body: dict | None = Body(None)):
    name = (body or {}).get("fixture", "ingest_change_batch.json")
    return ingest.run_batch(name)


@router.post("/detect/run")
def run_detect(body: dict | None = Body(None)):
    batch_id = (body or {}).get("batch_id")
    return detect.run_detect(batch_id)


@router.post("/scd/apply")
def run_scd(body: dict = Body(...)):
    run_id = body.get("detect_run_id")
    if not run_id:
        raise HTTPException(400, "需要 detect_run_id")
    return scd.apply_type2(run_id)


@router.post("/scd/abandon")
def abandon_scd(body: dict = Body(...)):
    run_id = body.get("detect_run_id")
    if not run_id:
        raise HTTPException(400, "需要 detect_run_id")
    result = hold.abandon_run(run_id)
    if result is None:
        raise HTTPException(404, "检测跑次不存在")
    return result


@router.post("/proofs/rebuild")
def rebuild_proofs():
    return scd.rebuild_proofs()


# ---------------------------------------------------------------------------
# 并发持锁核证（只读）
# ---------------------------------------------------------------------------
@router.get("/concurrency/proof")
def concurrency_proof(store_id: str | None = None):
    conn = connect()
    try:
        return hold.build_proof(conn, store_id)
    finally:
        conn.close()


@router.get("/concurrency/leases")
def concurrency_leases():
    return hold.list_leases()


@router.get("/concurrency/holds")
def concurrency_holds(store_id: str | None = None):
    return hold.list_holds(store_id)


@router.get("/settings")
def settings():
    conn = connect()
    try:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        data = {r["key"]: r["value"] for r in rows}
        data["_conflict_mode_active"] = hold.active_mode(conn)
        return data
    finally:
        conn.close()


@router.post("/settings/conflict-mode")
def set_conflict_mode(body: dict = Body(...)):
    value = body.get("mode") or body.get("conflict_mode")
    if not value:
        raise HTTPException(400, "需要 mode = hold(甲) 或 rollback(乙)")
    try:
        mode = hold.set_mode(value)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {
        "ok": True,
        "conflict_mode": mode,
        "architecture_label": hold.MODE_LABELS[mode],
        "policy": hold.MODE_POLICY[mode],
        "note": "全局只贯彻一种口径；已落搁置行保留为历史冲突证据。",
    }


@router.post("/concurrency/lease/{store_id}/recover")
def recover_lease(store_id: str):
    """过期租约回收（只读核证发现 stale 时用）；未过期不得强夺。"""
    conn = hold.connect_immediate()
    try:
        conn.execute("BEGIN IMMEDIATE")
        lease = hold.lease_state(conn, store_id)
        if not lease:
            conn.rollback()
            return {"ok": True, "released": False, "message": "无租约"}
        if not lease["expired"]:
            conn.rollback()
            return {
                "ok": False,
                "released": False,
                "message": "租约未过期，不得强夺",
                "lease": lease,
            }
        hold.release_lease(conn, store_id, lease["holder_run"])
        conn.commit()
        return {
            "ok": True,
            "released": True,
            "store_id": store_id,
            "holder_run": lease["holder_run"],
        }
    finally:
        conn.close()


@router.get("/stubs")
def stubs():
    return {"stubs": STUBS}
