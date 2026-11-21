import React, { useEffect, useState } from "react";
import { api } from "../api.js";

export default function Concurrency() {
  const [proof, setProof] = useState(null);
  const [leases, setLeases] = useState([]);
  const [holds, setHolds] = useState([]);
  const [storeId, setStoreId] = useState("");
  const [err, setErr] = useState("");

  const reload = (sid) => {
    setErr("");
    const q = (sid ?? storeId).trim() ? `?store_id=${encodeURIComponent((sid ?? storeId).trim())}` : "";
    Promise.all([
      api(`/api/concurrency/proof${q}`),
      api("/api/concurrency/leases"),
      api(`/api/concurrency/holds${q}`),
    ])
      .then(([p, l, h]) => { setProof(p); setLeases(l); setHolds(h); })
      .catch((e) => setErr(String(e.message || e)));
  };

  useEffect(() => { reload(""); }, []);

  const recover = async (sid) => {
    try {
      await api(`/api/concurrency/lease/${encodeURIComponent(sid)}/recover`, {
        method: "POST",
        body: "{}",
      });
      reload();
    } catch (e) {
      setErr(String(e.message || e));
    }
  };

  const s = proof?.summary || {};
  const keys = proof?.keys || [];

  return (
    <>
      <div className="topbar">
        <div>
          <h1>并发持锁</h1>
          <p>
            {proof?.architecture_label || "甲·冲突入搁置"} · 同键单持租；后到冲突落搁置行，正式维与当前镜不变。
            本页只读。
          </p>
        </div>
      </div>
      <div className="panel">
        <div className="actions">
          <input
            value={storeId}
            onChange={(e) => setStoreId(e.target.value)}
            placeholder="按门店过滤，如 ST-EAST-01"
            style={{ minWidth: 220, padding: "0.4rem 0.6rem", borderRadius: 8, border: "1px solid var(--line)", background: "#0c2226", color: "var(--text)" }}
          />
          <button onClick={() => reload()}>刷新核证</button>
        </div>
        {err && <p className="error">{err}</p>}
        {proof && (
          <p className={proof.ok ? "" : "error"}>
            <span className={`badge ${proof.ok ? "ok" : "bad"}`}>
              {proof.ok ? "核证通过" : `违规 ${proof.violations.length}`}
            </span>
            <span className="muted" style={{ marginLeft: 10 }}>
              键 {s.keys} · 活跃租约 {s.leases_active} · 过期租约 {s.leases_expired} ·
              搁置 {s.holds} · 冲突回执 {s.conflict_receipts} · 未提交跑次 {s.open_runs} · 双当前键 {s.double_current_keys}
            </span>
          </p>
        )}
        {proof && (
          <p className="muted" style={{ marginTop: 6 }}>
            冲突证据面：<span className="mono">{proof.conflict_evidence?.source}</span>
            （甲看 version_holds，乙看 apply_receipts）
          </p>
        )}
        <p className="muted mono" style={{ fontSize: "0.78rem" }}>{proof?.policy}</p>
      </div>

      {proof && (proof.warnings || []).length > 0 && (
        <div className="panel">
          <h2>告警（不破坏不变量，但会阻塞写入）</h2>
          <table>
            <thead><tr><th>类型</th><th>门店</th><th>细节</th><th></th></tr></thead>
            <tbody>
              {proof.warnings.map((w, i) => (
                <tr key={i}>
                  <td className="mono">{w.type}</td>
                  <td>{w.store_id}</td>
                  <td className="mono">{JSON.stringify(w.detail)}</td>
                  <td>
                    {w.type === "stale_lease" && (
                      <button className="secondary" onClick={() => recover(w.store_id)}>
                        回收过期租约
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {proof && proof.violations.length > 0 && (
        <div className="panel">
          <h2>违规（须为 0）</h2>
          <table>
            <thead><tr><th>类型</th><th>门店</th><th>细节</th></tr></thead>
            <tbody>
              {proof.violations.map((v, i) => (
                <tr key={i}>
                  <td className="mono">{v.type}</td>
                  <td>{v.store_id}</td>
                  <td className="mono">{JSON.stringify(v.detail)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <div className="panel">
        <h2>逐键核证（holder / is_current 唯一性 / 租约态 / 冲突证据）</h2>
        <table>
          <thead>
            <tr>
              <th>门店</th><th>holder_run</th><th>租约态</th><th>当前段数</th>
              <th>唯一</th><th>当前 hash</th><th>当前段</th><th>区间互盖</th>
              <th>未提交草稿</th><th>冲突搁置</th><th>结论</th>
            </tr>
          </thead>
          <tbody>
            {keys.map((k) => (
              <tr key={k.store_id}>
                <td>{k.store_id}</td>
                <td className="mono">{k.holder_run || "—"}</td>
                <td><span className={`badge ${k.lease_state === "free" ? "" : k.lease_state === "held" ? "ok" : "bad"}`}>{k.lease_state}</span></td>
                <td>{k.is_current.count}</td>
                <td>{k.is_current_unique ? "是" : "否"}</td>
                <td className="mono">{k.is_current.row_hash || "—"}</td>
                <td className="mono">{k.is_current.valid_from || "—"}</td>
                <td>{k.history.overlap ? <span className="badge bad">是</span> : "否"}</td>
                <td className="mono">{k.pending_drafts.map((d) => d.run_id).join(", ") || "—"}</td>
                <td>{k.conflicts.length ? <span className="badge suspended">{k.conflicts.length}</span> : "—"}</td>
                <td>{k.ok ? <span className="badge ok">通过</span> : <span className="badge bad">失败</span>}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="panel">
        <h2>租约 version_leases</h2>
        <table>
          <thead><tr><th>门店</th><th>holder_run</th><th>取得</th><th>心跳</th><th>到期</th><th>状态</th></tr></thead>
          <tbody>
            {leases.map((l) => (
              <tr key={l.store_id}>
                <td>{l.store_id}</td>
                <td className="mono">{l.holder_run}</td>
                <td>{l.acquired_at}</td>
                <td>{l.heartbeat_at}</td>
                <td>{l.expires_at}</td>
                <td><span className={`badge ${l.active ? "ok" : "bad"}`}>{l.active ? "持租中" : "已过期"}</span></td>
              </tr>
            ))}
            {leases.length === 0 && <tr><td colSpan={6} className="muted">无租约（空闲）</td></tr>}
          </tbody>
        </table>
      </div>

      <div className="panel">
        <h2>搁置行 version_holds（甲冲突证据）</h2>
        <p className="muted">乙口径下此处为空，冲突证据改由 apply_receipts 冲突回执承载。</p>
        <table>
          <thead>
            <tr><th>id</th><th>门店</th><th>冲突跑次</th><th>挡路跑次</th><th>holder</th><th>原因</th><th>当时 is_current</th><th>时间</th></tr>
          </thead>
          <tbody>
            {holds.map((h) => (
              <tr key={h.id}>
                <td>{h.id}</td>
                <td>{h.store_id}</td>
                <td className="mono">{h.conflict_run}</td>
                <td className="mono">{h.blocking_run || "—"}</td>
                <td className="mono">{h.holder_run || "—"}</td>
                <td className="mono">{h.reason}</td>
                <td className="mono" style={{ maxWidth: 340, overflow: "hidden", textOverflow: "ellipsis" }}>
                  {JSON.stringify(h.snapshot_at_conflict)}
                </td>
                <td>{h.created_at}</td>
              </tr>
            ))}
            {holds.length === 0 && <tr><td colSpan={8} className="muted">无搁置行</td></tr>}
          </tbody>
        </table>
      </div>
    </>
  );
}
