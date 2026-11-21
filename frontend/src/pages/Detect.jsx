import React, { useEffect, useState } from "react";
import { api } from "../api.js";

export default function Detect() {
  const [runs, setRuns] = useState([]);
  const [batchId, setBatchId] = useState("");
  const [msg, setMsg] = useState("");
  const [err, setErr] = useState("");

  const reload = () =>
    api("/api/detect-runs").then(setRuns).catch((e) => setErr(String(e.message || e)));

  useEffect(() => { reload(); }, []);

  const run = async () => {
    setErr("");
    try {
      const body = batchId.trim() ? { batch_id: batchId.trim() } : {};
      const r = await api("/api/detect/run", { method: "POST", body: JSON.stringify(body) });
      setMsg(JSON.stringify(r, null, 2));
      await reload();
    } catch (e) {
      setErr(String(e.message || e));
    }
  };

  const apply = async (runId) => {
    setErr("");
    try {
      const r = await api("/api/scd/apply", {
        method: "POST",
        body: JSON.stringify({ detect_run_id: runId }),
      });
      setMsg(JSON.stringify(r, null, 2));
      await reload();
    } catch (e) {
      setErr(String(e.message || e));
    }
  };

  const abandon = async (runId) => {
    setErr("");
    try {
      const r = await api("/api/scd/abandon", {
        method: "POST",
        body: JSON.stringify({ detect_run_id: runId }),
      });
      setMsg(JSON.stringify(r, null, 2));
      await reload();
    } catch (e) {
      setErr(String(e.message || e));
    }
  };

  return (
    <>
      <div className="topbar">
        <div>
          <h1>变更检测</h1>
          <p>
            对照当前镜与快照 row_hash 产出变更清单（new / attr_change）。跑次以 open 落库 = 未提交草稿；
            同键后到 apply 将落搁置（甲·冲突入搁置），正式维不变。
          </p>
        </div>
      </div>
      <div className="panel">
        <div className="actions">
          <input
            value={batchId}
            onChange={(e) => setBatchId(e.target.value)}
            placeholder="可选 batch_id，空=各店最新快照"
            style={{ minWidth: 280, padding: "0.4rem 0.6rem", borderRadius: 8, border: "1px solid var(--line)", background: "#0c2226", color: "var(--text)" }}
          />
          <button onClick={run}>跑检测</button>
          <button className="secondary" onClick={reload}>刷新</button>
        </div>
        {msg && <pre className="msg">{msg}</pre>}
        {err && <p className="error">{err}</p>}
      </div>
      <div className="panel">
        <h2>检测跑次</h2>
        <table>
          <thead>
            <tr>
              <th>跑次</th><th>序号</th><th>批</th><th>状态</th><th>时间</th><th>变更数</th>
              <th>未提交键</th><th>搁置</th><th>操作</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((r) => (
              <tr key={r.id}>
                <td className="mono">{r.id}</td>
                <td>{r.seq ?? "—"}</td>
                <td className="mono">{r.batch_id || "—"}</td>
                <td>
                  <span className={`badge ${r.status === "open" ? "suspended" : r.status === "applied" ? "ok" : ""}`}>
                    {r.status || "closed"}
                  </span>
                </td>
                <td>{r.created_at}</td>
                <td>{r.changed_count}</td>
                <td className="mono">{(r.pending_stores || []).join(", ") || "—"}</td>
                <td>{r.held?.length ? <span className="badge suspended">{r.held.length}</span> : "—"}</td>
                <td>
                  {r.status === "open" ? (
                    <span className="actions">
                      <button onClick={() => apply(r.id)}>应用</button>
                      <button className="secondary" onClick={() => abandon(r.id)}>放弃</button>
                    </span>
                  ) : (
                    <span className="muted">—</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
