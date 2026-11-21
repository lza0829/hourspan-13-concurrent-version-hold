import React, { useEffect, useState } from "react";
import { api } from "../api.js";

const MODES = [
  ["hold", "甲 · 冲突入搁置", "后到写入搁置表（键+冲突 run 对+当时 is_current 快照），正式不变；核证可见 holder 与冲突对。"],
  ["rollback", "乙 · 冲突整票回滚", "后到不入搁置，整票回滚并返回冲突回执；正式不变；核证无搁置行，只认冲突回执/租约 holder。"],
];

export default function Settings() {
  const [settings, setSettings] = useState({});
  const [stubs, setStubs] = useState([]);
  const [msg, setMsg] = useState("");
  const [err, setErr] = useState("");

  const reload = () =>
    Promise.all([api("/api/settings"), api("/api/stubs")])
      .then(([s, st]) => { setSettings(s); setStubs(st.stubs || []); })
      .catch((e) => setErr(String(e.message || e)));

  useEffect(() => { reload(); }, []);

  const switchMode = async (mode) => {
    setErr("");
    try {
      const r = await api("/api/settings/conflict-mode", {
        method: "POST",
        body: JSON.stringify({ mode }),
      });
      setMsg(JSON.stringify(r, null, 2));
      await reload();
    } catch (e) {
      setErr(String(e.message || e));
    }
  };

  const active = settings._conflict_mode_active;

  return (
    <>
      <div className="topbar">
        <div>
          <h1>站规</h1>
          <p>端口 6200 / 11200 · 公有仓 lza0829/hourspan-* · 禁门店运营 CRUD 皮。</p>
        </div>
      </div>
      {err && <p className="error">{err}</p>}
      <div className="panel">
        <h2>并发冲突口径（全局只贯彻一种）</h2>
        <p className="muted">
          当前生效：<span className="badge ok">{active || "—"}</span>。
          切换后新跑次按新口径；已落搁置行保留为历史冲突证据。
        </p>
        <div className="actions">
          {MODES.map(([id, label, desc]) => (
            <button
              key={id}
              className={id === active ? "" : "secondary"}
              onClick={() => switchMode(id)}
              title={desc}
            >
              {label}{id === active ? "（生效中）" : ""}
            </button>
          ))}
        </div>
        <ul className="muted">
          {MODES.map(([id, label, desc]) => (
            <li key={id}><span className="mono">{id}</span> · {label}：{desc}</li>
          ))}
        </ul>
        {msg && <pre className="msg">{msg}</pre>}
      </div>
      <div className="panel">
        <h2>设置</h2>
        <table>
          <thead><tr><th>键</th><th>值</th></tr></thead>
          <tbody>
            {Object.entries(settings).map(([k, v]) => (
              <tr key={k}><td>{k}</td><td className="mono">{v}</td></tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="panel">
        <h2>空桩清单</h2>
        <p className="muted">concurrent_version_hold 已实现（并发持锁 · 甲/乙可切换核证）。</p>
        <ul>
          {stubs.map((s) => (
            <li key={s} className="mono">{s}</li>
          ))}
        </ul>
      </div>
    </>
  );
}
