# 时档（HourSpan）

**骨架：** 门店营业属性贴源入档 → 业务键变更检测 → Type2 历史行开闭 → 当前镜投影 → 历史与当前互证

- 领域：数据工程（去皮=维表 SCD / 历史开闭 / 当前镜 / 核证；不是门店后台 CRUD）
- 端口：前端 **6200**，接口 **11200**（避开 Chrome 封锁的 6000–6063）
- 公有仓前缀：`lza0829/hourspan-NN-*`
- 栈：Python 3.12 + FastAPI + SQLite；前端 **React + Vite**
- 避让：勿撞筑层分层汇总、价舱改价调度、信汇多源去重、变流 CDC 游标、格湾占用态

本地冒烟：

```powershell
cd claude-workspace/hourspan/_scaffold/backend
pip install -r requirements.txt
$env:DATA_DIR="$PWD/data"; $env:FIXTURES_DIR="$PWD/../fixtures"
python -m uvicorn app.main:app --host 127.0.0.1 --port 11200

cd ../frontend
npm install
npm run dev
```

或：`docker compose up -d --build`（6200 / 11200）

## 并发版本持锁（concurrent_version_hold）

同键（`store_id`）两检测或 Type2 应用交错时的落库口径。全局只贯彻一种架构，
`settings.conflict_mode` 切换，默认 **甲·冲突入搁置**：

| | 甲 · 冲突入搁置（默认） | 乙 · 冲突整票回滚（对照核证） |
|---|---|---|
| 后到冲突 | 写 `version_holds` 搁置行（键 + 冲突 run 对 + 当时 is_current 快照），正式不变 | 整票回滚，不入搁置，返回 `apply_receipts` 冲突回执，正式不变 |
| 核证证据 | 搁置行 + holder_run | 冲突回执 + 租约 holder，无搁置行 |

不变量（两架构同）：
- 同键单持租，正式链恒为**单段当前**（DB 唯一索引兜底）；
- 先到未释租 / 未关闭草稿时后到不得写正式维；先到仍可完成；
- 禁止后到覆盖先到、禁止区间互盖、禁止释租后双当前；
- 邻键并行不受本键租约误伤；先到完成后释放租约，第三者才能再写。

接口：
- `GET  /api/concurrency/proof[?store_id=]` — 只读核证：holder_run / 冲突证据 / is_current 唯一性 / 租约态
- `GET  /api/concurrency/leases`、`GET /api/concurrency/holds`
- `POST /api/settings/conflict-mode {mode: hold|rollback}` — 全局切口径
- `POST /api/scd/abandon {detect_run_id}` — 先到让位（撤下未提交草稿）
- `POST /api/concurrency/lease/{store_id}/recover` — 回收过期租约

测试：`cd backend && python -m pytest tests/ -q`（17 项，甲乙各场景 + 真并发线程）。

