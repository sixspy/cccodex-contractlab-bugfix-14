# 入站契约

> 遵守 `ops/底座生成前必读_效率与技术栈.md`。FastAPI + Vue 3 + SQLite，交付须 docker compose 一键起；库用 SQLite，不挂外置 Postgres。  
> **禁止同构换皮**：本仓主链路、表结构、接口与页面均独立，不得与其它仓共用同一套 `/api/items` 壳。

## 骨架一句话

Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账

## 基本信息

| 项 | 值 |
|----|-----|
| 产品名（首页） | 入站契约 |
| 目录 | `05-contractlab` |
| 领域建议 | 数据采集、爬虫与 RPA |
| 接口 | http://127.0.0.1:10739 |
| 库 | `data/app.db` |
| 账号 | admin / 123456 |

## 为何能出多题

字段增删改类型、宽严模式、影子契约、缓冲窗、生产者消费者双视角

## 避开

别做成质检签发换皮；别做成纯表单 CRUD

## 互斥切口候选

- 破坏性变更直接拒收 vs 双写缓冲
- 影子只读试跑 vs 双轨分开展示
- 契约回滚整版原子切换 vs 按生产者摘流

## 对账标齐（假绿修复）

对齐标记只来自同源快照，页面只读 `/api/reconcile` 的结论，不在前端聚合计数。

- `POST /api/reconcile/seal`
  - 甲径（默认）：写对齐标记前在同一事务内重算四类计数（通过/缓冲/拒收/缓冲行）并逐笔核对缓冲明细，不一致即拒绝标齐并记事件。
  - 乙径：先落对账草稿，同一事务内复核一致后才原子挂正式对齐标记；不一致则只保留草稿（`snapshot: null`，草稿在 `draft`）。
  - 路径当天首次选定后写入 `fix_notes` 锁定，偏离即 400 并记“假绿偏了”。
  - 两名值班同时抢点：后到者排队，只记“假绿争用”，沿用先到者封存的快照。
  - 标齐只写 `reconcile_snapshots` 与 `reconcile_events`，从不改写入站业务结果。
- `GET /api/reconcile`：结论、状态与复核卡片全部取自快照；账册有变动（包括绕过接口改库）时快照判为失效。
- `GET /api/reconcile/ledger`：对账账册、错挂/孤立缓冲行与事件流水。
- 验收：`tests/test_reconcile_snapshot.py` 先复现“计数相等但明细错挂仍标齐”的假绿，再覆盖拒绝标齐、乙径草稿、并发争用与路径偏离。

## 启动（一键）

`powershell
cd projects/05-contractlab
docker compose up -d --build
`

浏览器：http://127.0.0.1:10739/  
健康检查：GET http://127.0.0.1:10739/api/health

本机开发（可选）：

`powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --app-dir backend --host 127.0.0.1 --port 10602
python -m pytest -q
`
