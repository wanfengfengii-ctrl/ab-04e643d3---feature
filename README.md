# Experiment Event Service（大型实验装置多流事件服务）

纯后端服务：采集代理以**生产者事务**方式写入多流联动观测，分析程序以**不可变快照 + 不透明游标**读取整批原子可见的数据。所有权威状态持久化在 PostgreSQL 中，API 进程无状态、可随时崩溃、可水平扩容；并发安全由数据库行锁、单个全局序列、部分唯一索引和事务级 advisory lock 保证，**不依赖任何进程内互斥锁**。

- 语言/运行时：Python 3.11 + FastAPI + asyncpg
- 持久化依赖：PostgreSQL 16
- 克隆后只需 Docker：`docker compose up --build`

---

## 1. 快速开始

```bash
docker compose up --build           # 构建并启动 db + api，并运行一次性 verify 验收
# API 地址: http://localhost:${API_PORT:-8080}

docker compose run --rm verify      # 重新运行一次性验收
docker compose logs -f api          # 查看 API 日志
```

环境变量（均有默认值，生产默认**关闭**故障注入）：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `API_PORT` | `8080` | 宿主机映射端口（容器内固定 8080） |
| `DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME` | `db/5432/eventsvc/...` | 数据库连接 |
| `MAX_EVENTS_PER_BATCH` | `100` | 单批事件上限 |
| `MAX_BATCH_BYTES` | `1048576` | 单批规范化后字节上限 |
| `MAX_EVENT_PAYLOAD_BYTES` | `65536` | 单事件载荷字节上限 |
| `DEFAULT_SNAPSHOT_TTL_SECONDS` | `3600` | 快照默认 TTL |
| `MAX_SNAPSHOT_TTL_SECONDS` | `604800` | 快照 TTL 上限 |
| `RETENTION_NO_GROUP_POLICY` | `reject` | 无消费组时回收策略：`reject`（拒绝回收）/ `delete`（按时间地平线回收） |
| `RETENTION_NO_GROUP_HORIZON_SECONDS` | `86400` | `delete` 策略下的提交时间地平线 |
| `CONSUMER_GROUP_IDLE_TTL_SECONDS` | `0` | 消费组闲置失效秒数；`0` = 永不失效。失效判定仅依据持久化的 `last_ack_at`，不依赖进程内状态 |
| `FAULT_POINTS` | 空 | 故障注入（逗号分隔）：`crash-after-commit`、`crash-during-reclaim`。**生产必须为空** |
| `FAULT_TOKEN` | 空 | 触发已装载故障点所需令牌 |
| `CLOCK_OVERRIDE_TOKEN` | 空 | 确定性时钟覆写令牌；为空时禁止 `X-Now`。Compose 验收环境设为 `acceptance-clock` |

健康检查：

| 路径 | 用途 |
|---|---|
| `GET /health/live` | 进程存活（不检查依赖） |
| `GET /health/ready` | 进程 **且** PostgreSQL 可用；数据库不可用时返回 `503` |

所有时间判断使用**服务端 UTC**。验收测试通过带令牌的 `X-Now: <unix秒>` + `X-Now-Token` 请求头注入确定性时间；生产未配置令牌时该头返回 `CLOCK_OVERRIDE_DISABLED`。

---

## 2. 通用契约（稳定）

- API 版本前缀：`/v1`；请求与响应均为 `application/json; charset=utf-8`。
- 时间字段均为 UTC ISO-8601 字符串。
- 错误响应形状固定：

```json
{ "error": { "code": "FENCED", "message": "human readable",
             "details": { "currentEpoch": 2 } } }
```

### 2.1 名称格式（受限 ASCII）

`producer`、`txId`、消费组名、流名、事件 `key` 全部遵守同一规则：

- 1–128 个字符；
- 首字符必须是 `[A-Za-z0-9]`；
- 其余字符为 `[A-Za-z0-9._-]`；
- 正则：`[A-Za-z0-9][A-Za-z0-9._-]{0,127}`。

### 2.2 数值与 JSON 严格校验

- 协议整数（`epoch`、`firstSequence`、位置、`pageSize`、`ttlSeconds`、计数）必须是 JSON 整数：
  **拒绝布尔值（`true/false`）、小数（`1.5`、`1.0`）、指数形式（`1e3`）、越界值与字符串**。
- 事件载荷接受任意可规范化 JSON；**拒绝重复对象键、`NaN`/`Infinity`/`-Infinity`、非 UTF-8**。
- 规范化规则：对象键按 Unicode 码点排序、紧凑分隔符、UTF-8、最短往返浮点表示。
- 校验失败返回 `400 VALIDATION_ERROR` / `400 INVALID_JSON` / `413 BATCH_TOO_LARGE`，且**不创建任何**生产者、事务、快照或确认记录。

### 2.3 错误码 → HTTP 状态码（稳定表）

| code | status | 含义 |
|---|---|---|
| `VALIDATION_ERROR` | 400 | 字段校验失败（名称/整数范围/类型） |
| `INVALID_JSON` | 400 | 非法 JSON、重复键、非有限数 |
| `INVALID_TIME` | 400 | `X-Now` 非法或越界 |
| `CLOCK_OVERRIDE_DISABLED` | 400 | 服务未开启时钟覆写 |
| `CLOCK_OVERRIDE_FORBIDDEN` | 403 | 时钟令牌错误 |
| `PRODUCER_NOT_FOUND` | 404 | 生产者不存在 |
| `TRANSACTION_NOT_FOUND` | 404 | 事务不存在（或已被回收） |
| `SNAPSHOT_NOT_FOUND` | 404 | 快照不存在（未过期的查询用） |
| `CONSUMER_GROUP_NOT_FOUND` | 404 | 消费组不存在 |
| `EPOCH_NOT_CURRENT` | 409 | 注册 epoch 低于当前，或使用未注册的更高 epoch |
| `FENCED` | 409 | 旧 epoch 的写入/提交已被更高 epoch 隔离 |
| `TX_ID_REUSED` | 409 | 同 `(producer, txId)` 出现内容差异，禁止覆盖 |
| `OPEN_TRANSACTION_EXISTS` | 409 | 同 epoch 已存在未完成事务 |
| `INVALID_FIRST_SEQUENCE` | 409 | `firstSequence` ≠ 该生产者最后已提交序号 + 1 |
| `BATCH_NOT_WRITTEN` | 409 | 未写入批次即提交 |
| `BATCH_TOO_LARGE` | 413 | 批次/载荷超过配置尺寸 |
| `TERMINAL_CONFLICT` | 409 | 提交与中止竞争后败方调用；`details.status` 给出实际终态与 `commitPosition` |
| `CURSOR_INVALID` | 400 | 游标缺失、篡改、签名不符或绑定的流集合不匹配 |
| `CURSOR_EXPIRED` | 410 | 快照 TTL 已过或快照已不存在 |
| `ACK_POSITION_INVALID` | 400 | 确认位置非法 |
| `ACK_POSITION_REGRESSED` | 409 | 确认位置回退 |
| `RETENTION_NOTHING` | 409 | 默认策略下不存在任何消费组，拒绝回收 |

`CURSOR_EXPIRED` 的 `details` 固定包含：

```json
{ "earliestAvailablePosition": 12,
  "snapshotExpiresAt": "2030-01-01T00:00:00+00:00",
  "createSnapshot": { "method": "POST", "path": "/v1/snapshots",
                      "body": { "streams": ["..."], "ttlSeconds": 3600, "pageSize": 100 } } }
```

不会静默从现存最早记录继续读取。

### 2.4 状态机（稳定）

```
生产者 epoch: created -> (同 epoch 幂等) -> epoch+ 提升（旧 epoch 未完成事务立即 fenced）

事务: open ──commit──▶ committed(immutable, 分配 commitPosition)
      ├──abort──▶ aborted
      └──被更高 epoch 抢占──▶ fenced（终态；显式 abort 幂等返回 fenced，不改变终态）
committed / aborted / fenced 均为终态，记录不可变。
```

---

## 3. 端点契约

### 3.1 生产者会话

#### `POST /v1/producers`
```json
请求: {"name": "laser-a", "epoch": 1}
```
- 201 `{"producer","epoch":1,"status":"created"}`
- 200 重复同 epoch：`"status":"already_current"`；提升到更高 epoch：`"status":"fenced_previous"`
- 409 `EPOCH_NOT_CURRENT`：epoch 低于当前（`details.currentEpoch`）
- 提升成功的同一数据库事务内，该生产者所有更低 epoch 的 `open` 事务立即变为 `fenced`，立刻失去提交资格。

#### `GET /v1/producers/{name}` → `{producer, epoch, createdAt, updatedAt}`

### 3.2 事务

#### `POST /v1/transactions`
```json
{"producer": "laser-a", "txId": "run-42", "epoch": 1}
```
- 201 创建（`status=open`）；同 `(producer, txId, epoch)` 重试返回 200 原记录。
- 每个 `(producer, epoch)` 同时最多一个 `open` 事务，否则 409 `OPEN_TRANSACTION_EXISTS`。
- epoch 低于当前 → 409 `FENCED`；epoch 高于已注册值 → 409 `EPOCH_NOT_CURRENT`。
- 同 txId 不同 epoch → 409 `TX_ID_REUSED`。

#### `PUT /v1/transactions/{producer}/{txId}/batch`（一次性写入批次）
```json
{
  "epoch": 1,
  "firstSequence": 1,
  "events": [
    {"stream": "spectrometer", "key": "peak-1", "payload": {"hz": 12.5, "ok": true}},
    {"stream": "camera",       "key": "frame",  "payload": {"exposure": 0.02}}
  ]
}
```
- `events`：1–100 条；序号由 `firstSequence` 起在批内**连续隐含生成**。
- 接纳条件：`firstSequence == 该生产者最后已提交序号 + 1`；中止/fence 不消耗序号。
  不满足 → 409 `INVALID_FIRST_SEQUENCE`（`details.expectedFirstSequence`、`details.lastCommittedSequence`）。
- **幂等重试**：同 `(txId, epoch)` 且规范化后批次完全相同（含相同 `firstSequence`）→ 200 返回同一事务，不覆盖。
- 任何内容差异（事件不同或 `firstSequence` 不同）→ 409 `TX_ID_REUSED`。
- 对 `fenced` 事务的写入 → 409 `FENCED`；对终态事务写不同内容 → 409 `TX_ID_REUSED`。
- 超过批大小 → 413 `BATCH_TOO_LARGE`。

#### `POST /v1/transactions/{producer}/{txId}/commit`
```json
{"epoch": 1}
```
- 200（含首次成功提交与重试）：
```json
{"producer":"laser-a","txId":"run-42","epoch":1,"status":"committed",
 "firstSequence":1,"eventCount":2,"commitPosition":7,
 "committedAt":"2030-01-01T00:00:00+00:00"}
```
- 提交在**一个**数据库事务中：获取全局 advisory lock → 分配不可变 `commitPosition`（全局严格递增，来自全局 sequence，重启不复用）→ 写入全部事件 → 置 `committed`。整批同时可见。
- 提交已持久化但响应丢失：重启后重试 commit 或 `GET` 都返回**原 `commitPosition`**，不会产生第二批事件。
- 更高 epoch 建立后，对已经 `committed` 的旧事务重试提交：仍按 txId 识别并返回既有 `committed` 结果，**绝不误报 `FENCED`**。
- 对 `aborted` 事务提交 → 409 `TERMINAL_CONFLICT`，`details: {status:"aborted", commitPosition:null}`。
- 对 `fenced` 事务提交 → 409 `FENCED`。未写批次 → 409 `BATCH_NOT_WRITTEN`。

#### `POST /v1/transactions/{producer}/{txId}/abort`
- 首次中止 → 200 `status:"aborted"`；重复中止 → 200 原结果（幂等）。
- 与提交竞争：只有一个终态获胜。对 `committed` 事务调 abort → 409 `TERMINAL_CONFLICT`，`details: {status:"committed","commitPosition":N}`。
- 中止不消耗生产者序号。

#### `GET /v1/transactions/{producer}/{txId}`
返回事务当前资源表示（字段同上）。用于提交结果不确定时消歧。404 = 从未存在或已被回收。

### 3.3 读取快照与游标

#### `POST /v1/snapshots`
```json
{"streams": ["spectrometer", "camera"], "ttlSeconds": 600, "pageSize": 100}
```
- 201：
```json
{"snapshotId":"<uuid>", "streams":[...], "highWatermark": 7,
 "createdAt":"...", "expiresAt":"...", "pageSize":100,
 "page": {"transactions":[ <envelope>... ],
          "transactionCount": 2,
          "nextCursor": "<opaque>",
          "hasMore": true}}
```
- **不可变高水位** `highWatermark`：创建时已提交的最大 `commitPosition`；快照创建之后的新提交永不混入该快照。
- 流集合为非空、去重、受限名称数组（1–100 个）。
- `pageSize`：1–1000，表示**事务信封数**（不是事件数）。

#### `GET /v1/streams/read?cursor=<opaque>&pageSize=100`
响应中的 `page` 形状同上（并回带 `snapshotId/streams/highWatermark`）。

读取语义（稳定）：

1. 页面以**事务信封**为单位，按 `commitPosition` 严格递增；**单个事务绝不跨页拆分**。
2. 只返回与所选流集合**至少有一条事件匹配**的事务；信封中包含该事务在**所有所选流**中的完整事件子集（未选流的事件不出现）。
3. 游标为不透明字符串，绑定 `{snapshotId, streams(集合), nextPosition}`，带 HMAC-SHA256 签名（密钥持久化在数据库，所有 API 实例共享，重启有效）。
   - 篡改、伪造、结构损坏、用于其他流集合 → 400 `CURSOR_INVALID`。
4. 续读可在**任意健康 API 实例**上进行，不重复、不漏读：
   - 当页取满 `pageSize` 个信封且后面还有数据时 `hasMore=true`；
   - 游标中的 `nextPosition` 为最后已交付信封的 `commitPosition`（首页之前为 0）；
   - `hasMore=false` 后继续续读返回空页与稳定新游标。
5. 快照 TTL 过期（或快照已被回收）后任何旧游标 → **410 `CURSOR_EXPIRED`**，带 `earliestAvailablePosition` 与创建新快照所需信息。

事务信封形状：

```json
{
  "commitPosition": 7,
  "producer": "laser-a",
  "txId": "run-42",
  "epoch": 1,
  "firstSequence": 1,
  "committedAt": "2030-01-01T00:00:00+00:00",
  "events": [
    {"stream":"spectrometer","key":"peak-1","sequence":1,"payload":{"hz":12.5}},
    {"stream":"camera","key":"frame","sequence":2,"payload":{"exposure":0.02}}
  ]
}
```

#### `GET /v1/snapshots/{id}`、`DELETE /v1/snapshots/{id}`（管理）

### 3.4 消费组

#### `POST /v1/consumer-groups`
```json
{"name": "analysis-ml"}
```
201 创建（`ackPosition=0`）；同名重试 200 `status:"already_exists"`。

#### `POST /v1/consumer-groups/{name}/ack`
```json
{"position": 12}
```
- 位置只能单调前进：前进 → 200 `"status":"advanced"`；重复相同位置 → 200 `"status":"idempotent"`。
- 回退 → 409 `ACK_POSITION_REGRESSED`（`details.currentPosition`、`details.requestedPosition`）。
- 组不存在 → 404 `CONSUMER_GROUP_NOT_FOUND`（不会隐式建组）。

#### `GET /v1/consumer-groups/{name}`
#### `GET /v1/consumer-groups` / `DELETE /v1/consumer-groups/{name}`（管理；删除组是显式放弃历史保护）

**闲置失效**：`CONSUMER_GROUP_IDLE_TTL_SECONDS > 0` 时，在任何回收/状态查询路径上，按持久化时间戳删除 `last_ack_at` 早于 `now - ttl` 的组；从未确认（`last_ack_at = NULL`）的组不失效。该机制在重启后依旧正确，不使用进程内状态。

### 3.5 历史回收

#### `POST /v1/retention/reclaim`
分两个独立提交的阶段：先在自己的事务中**持久化**过期快照回收与闲置消费组失效（即使随后判定"无组可删"而返回 409，这一阶段也不会回滚）；随后在持有提交顺序 advisory lock 的事务中完成回收决策与整封删除。

只删除**同时**满足下列条件的**完整已提交事务信封**（严格整封删除，禁止只删部分事件）：

1. `commitPosition` **严格低于每一个**有效消费组的确认位置（即 `< MIN(ack_position)`）；
2. `commitPosition` **严格高于每一个**未过期快照的高水位（即 `> MAX(snapshot.highWatermark)`，任何存活快照都不需要它）。

- 不存在任何消费组时：
  - `RETENTION_NO_GROUP_POLICY=reject`（默认）：409 `RETENTION_NOTHING`，不删除任何数据；
  - `=delete`：仅删除 `committed_at <= now - RETENTION_NO_GROUP_HORIZON_SECONDS` 的信封，仍受存活快照约束。
- 响应：

```json
{"reclaimedTransactionCount": 3, "reclaimedEventCount": 11,
 "positions": [3, 5, 7], "ackFloorPosition": 12,
 "snapshotWatermarkCeiling": 2, "expiredSnapshotCount": 1,
 "earliestAvailablePosition": 10, "reclaimedAt": "..."}
```

并发确认、续读、回收在数据库层产生可串行化的结果（消费组行锁 + 提交顺序 advisory lock；读取使用快照既定水位，不被回收结果破坏）。

#### `GET /v1/retention/status`
返回 `earliestAvailablePosition`、`latestPosition`、`activeConsumerGroupCount`、`minAckPosition`、`liveSnapshotCount`、`snapshotWatermarkCeiling`、`noGroupPolicy`、`asOf`。

---

## 4. 故障注入（仅验收环境）

默认关闭。验收时以 `FAULT_POINTS` 显式装载，并可要求请求体携带正确的 `faultToken`：

| 故障点 | 触发时机 | 验证的恢复性质 |
|---|---|---|
| `crash-after-commit` | commit 数据库事务已提交（持久化）**之后**、HTTP 响应返回之前，`_exit(137)` 硬终止 | 重启后无可见半事务、无重复 `commitPosition`、重试返回原位置、无第二批事件 |
| `crash-during-reclaim` | 回收事务持锁、选出候选信封之后、删除之前硬终止 | 数据库事务整体回滚：无半删信封、已回收记录不复活、确认位置不变 |

## 5. 确定性时间

`X-Now: <posix-seconds>` + `X-Now-Token: <CLOCK_OVERRIDE_TOKEN>` 对所有读写生效，TTL 边界测试无需 sleep；未配置令牌的生产环境拒绝该头。

---

## 6. 持久化与并发设计要点

- `transactions`：`(producer, tx_id)` 主键；`commit_position` 全局唯一；
  部分唯一索引 `ux_open_tx_per_epoch` 保证每个 epoch 至多一个 `open` 事务。
- `events`：`(commit_position, ordinal)` 主键，并有指向事务信封的外键（`ON DELETE RESTRICT`），配合“先删事件再删信封”的单事务删除，杜绝半封残留。
- `commit_position_seq`：全局 BIGINT sequence；`nextval` 在持全局 advisory lock 的提交事务中调用，提交完成顺序即位置顺序，崩溃不回滚已分配值（允许空洞，绝不重复）。
- 游标 HMAC 密钥存于 `settings` 表，所有实例共享。
- 快照、消费组均为持久化行；重启后游标、TTL、确认位置、回收判定全部延续。

## 7. 测试与验收

```bash
docker compose up --build            # 端到端：pytest 契约套件 + 两个崩溃点恢复 + 跨实例游标续读
docker compose run --rm verify       # 仅重跑一次性 verify 服务
```

`tests/` 中的测试只通过 HTTP 验证契约（校验、epoch/fencing、序号、幂等、终态竞争、快照分页/篡改/TTL、消费组单调确认与回收）。`scripts/verify.py` 额外在本容器内拉起带故障注入的临时 API 工作进程（与常驻 API 共享同一 PostgreSQL），验证：

1. 提交持久化后/响应前崩溃 → 健康实例可见唯一已提交信封，重试返回同一 `commitPosition`，事件不重复、序号连续；
2. 回收期间崩溃 → 整体回滚，信封完整、确认位置不变，之后正常回收成功且不复活；
3. 在实例 A 创建快照，在实例 B 上用游标续读，无重复无遗漏。

## 8. 目录

```
app/            FastAPI 应用（config/db/validation/main）
tests/          HTTP 黑盒契约测试（pytest）
scripts/verify.py  一次性验收服务
Dockerfile      API / verify 镜像
docker-compose.yml  db + api + verify，仅依赖 Docker
```
