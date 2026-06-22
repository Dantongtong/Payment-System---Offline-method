# 支付系统（Payment System）— 按白板设计实现，全部 mock 数据

这是对设计图的一个**可运行**实现：用内存里的 mock 数据模拟了 Payment Server、
Bank Provider、Kafka、Payment DB、Ledger DB 以及 Settlement 结算流程，不依赖任何
外部中间件，`pip install` 后直接能跑。

## 设计图 → 代码映射

| 设计图里的元素 | 对应实现 |
| --- | --- |
| API Gateway / LB + Payment Server | `app/main.py`（FastAPI 接口）+ `app/payment_service.py`（核心逻辑） |
| Bank Provider（Authorized→Processing→Completed，SLA/超时/宕机） | `app/bank_provider.py`（mock，按 client 走不同失败场景） |
| Kafka（每次状态变更都 emit；解耦 DB 写入） | `app/event_bus.py`（每个消费者独立队列+线程，类似 consumer group） |
| Payment DB（payment 表，idempotency_key 唯一约束） | `app/db.py: PaymentDB` |
| Ledger DB（双分录账本） | `app/db.py: LedgerDB` + `app/ledger.py` |
| Settlement Process（结算，FR3） | `app/settlement.py: SettlementProcessor` |
| 状态机 Started→Processing→Completed/Failed | `app/models.py`（`ALLOWED_TRANSITIONS` 严格校验） |
| 重试：retry limit + timeout + 指数退避 + jitter + DLQ | `app/retry.py` + 事件总线 DLQ |
| 幂等 idempotency_key 防重复扣款 | `PaymentDB` 唯一索引 + `create_payment` 去重 |

## 功能需求（FR）覆盖

- **FR1 商户创建支付请求** → `POST /v1/payments`（按 `idempotency_key` 幂等）
- **FR2 客户完成支付** → `POST /v1/payments/{id}/complete`（状态机 + 银行重试）
- **FR3 结算** → `POST /v1/settlements/run`（按商户净额批量结算）
- **FR4 商户查询状态** → `GET /v1/payments/{id}`

## 非功能需求如何体现

- **一致性/正确性**：状态机只允许合法跃迁；金额用整数最小单位（分），双分录账本借贷必相等。
- **防重复扣款/幂等**：`(merchant_id, idempotency_key)` 唯一；重复创建直接返回原单；重复 complete 是 no-op。
- **可用性/容错**：银行调用带指数退避+jitter 重试；重试耗尽进 DLQ，支付置为 FAILED，不会卡住主流程。
- **可扩展性/低延迟**：Payment Server 只同步写 Payment DB（事实源），账本/结算通过 Kafka 异步消费，写放大与批量操作被解耦。

## 运行方式

```bash
pip install -r requirements.txt

# ★ 实时看板（推荐）：启动服务后用浏览器打开
uvicorn app.main:app --reload
# 然后访问 http://127.0.0.1:8000/
#   顶部：连接状态 + 计数器（请求 / 处理中 / 成功 / 失败 / DLQ）
#   左侧：新建支付请求；每笔支付的「状态机轨道」随事件实时点亮
#   右侧：Kafka 事件流（终端式滚动）+ DLQ 死信
#   底部：双分录账本（借贷是否相等）+ 结算批次
#   按钮：Complete payment / Complete all pending / Run settlement / Reset
# API 文档同时在 http://127.0.0.1:8000/docs

# 一键命令行演示（无需浏览器）
python demo.py

# 跑测试
pytest -q
```

### 看板如何做到“实时”

后端用 **SSE（Server-Sent Events）** 在 `GET /v1/stream` 上把事件总线里每条事件推给
浏览器；每次状态变更（`payment.started/processing/completed/failed`）即时出现在事件流，
并驱动对应卡片的状态机轨道动起来。为了让 `PROCESSING` 这一步肉眼可见，服务端把 mock
银行延迟调到 0.25–0.6 秒（`app/main.py` 的 `_BANK_LATENCY`，可调小）。点 `c_bob` 能看到
重试退避，点 `c_erin` 能看到重试耗尽落入 DLQ。

## mock 的客户场景（用来覆盖各分支）

| client | 场景 | 结果 |
| --- | --- | --- |
| c_alice | 正常 | 一次成功 |
| c_bob | 前 2 次 503 | 重试后成功（attempts=3） |
| c_carol | 第一次超时 | 重试后成功 |
| c_dave | 硬拒付 | FAILED，**不重试** |
| c_erin | Provider 持续宕机 | 重试耗尽 → DLQ → FAILED |

## 接口速查

```
POST /v1/payments                 创建支付请求（幂等）
POST /v1/payments/{id}/complete   完成支付
GET  /v1/payments/{id}            查询单笔
GET  /v1/payments?merchant_id=    列出商户支付
POST /v1/settlements/run          运行结算
GET  /v1/settlements              结算批次
GET  /v1/ledger?payment_id=       账本明细
GET  /v1/events                   Kafka 事件日志 + DLQ
GET  /v1/reference                mock 商户/客户
GET  /                            实时看板（dashboard）
GET  /v1/stream                   SSE 实时事件流
GET  /v1/snapshot                 当前全量状态（看板启动用）
POST /v1/reset                    重建 + 重新灌入 mock 数据
```

## 说明

数据都在内存里，进程重启即清空 —— 这是一个聚焦架构的 mock 实现，不是生产代码。
要落地生产，把 `PaymentDB/LedgerDB` 换成真正的 MySQL/Postgres、把 `EventBus` 换成
真正的 Kafka、把 `BankProvider` 换成真实银行/支付通道即可，业务逻辑层基本不动。
