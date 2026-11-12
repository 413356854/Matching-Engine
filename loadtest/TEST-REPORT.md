# 撮合引擎压测报告

## 1. 测试环境

| 项目 | 配置 |
|---|---|
| 机器 | 单机 4C/8T（笔记本） |
| 组件 | Kafka 4.4.0 / MySQL / QuestDB 10.1.0 / 应用同机 |
| JVM | `-Xms512m -Xmx1280m -XX:+UseG1GC` |
| 磁盘 | QuestDB 测试时 C 盘可用约 52G |
| 账户 | 200 个用户预充值；每个维度重启应用并重新充值，避免状态污染 |
| 预热 | 10,000 单，预热后等待全部 topic 排空 |

**入口**
- HTTP：压测客户端逐单调用 `POST /api/v1/currency/orders`，`users=200`，常用 `concurrency=32`。
- Kafka：调用批量接口（服务端并发循环走 Disruptor），或历史轮次直接写 `OrderCommandBatch` topic。

**TPS 口径**

```
TPS = accepted / (最后一条清算消息消费完成的时刻 - 第一笔下单提交的时刻)
```

`accepted` 是入口成功受理的笔数（HTTP 202 / 批量接口 accepted）；rejected 单独记录。
结束条件为 `account-trade-settlement-consumer` 的 lag 回到 pre-load 基线（事务 commit marker
导致基线常驻 lag=1）。指标只统计到清算消费完成，funds-flow / marketdata 不计入 TPS，
但会在指标结束后等待全部 topic 排空。

> 历史报告中的 `pipeline-trace` / `consumer-timing` / `hot-path` 埋点及对应诊断接口
> 已在演示开发收尾时移除。本报告中的诊断轮数据为历史实测值，不能按原命令复现。

## 2. 批量消息改造（核心）

四个下游输出 topic 改为 `-batch` 批量消息后，以 100k c32 HTTP 为主对比口径。

### 2.1 端到端结果（100k c32）

| 指标 | Fory 逐条基线 | 批量消息 | 变化 |
|---|---:|---:|---:|
| 发送耗时 | 36.987 s | **19.435 s** | −47.5% |
| 受理 TPS（HTTP 202） | 2,703.7 | **5,145.3** | **+90.3%** |
| HTTP p50 / p99 | 5.57 / 103.91 ms | **4.21 / 42.48 ms** | −24% / −59% |
| 排空 | 6 s | **2 s** | −67% |
| 总时间（发送 + 排空） | 42.99 s | **21.44 s** | **−50.1%** |
| 平均端到端速率 | 2,326 /s | **4,665 /s** | **+100.6%** |

- 100,000 单全部 HTTP `202`，0 ERROR；结束后 `openOrders=0`，余额与充值基线一致。
- 第二轮重复性验证：受理 TPS 4,878.3、总时间 23.50 s，与第一轮基本一致。

### 2.2 Kafka 发布成本（100k c32）

| topic | 记录数（基线 → 批量） | send 总耗时 | serialize 总耗时 |
|---|---:|---:|---:|
| `me.funds` → `me.funds-batch` | 300,000 → **279** | 4,902 → **346 ms**（−93%） | 1,400 → **319 ms** |
| `me.account.settlements` → `-batch` | 50,000 → **107** | 2,893 → **74 ms**（−97%） | 1,363 → **70 ms** |
| `me.order.freeze-results` → `-batch` | 100,000 → **124** | 2,716 → **111 ms**（−96%） | 555 → **38 ms** |
| `me.marketdata` → `-batch` | 50,000 → **107** | 747 → **24 ms**（−97%） | 389 → **19 ms** |
| 四个输出 topic 合计 | **500,000 → 617（−99.88%）** | **11,258 → 555 ms（−95.1%）** | **3,707 → 446 ms（−88.0%）** |

消费链 listener 耗时（每批平均）：
- account-command：70.9 → **10.1 ms**（−86%）
- order-freeze-result：116.6 → **12.0 ms**（−90%）
- account-settlement：47.1 → **16.6 ms**（−65%）
- order-command：18.3 → **8.1 ms**（−56%）
- funds-flow / marketdata 受 QuestDB/JVM 停顿毛刺影响，单轮平均值被拉高；两者不在下单关键路径，结束后 lag 均为 0。

### 2.3 200k c32 参考轮

| 指标 | 基线 | 批量消息 | 变化 |
|---|---:|---:|---:|
| 发送耗时 | 57.069 s | **45.305 s** | −20.6% |
| 受理 TPS | 3,504.5 | **4,414.6** | +26.0% |
| 总时间 | 79.07 s | **49.31 s** | −37.6% |
| 平均端到端速率 | 2,529 /s | **4,056 /s** | +60.4% |
| 12 段 p50 合计 | 2,422.5 ms | **874.4 ms** | −63.9% |

第二轮受理 TPS 4,717.2、总时间 47.40 s。两轮均全部 `202`，0 ERROR，
无 `RecordTooLarge`，结束后余额守恒、`openOrders=0`。

### 2.4 结论

1. 批量消息按预期消除发布成本：四个输出 topic 记录数从 50 万降到 617 条，send −95%、serialize −88%。
2. 消费链 listener 明显变快，account-command −88%、order-freeze-result −90%。
3. 端到端 p50 下降 45%，100k c32 受理 TPS 5,145.3 / 4,878.3，远高于逐条基线 2,703.7。
4. 剩余瓶颈是队列等待；commit 已只占约 4.3%。未改造的 `me.account.commands`
   事务竞争集中在冻结命令生产路径，是后续批量化目标。
5. 新的尾部成本在 funds-flow（QuestDB 归档），但不在订单关键路径。

## 3. 委托消息批量化（OrderCommandBatch）

改动：`me.order.commands` 逐条 `OrderCommand` → Disruptor 聚合（2000 条 / 20ms）→
`OrderCommandBatch` 写入 `me.order.commands-batch`，consumer 每 poll 最多 10 条批量记录。
100,000 单 + 10,000 预热，s1 只保留 order-command 消费者，s6 全链路。

### 3.1 吞吐

| 用例 | 维度 | 指标 | 旧版 | 新版均值 | 均值变化 |
|---|---|---|---:|---:|---:|
| s1-order-command | HTTP | ingestTps | 5,368.5 | 5,661.6 | +5.5% |
| s1-order-command | HTTP | totalTps | 4,654.4 | 4,881.1 | +4.9% |
| s1-order-command | Kafka | ingestTps | 49,811.9 | **119,461.9** | **+139.8%** |
| s1-order-command | Kafka | totalTps | 19,523.6 | **26,863.5** | **+37.6%** |
| s6-full | HTTP | ingestTps | 3,788.1 | 3,808.9 | +0.6% |
| s6-full | HTTP | totalTps | 3,379.4 | 3,391.6 | +0.4% |
| s6-full | Kafka | ingestTps | 31,206.1 | **126,379.0** | **+305.0%** |
| s6-full | Kafka | totalTps | 15,860.4 | 19,512.8 | +23.0% |

### 3.2 Kafka 记录数（order-command runner 实际消费）

| 用例 | 旧版 | 新版 run1 | 新版 run2 | 降幅 |
|---|---:|---:|---:|---:|
| s1-order-command HTTP | 100,000 | 715 | 657 | −99.3% |
| s1-order-command Kafka | 100,000 | 50 | 50 | **−99.95%** |
| s6-full HTTP | 100,000 | 241 | 346 | −99.7% |
| s6-full Kafka | 100,000 | 50 | 50 | **−99.95%** |

### 3.3 结论

1. Kafka 维度 100,000 单压缩为 50 条批量命令；HTTP 维度约为 137–415 单/条。
2. 隔离的 order-command 链路明显变快：Kafka ingest 约 2.4x、totalTps +37.6%，
   process 总耗时约减半，事务 commit 总耗时 −96%。
3. HTTP 维度基本不变，s1/s6 totalTps 变化在 ±5% 内；入口（HTTP/JSON/同步校验）
   本身是瓶颈，批量消息无法提升受理吞吐。
4. 全链路 Kafka 维度波动较大（两轮 totalTps 14.7k / 24.3k，旧 15.9k）；
   下游单事务最多处理 10 条 batch × 2000 = 20k 元素，drain 由下游批处理调度决定。

## 4. 端到端 TPS（最新一轮）

### 4.1 HTTP c32（2026-11-12 22:46，最新）

| 指标 | 数值 |
|---|---:|
| 订单数 | 200,000 BTC-USDT LIMIT |
| 并发 | users=200，concurrency=32 |
| 入口受理 TPS | **5,038.5** |
| 灌入耗时 | 39.695 s |
| 清算排空 | 2.086 s |
| 总时间 | 41.780 s |
| **端到端 TPS** | **4,787.0** |
| errors | 0 |

HTTP 维度 TPS 由入口（Tomcat/JSON/同步校验 + 压测客户端）主导，清算只占约 2 秒。
历史 100k HTTP 基线 TPS 3,596.6 已被本轮 200k c32 数据取代。

### 4.2 Kafka 单 symbol 200k

| 轮次 | accepted | totalSec | TPS | rejected |
|---|---:|---:|---:|---:|
| eventId 优化前（干净基线） | 199,253 | 8.956 | **22,248.4** | 747 |
| eventId 优化后 #1 | 199,968 | 10.985 | 18,203.7 | 32 |
| eventId 优化后 #2 | 199,648 | 9.184 | 21,738.7 | 352 |
| eventId 后诊断轮（pipeline-trace 1/10） | 200,000 | 9.099 | 21,980.4 | 0 |

两轮干净结果均值约 **19.97k**。eventId 优化消除了 settleLocked 的 UUID 成本，
但端到端 TPS 没有明显提升，瓶颈转移到单 symbol 串行链的队列等待。

### 4.3 Kafka 双 symbol 400k（清算优化后的最新一轮）

场景：400,000 单（BTC-USDT 200k + ETH-USDT 200k），批量接口并发提交，
`account-settlement` concurrency=8。

| 方案 | TPS | drain | apply.lockWait | settle.lockWait |
|---|---:|---:|---:|---:|
| 清算拆分 + slot 分区 | 25,023.7 | 13.921 s | 517 ms | 796 ms |
| 全局单锁（仅作上界参考） | 27,121.7 | 13.710 s | 6,529 ms | 4,785 ms |
| **symbol 冻结 + 批内 8 线程 + settlement slot（最终方案）** | **26,813.0** | **12.867 s** | **140 ms** | **139 ms** |

最终方案恢复冻结 topic 的 `key=symbol`，同一 symbol 由单 consumer 顺序消费，
批内再按 `slot(userId)` 交 8 线程并行，最后按原始顺序汇合，保证同 symbol 价格-时间优先。
账户锁等待已基本消除，关键路径转移到撮合阶段（`match.total` 约 12.5s，
`freezeResult.onResult.total` 约 18.9s）。

## 5. QWP 写入与 marketdata 优化

### 5.1 QWP clean 基线（100k）

| 用例 | 维度 | ingestTps | totalTps | drainSec |
|---|---|---:|---:|---:|
| s1-order-command | HTTP | 5,436.9 | 4,571.8 | 3.480 |
| s1-order-command | Kafka | 91,333.4 | 21,857.9 | 3.480 |
| s6-full | HTTP | 4,040.7 | 3,466.7 | 4.098 |
| s6-full | Kafka | 86,272.2 | 19,394.9 | 3.997 |

QWP 切换后 WAL 健康：`funds_flow` 661,200 行，`writerTxn == sequencerTxn`，
所有表 `suspended=false`，无写入失败。

### 5.2 marketdata 优化（最后 20 条成交 + K 线批次节流）

| 指标 | 优化前 | 优化后 | 变化 |
|---|---:|---:|---:|
| s6 HTTP marketdata 堆积积分 | 848.4 | **243.5** | **−71%** |
| s6 HTTP marketdata 峰值 | 135 | **60** | **−56%** |
| s6 HTTP marketdata listener | 372.6 ms/批 | **253.5 ms/批** | −32% |
| s6 Kafka marketdata listener | 132.7 ms/批 | **64.5 ms/批** | −51% |
| 200k 成交写入 `md_trade` | — | **1,152 行** | 写放大显著收敛 |
| 200k 成交写入 `md_kline` | — | **11 行** | 同上 |

HTTP 端到端 totalTps 基本不变（入口仍是瓶颈），但下游 QuestDB 压力明显下降；
最大堆积 topic 仍是 `me.funds-batch`。

### 5.3 funds-flow 单分区实验

固定 `key=batch` + 单 consumer 后，s6 HTTP totalTps 3,440.0 → 3,304.5（−3.9%），
funds-batch backlog +67%；Kafka 维度 totalTps 16,708.4 → 15,738.1（−5.8%），
backlog +224%。**结论：不划算，已回退到按 userId 分区 + 8 consumer。**

## 6. 12 段链路耗时（最新，200k 诊断轮）

eventId 优化后的单笔 p50（诊断轮，200k，`pipeline-trace` 1/10）：

| # | 阶段 | p50 (ms) | 占比 |
|--:|---|---:|---:|
| 1 | 接单 → Kafka ack | 181.6 | 3.7% |
| 2 | Kafka ack → 委托消费 | **2,739.3** | **56.4%** |
| 3 | 委托消费 → 冻结入队 | 143.4 | 3.0% |
| 4 | 冻结入队 → 冻结 commit | 27.6 | 0.6% |
| 5 | 冻结 commit → 账户消费 | 177.0 | 3.6% |
| 6 | 账户消费 → 冻结完成 | 0.0 | 0% |
| 7 | 冻结完成 → 回执 commit | 151.9 | 3.1% |
| 8 | 回执 commit → 委托收到回执 | **727.4** | **15.0%** |
| 9 | 委托收到回执 → 撮合 | 0.0 | 0% |
| 10 | 撮合完成 → 清算入队 | 183.1 | 3.8% |
| 11 | 清算入队 → 清算 commit | 33.9 | 0.7% |
| 12 | 清算 commit → 清算完成 | **492.0** | **10.1%** |
| | **p50 合计** | **4,857.2** | 100% |

- Top 3：Stage 2（2,739 ms，56.4%）、Stage 8（727 ms，15.0%）、Stage 12（492 ms，10.1%），合计 81.5%。
- 与 eventId 优化前（p50 合计 6,179.9 ms）相比下降 21.4%；Stage 5 从 2,270 ms 降到 177 ms，
  Stage 12 从 1,007 ms 减半到 492 ms，但 Stage 2/8 的队列等待上升。
- 三段都是队列等待，不是业务计算；单条冻结/清算业务只有 0.025–0.045 ms。
- 结论：瓶颈在“Kafka ack → 委托消费”和“冻结回执 commit → 委托收到回执”的队列等待，
  以及单 symbol 串行链，而不是 CPU 业务逻辑。

## 7. 清算分段与账户锁优化

400k 双 symbol 场景（BTC-USDT 200k + ETH-USDT 200k）的关键结论：

| 阶段 | 发现 |
|---|---|
| settlement listener | 处理耗时占主导（约 66%），poll 不是瓶颈 |
| `handle.settle` | 最大方法，锁等待 3,079 ms 是主要块，不是余额运算 |
| `settlementKeys` | 1,693 ms（8.5 µs/笔），流式去重排序的开销 |
| `publishFlows` | 4,349 ms（listener 的 28.2%），资金流水发送是第二大块 |
| 账户锁优化 | 8 路 slot 后 apply.lockWait 517→140 ms、settle.lockWait 796→139 ms |
| 最终方案 | symbol 冻结保序 + 批内 8 线程 + settlement slot，TPS 26,813.0 |
| 新瓶颈 | 撮合 `match.total` 约 12.5 s、order-freeze-result listener 约 18.9 s |

清算拆分曾使 settlements topic 记录数由约 56 升到 291、序列化耗时升到 2,243 ms，
是 TPS 未明显提升的原因之一。最终方案在保证同 symbol 价格-时间优先的前提下，
把账户锁等待降到可忽略水平，下一步应优化撮合阶段而不是账户链路。

## 8. 总结

1. **批量消息改造收益明确**：四个输出 topic 记录数 50 万 → 617，send 耗时 −95%，
   100k c32 受理 TPS 2,703.7 → 5,145.3，端到端 p50 −45%。
2. **委托批量化的收益集中在 Kafka 维度**：隔离链路 totalTps +37.6%，HTTP 维度基本不变，
   因为入口本身是瓶颈。
3. **HTTP 最新基线**：200k、c32，入口 TPS 5,038.5，端到端 TPS 4,787.0，错误 0。
4. **Kafka 单 symbol 200k** 约 20k TPS；双 symbol 400k 在清算优化后约 26.8k TPS。
5. **剩余瓶颈**：Stage 2/8 队列等待、单 symbol 串行链、清算命令记录数与序列化成本。
6. **不在关键路径但需关注**：funds-flow / marketdata 的 QuestDB 归档堆积；
   marketdata 优化已使堆积积分 −71%、listener −32%~−51%。

## 9. 口径与限制

- 所有数据来自单机 4C/8T 热 JVM 压测，Kafka/MySQL/QuestDB/应用同机，不能直接外推到多机生产环境。
- 诊断轮（`pipeline-trace` / `consumer-timing` / `hot-path`）本身有开销，
  绝对 TPS 不能与干净基线直接比较，只用来看分段比例。
- 本报告只保留最新一轮同维度数据；旧版 Fory 100k、HTTP 100k 3,596.6、Kafka direct 16,630.6
  等均为历史基线，仅用于变化幅度说明。
- 原始压测脚本、原始结果和诊断接口已在演示开发收尾时清理；本文件是保留的唯一压测报告。
