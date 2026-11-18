# 撮合引擎压测报告

> 压测对象：当前工作区构建的单体撮合引擎（下单 → 冻结 → 撮合 → 清算 → 行情 / 委托历史归档）。
> 本报告以 HTTP c16 / c32 端到端压测为主口径；批量入口作为独立入口对比。
> 当前版本：顺序化 save/clear/apply、md_trade 1s 后台刷新、K 线两级聚合（逐笔 → 1m → 高周期）；
> JVM `-Xms2g -Xmx2g -XX:+UseG1GC`。
>
> 统一规格：每轮 **100w 单（1,000,000）**（BTC-USDT 500k + ETH-USDT 500k 交错），预热 10k。

## 1. 测试环境与口径

| 项目 | 配置 |
|---|---|
| 机器 | 单机 4核8G |
| 组件 | Kafka 4.4.0 / MySQL / QuestDB 10.1.0 / 应用同机 |
| JVM | `-Xms2g -Xmx2g -XX:+UseG1GC` |
| 用户 | 200 个用户预充值（USDT 1,000,000 / BTC 1,000 / ETH 10,000 每人） |
| 订单 | 每轮 1,000,000 单，BTC-USDT 500k + ETH-USDT 500k 交错提交 |
| 预热 | 每轮 10,000 单，预热排空后才开始计时 |
| 隔离 | 每轮重启应用、重置消费组 offset 到 latest、重新充值 |

**入口**

- HTTP：逐单 `POST /api/v1/currency/orders`，客户端并发 16 / 32；请求不携带 orderId，
  由币币模块生成。
- 批量接口：`POST /api/v1/currency/orders/kafka-test-batch`，每 2000 单一个请求，
  服务端逐单生成 orderId 后写入同一个下单 Disruptor。

**压测客户端**

- HTTP 压测使用**最小 HTTP/1.1 keep-alive 客户端**：每个并发一条长连接，手工拼接
  请求并按 `Content-Length` / chunked 编码读取响应，避免通用 HTTP 客户端在高并发下的
  额外开销。
- 批量入口使用 `BatchLoadTest`：c8 × 2000/请求，100w 单，预热 10k。

**TPS 口径**

```
受理 TPS   = accepted / 灌入耗时
端到端 TPS = accepted /（全链路排空时刻 − 首批提交时刻）
```

- HTTP accepted = `202` 笔数。
- 全链路排空 = order-command / account-order-freeze / order-freeze-result /
  account-trade-settlement 四个消费组 lag 回到阈值内。
- marketdata / funds-flow / order-history 三个 off-path 组单独等待排空。

## 2. 测试结果

### 2.1 HTTP c16 / c32

每种并发跑两轮（2G 堆，100w 单）：

| 轮次 | 并发 | 受理 | 失败(4xx/5xx/IO) | 灌入耗时 s | 受理 TPS | 清算排空 s | 总时间 s | 端到端 TPS | p50 ms | p99 ms | 残余挂单 | 余额差 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| c16 #1 | 16 | 1,000,000 | 0/0/0 | 90.324 | 11,071.3 | 0.000 | 90.324 | 11,071.3 | 0.80 | 7.33 | 0 | 0 |
| c16 #2 | 16 | 1,000,000 | 0/0/0 | 85.452 | 11,702.5 | 0.000 | 85.452 | 11,702.5 | 0.86 | 6.63 | 0 | 0 |
| c32 #1 | 32 | 1,000,000 | 0/0/0 | 88.109 | 11,349.6 | 0.000 | 88.109 | 11,349.6 | 1.75 | 13.88 | 0 | 0 |
| c32 #2 | 32 | 1,000,000 | 0/0/0 | 85.210 | 11,735.7 | 0.000 | 85.210 | 11,735.7 | 1.79 | 11.94 | 0 | 0 |

均值：

| 并发 | 受理 TPS | 灌入耗时 s | 端到端 TPS | p50 ms | p99 ms |
|---:|---:|---:|---:|---:|---:|
| c16 | **11,386.9** | 87.888 | **11,386.9** | 0.83 | 6.98 |
| c32 | **11,542.7** | 86.660 | **11,542.7** | 1.77 | 12.91 |

表中 TPS 与 p50 / p99 均取两轮算术平均；两轮主链路排空均低于 50ms 采样分辨率，
端到端 TPS 与受理 TPS 相同（偏乐观最多约 1%）。

- off-path 排空：c16 2.064 / 3.297 s，c32 6.723 / 4.553 s（含 order-history → QuestDB 归档）；
- 四轮 `ord_history` 主轮均归档 1,000,000 行（预热 10k 另计），无异步拒单；
- 四轮结束后 `openOrders=0`，BTC / ETH / USDT 汇总余额变化均为 0；
- 四轮应用日志均 0 ERROR。

### 2.2 批量下单入口压测（1M 单）

应用重启、消费组 offset reset 到 latest、重新充值后执行；测试前清空
Kafka `me.*` topic 与 QuestDB 归档表。入口 `POST /api/v1/currency/orders/kafka-test-batch`，
客户端 c8 keep-alive × 2000 单/请求，100w 单，预热 10k；JVM `-Xms2g -Xmx2g -XX:+UseG1GC`。

| 指标 | 数值 |
|---|---:|
| 受理 / 拒绝 / 失败批次 | **1,000,000 / 0 / 0** |
| 业务批次 / HTTP 请求 / 重试事件 | 500 / 591 / 182 |
| 503（ring 满）/ IO 断连 | 91 / 91，全部续传成功 |
| 灌入耗时 | **5.711 s** |
| 受理 TPS | **175,090.8** |
| 主链路端到端耗时 / TPS | **22.521 s / 44,403.3** |
| 全链路（含归档）耗时 / TPS | **28.737 s / 34,798.6** |
| HTTP 批次延迟 p50 / p99 | 21.74 ms / 543.87 ms |
| 平均每请求受理 | 1,692.0 单 |
| 预热 10k | 受理 10,000 / 0 拒；灌入 0.256 s（39,116 TPS） |
| 应用 ERROR | 91 条，均为 ring 满 503；无其他异常 |

> 批量入口灌入约 **17.5 万单/s**；端到端吞吐由下游批量消费决定：主链路约
> **4.44 万单/s**，含归档约 **3.48 万单/s**。

### 2.3 各 topic 堆积与排空

口径：`produced` 为正式轮 latest offset 增量（record 数）；`peakLag` 为采样到的消费组
最大 lag（record）；`consumed` 为消费完成 record 数（同 topic 多消费组时求和）；
`records/s` = produced / 灌入耗时；`drain@submit` 从首笔提交计时，`drain@end` 从最后
一批 HTTP 响应计时。批量 topic 一条 record 内含多条业务数据。

| 排名 | topic | 消费组 | produced | peakLag | consumed | records/s | drain@submit s | drain@end s |
|---:|---|---|---:|---:|---:|---:|---:|---:|
| 1 | `me.funds-batch` | admin-funds-flow | 4,350 | **1,191** | 4,350 | 762 | **28.737** | 23.025 |
| 2 | `me.order.history-batch` | admin-order-history | 602 | 242 | 602 | 105 | **28.091** | 22.379 |
| 3 | `me.match-pair-batch` | marketdata-trade-consumer + admin-user-trade | 358 | 66 | 716 | 125 | 22.922 | 17.211 |
| 4 | `me.account.settlements-batch` | account-trade-settlement-consumer | 889 | 72 | 889 | 156 | 22.521 | 16.810 |
| 5 | `me.order.freeze-results-batch` | order-freeze-result-consumer | 595 | 103 | 595 | 104 | 22.047 | 16.336 |
| 6 | `me.account.order-freeze-commands-batch` | account-order-freeze-consumer | 586 | 87 | 586 | 103 | 21.567 | 15.856 |
| 7 | `me.order.commands-batch` | order-command-consumer | 560 | 469 | 560 | 98 | 20.076 | 14.365 |
| — | `me.account.offset-seq-batch` | 无消费者（审计序列） | 513 | n/a | n/a | 90 | n/a | n/a |
| — | `me.order.offset-seq-batch` | 无消费者（审计序列） | 257 | n/a | n/a | 45 | n/a | n/a |
| — | `me.account.commands` | account-command-consumer | 0 | 0 | 0 | 0 | 0.000 | 0.000 |
| — | `me.order.cancels-batch` | order-cancel-consumer | 0 | 0 | 0 | 0 | 0.000 | 0.000 |

要点：

- 主链路最后排空的是 `me.account.settlements-batch`（22.521 s）；行情 / 资金流水 /
  委托历史三个归档 topic 继续运行到 28.7 s，比主链路多约 6.2 s。
- `me.funds-batch` 数据量最大（约 300 万行流水），produced 4,350 条 record、
  peakLag 1,191 条，是耗时和堆积双第一。
- 下单入口 5.711 s 收下 100 万单（17.5 万 TPS），`me.order.commands-batch` 峰值 lag
  469 条 record，进一步确认瓶颈在下游批量消费与归档，不在 HTTP / 校验入口。

### 2.4 正确性校验

| 校验项 | 结果 |
|---|---|
| 残余挂单 | **0** |
| BTC / ETH / USDT 汇总余额 | total / available / frozen 差值均为 **0** |
| QuestDB `ord_history` 正式轮增量 | **1,000,000** |
| QuestDB `user_trade` 正式轮增量 | **1,000,000** |
| QuestDB `funds_flow` 正式轮增量 | **3,000,000** |
| QuestDB `md_trade` 正式轮增量 | 3,300（设计如此：每 poll 只归档最后 50 条成交） |
| topic end offsets | `me.account.offset-seq-batch` +513、`me.order.offset-seq-batch` +257 |

### 2.5 入口对比

- **HTTP 单笔入口仍是吞吐瓶颈**：批量入口受理约 **17.5 万 TPS**、主链路端到端约
  **4.44 万 TPS**，远高于 HTTP 的 1.1 万级；HTTP 侧应优先优化入口 / JSON / 校验，
  而不是继续加并发；或接口模块作集群部署。

## 3. 限制

- 压测客户端与服务端、Kafka、QuestDB 同机竞争 4核8G，数据不能直接外推到多机生产环境；
- Kafka 单节点 RF=1，`acks=all` 语义与多副本不同；
- c16 两轮在 QuestDB 重启前完成；c32 两轮在 QuestDB 重启后完成（重启前两次 c32 因
  QWP ack 30s 超时 fatal，未计入）。横向 c16/c32 差异含少量环境状态差异；
- 批量入口本轮为单轮结果，同构建轮间存在波动，对比时需保留该不确定性；
- 订单构造为同价同量全成交，匹配成本接近最小值，复杂订单簿形态下吞吐可能不同；
- HTTP 每轮数值存在约 ±5% 的轮间波动。
