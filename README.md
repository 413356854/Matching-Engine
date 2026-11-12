# Matching Engine · 币币撮合演示系统

用 **Java 21 + Spring Boot 4.1.2** 构建的现货（Spot）撮合演示系统，**单体架构**：账户、币币、委托与撮合、结算中心、行情五个业务模块跑在同一进程，以文件夹隔离模拟独立模块（撮合内核作为委托模块的进程内纯 JDK 子包）；核心目标 **单机 10,000+ orders/s（端到端）**，覆盖下单、风控、撮合、清算、记录、行情推送、K 线七条流程。

> 当前状态：**核心链路已实现并通过压测**（下单 → 冻结 → 撮合 → 结算 → 行情 / 流水归档）。冻结回执、结算与行情用 **Kafka**（不自研 WAL）；撮合内核自研，并在委托模块冻结回执流程中**进程内同步调用**（不再经 `me.orders` / `me.trades` 往返）。
> 系统形态：**单体（modular monolith）**——五个业务模块同进程、同 Spring 上下文，靠 `com.me.<module>` 包边界隔离，**不是微服务**。
> 订单簿**先集合版**：`TreeMap` 价格梯 + `LinkedHashMap` FIFO + `HashMap` 订单索引，实现见 `com.me.matching.service.impl.CollectionOrderBook`。
> **数组化（分页价格梯 + 两级位图 + 64B 槽 + 侵入式 FIFO）不作为 M0 方案**，推迟到 M3 优化；两种实现共用 `OrderBook` 接口，届时整层替换、调用方不动。

---

## 技术栈

| 类别 | 选型 | 版本 | 说明 |
|---|---|---|---|
| 语言 / 运行时 | Java（JDK） | **21 (LTS)** | 编译与运行统一 `--release 21`；Spring Boot 4.1.2 要求 JDK **≥ 17**、兼容至 **27** |
| 应用框架 | Spring Boot | **4.1.2** | 当前 GA 稳定版（2026-10-23 发布），依赖 Spring Framework **7.0.10+**；负责单体启动、DI、配置 |
| 构建工具 | Maven | **3.6.3+** | Spring Boot 4.1.2 官方最低要求；本机验证 3.9.11 |
| 接入层 | Spring MVC / Netty | — | REST 下单、交易对/余额/行情查询、管理端充值已实现（`/api/v1`）；Netty TCP/WS 网关（规划） |
| 消息 / 持久化 | Apache Kafka | broker **4.4.0** / client **4.2.2** | 直接使用 `org.apache.kafka:kafka-clients`（不引入 Spring Kafka）；冻结回执、单用户结算与行情事件，详见「持久化组件（当前实现与规划）」；本地单节点已装，见「本地 Kafka 环境（4.4.0）」 |
| 查询投影 | QuestDB（当前）/ ClickHouse / PostgreSQL | — | 异步写入，不进撮合热路径 |
| 前端 | React + Vite + lightweight-charts | — | `web/` 演示前端（已实现）：币币交易（交易对、余额、下单、1m K 线、最新成交）+ 后台管理（流水管理、K 线管理） |

> **基线口径：JDK 21 + Spring Boot 4.1.2 + Maven 3.6.3+**。
> 撮合内核（`com.me.matching.entity` / `service`）仍是零第三方依赖、纯 JDK，不引入 Spring/Kafka；它现在是**委托与撮合模块的进程内子包**，可独立编译、单测与压测。

## 系统形态：单体 + 模块化

**一个 Maven 模块、一个 Spring Boot jar、一个进程**，五个业务模块用文件夹（包）隔离，模拟独立模块的边界。撮合内核不再单独算一个模块，而是委托模块内部的纯 JDK 子包。

- **部署形态：单体**。`java -jar` 一条命令起全链路，断点能进任意模块，压测不受网络抖动干扰。
- **逻辑形态：模块化**。一个模块 = `com.me.<module>` 一个包 = 一个文件夹，模块内部按 Java 习惯分层：`controller / service / service.impl / mapper / entity / dto / vo`。
- **隔离手段**：跨模块只准调对方 `service` 接口或订阅事件；不直接访问对方 `mapper` / `entity`；不共享数据表；依赖方向固定，通过包结构与代码评审守护。
- **拆分路径**：边界写进目录与 import 规则后，将来要拆服务就是“搬文件夹”，业务代码基本不动。

### 五个业务模块（③委托与撮合合并）

| # | 模块 | 包 / 文件夹 | 职责 | 关键类 | 向外依赖 |
|---|---|---|---|---|---|
| ① | **账户模块** | `com.me.account` | 开户、余额（available / frozen）、冻结与解冻、资金流水、对账口径 | `AccountService`、`AccountController`、`AccountIngressRing` | `shared` |
| ② | **币币模块** | `com.me.currency` | 币种、交易对（symbol）、价格/数量精度、最小下单量、费率档位、上下架；**下单接入**（同步校验后写入 Disruptor，聚合为 `OrderCommandBatch` 投递 `me.order.commands-batch`，不直接调用委托模块）；**用户历史流水查询**（经 shared 只读查询端口读取 QuestDB `funds_flow`，最新在前） | `PairService`、`OrderPlacementController`、`OrderCommandPublisher`、`OrderIngressRing`、`CurrencyConfigCache`、`UserFundsFlowController` | `shared` + `com.me.kafka` 基础设施 |
| ③④ | **委托与撮合模块** | `com.me.order` + 内部纯 JDK 子包 `com.me.matching` | 消费 `me.order.commands-batch`（**唯一接单入口**）、参数与幂等校验、订单状态机、委托查询；撤单/改单（规划）；订单簿、价格-时间优先、成交生成；冻结回执后进程内撮合与成交清算 | `OrderCommandListener`、`OrderService`、`OrderMatchingService`、`MatchingEngine`、`OrderBook` | account、currency、`shared` |
| ⑤ | **结算中心** | `com.me.settlement` | （规划）双边记账、手续费、资金划转、清算幂等、日终对账；当前仅保留包结构，结算逻辑在 account / order 内 | 规划 | account、order |
| ⑥ | **行情模块** | `com.me.marketdata` | 成交行情、K 线聚合、成交 / 深度查询；落 QuestDB | `MarketDataListener`、`MarketDataService`、`MarketDataRepository`、`KlineAggregator` | 事件（order → marketdata） |

### 依赖方向（单向，无环）

- `account` → 只依赖 `shared`
- `currency` → `shared` + `com.me.kafka` 基础设施（下单接口只发布委托命令）；**不依赖 `order`**
- `order` → `account`、`currency`；消费 `me.order.commands-batch` 接单；**进程内直接调用** `com.me.matching.service.MatchingEngine`（撮合内核属于本模块）
- `matching` → 只依赖 `shared` 的命令/事件 DTO；**纯 JDK、零 Spring、零 Kafka**，不依赖 account / order / settlement
- `settlement` → `account`、`order`
- `marketdata` → 订阅 `order` 进程内撮合后输出的成交/深度事件（只认 `shared` 事件类型）
- `admin` → `account.service`（管理端充值）、`currency.service`（币种校验），只调 service 接口，不直接访问对方 mapper / entity

### 模块边界规则

| 规则 | 要求 | 落地手段 |
|---|---|---|
| 一模块一分层 | 一个模块 = `com.me.<module>` 一个包，内部固定 `controller / service / service.impl / mapper / entity / dto / vo` | 目录结构 |
| 只准依赖 `service` | 跨模块只能调对方 `service` 接口；`mapper` / `entity` 是模块私有，不直接跨模块访问 | 包结构 + 评审 |
| 分层职责 | `controller` 只做接入与校验；`service` 承载业务；`mapper` 只做持久化；`entity / dto / vo` 不混用 | 评审 |
| 不共享数据表 | 每模块独立表前缀：`acct_` `pair_` `ord_` `stl_` `md_`，跨模块读走对方 service | schema 约定 + 评审 |
| 禁止环依赖 | 依赖方向固定，禁止反向 import | 评审 + 依赖检查 |
| 同步走接口、异步走事件 | 撮合由委托模块在冻结回执流程中进程内调用；行情、结算用事件解耦 | 进程内调用 / Kafka |
| 撮合内核零框架 | `matching/entity`、`matching/service` 中不出现 `org.springframework.*` / `org.apache.kafka.*`；只有只读查询接入层 `matching/controller` 例外 | 编译期 + 单测断言 |

## 方案速览

```
Client
  │ POST /api/v1/currency/orders
  ▼
② 币币模块 currency（校验：交易对/精度/最小下单量）
  │ 命令写入 Disruptor；单聚合线程按「2000 条或 20ms」组装 OrderCommandBatch
  ▼
Kafka me.order.commands-batch（8 分区，key=symbol，每个 symbol 一条批量消息）
  │ ③④ 委托模块唯一接单入口 OrderCommandListener 消费，批内按列表顺序处理
  ▼
③④ 委托与撮合模块 order（保存委托 → 发出冻结命令）
  │ 冻结资金
  ▼
Kafka me.account.order-freeze-commands-batch（8 分区，key=symbol，每个 symbol 一条批量命令）
  │ ① 账户模块 account 消费（按 symbol 分区并行）
  ▼
① 账户模块 account（available → frozen，预占成功才接单）
  │ 冻结回执
  ▼
Kafka me.order.freeze-results-batch（8 分区，key=symbol，每个 symbol 一条批量回执）
  │ ③④ 委托与撮合模块消费冻结回执批量消息
  ▼
③④ 委托模块 order 收到冻结成功回执
  │ 进程内直接调用 MatchingEngine.place()
  ▼
MatchingEngine（订单簿，价格-时间优先，M0 集合版）
  │ 同步返回 MatchResult 成交对
  ▼
③④ 委托模块 order 成交清算：PAID → PARTIALLY_FILLED / FILLED
  │
  ├─ 行情 Kafka me.marketdata-batch（key=symbol，每个 symbol 一条批量行情）
  │     ─▶ ⑥ 行情模块 marketdata（→ 成交明细 / K 线 → QuestDB）
  │
  └─ 结算 Kafka me.account.settlements-batch（key=symbol，每个 symbol 一条批量结算消息，内含买方/卖方单用户命令）
        │
        ▼
     ① 账户模块 account（买方/卖方两条单用户命令分别结清，产出 me.funds-batch）
     （⑤ 结算中心 settlement：手续费 / 对账，规划）
```

**五个业务模块跑在同一个进程里**：Kafka 是同机部署的持久化与消息组件，不是服务边界；模块间同步调用走 `service` 接口，异步解耦走事件。撮合引擎不再是 Kafka 消费者/生产者，而是委托模块进程内的纯 JDK 组件。

**接单与撮合**：`币币模块 REST（仅投递命令）→ me.order.commands-batch → 委托模块唯一接单入口 OrderCommandListener → 账户模块预占 → me.account.order-freeze-commands-batch → 账户回执 me.order.freeze-results-batch → 委托模块进程内调用 MatchingEngine → 委托清算 → me.account.settlements-batch / me.marketdata-batch`。
充值等非冻结资金命令仍走 `me.account.commands`（key=userId），与冻结链路分开；两条链路共用 `AccountService` 的分片锁。
下单命令、冻结命令、冻结回执、行情、结算都按 symbol 分组，每个 symbol 一条批量消息（key=symbol）；资金流水按 userId 分组发送（key=userId），每个目标分区一条批量消息。消息体分别是 `OrderCommandBatch` / `OrderFreezeCommandsBatch` / `OrderFreezeResultBatch` / `MarketDataBatch` / `TradeSettlementBatch` / `FundsFlowBatch`；只有充值等非冻结资金命令 `me.account.commands` 仍是逐条消息。
`me.orders` 与 `me.trades` 两个 topic 已从撮合链路移除；订单/订单簿状态仍是内存态（演示口径），生产化需另做持久化与快照。

## 持久化组件（当前实现与规划）

| 层 | 组件 | 作用 | 关键配置 |
|---|---|---|---|
| **订单 / 撮合状态** | **进程内内存** | 委托状态机、订单簿、去重缓存；当前不持久化 | 进程重启丢失（演示口径） |
| **冻结 / 结算消息** | **Kafka `me.account.commands` / `me.account.order-freeze-commands-batch` / `me.order.freeze-results-batch` / `me.account.settlements-batch` / `me.funds-batch`** | 资金命令、冻结命令/回执、单用户结算命令、资金流水；当前用于传输与审计，未实现回放恢复 | 8 分区；本地 RF=1，RF=3 为生产规划；`acks=all`，幂等 Producer |
| **订单簿快照（温态）** | **本地 NVMe + 可选 S3** | 订单簿 + 活单 + 去重表 + offset，加速恢复 | 规划；每 1000 万条/15s，CRC 校验，保留 3 份 |
| 行情输出 | Kafka `me.marketdata-batch`（order 生产） | 成交 tick（K 线由行情模块聚合后写 QuestDB） | 本地 RF=1；RF=3 为生产规划 |
| 查询投影 | QuestDB（当前）/ ClickHouse / PostgreSQL（规划） | 审计与查询 | 异步，不进热路径 |

**一句话**：Kafka 负责冻结、结算、资金与行情的持久化/解耦；**订单与撮合状态是进程内内存态**（演示口径）。生产化恢复需要另做订单持久化 + 订单簿快照，不能依赖已移除的 `me.orders`。

**七个关键决策**

1. 撮合在委托模块冻结回执流程中进程内执行；冻结回执 runner 单线程，按消费顺序撮合，保证按 symbol 过滤后的价格-时间优先；`me.order.commands-batch` 与 `me.account.order-freeze-commands-batch` 都按 symbol 分区并可多 consumer 并行，同一 symbol 仍由单分区/单 consumer 串行处理；充值等非冻结资金命令留在 `me.account.commands`（key=userId）
2. 撮合在冻结回执线程内同步完成；行情、结算、K 线通过 Kafka 异步解耦
3. 订单簿**先集合版、后数组化**：M0 用 `TreeMap` 价格梯 + `LinkedHashMap` FIFO + `HashMap` 索引，先把正确性与确定性跑通；数组化（分页价格梯 + 两级位图 + 64B 订单槽 + 侵入式 FIFO，全 O(1)、零分配）列为 M3，换实现不动 `OrderBook` 接口
4. 批量：下单入口 Disruptor 按「2000 条或 20ms」聚合为 `OrderCommandBatch`；批量 topic 消费 `max.poll.records=10`（逐条的 `me.account.commands` 仍为 2000）+ `fetch.max.wait.ms=50ms` + `fetch.min.bytes≈1MB`，每个 poll 批次一个 Kafka 事务；整批回滚后在**新事务**中重投，账户拒绝按记录身份缓存、资金流水按 eventId 去重、QuestDB 按 poll 批次批量写入
5. 确定性：冻结回执按 symbol 分区并行消费（同一 symbol 内仍单线程）、进程内按 symbol 串行撮合、整数运算；演示版**不缓存撮合输出**，同一订单的重复回执由匹配引擎按 `orderId` 去重，不再产生行情/结算；冻结按 symbol 分区后，同一用户的充值（key=userId）与冻结（key=symbol）可能由不同线程并发处理，余额正确性由账户分片锁保证，但两者先后不再全局确定；快照/内存恢复列为后续计划
6. 资金预占前置，撮合器只接收已冻结订单
7. 背压快速失败（Kafka buffer / consumer lag 水位），绝不无限排队

## 账户模块

账户是整条链路里唯一“每笔订单都要写一次”的模块。当前演示版为**内存账本**：
`ConcurrentHashMap<(userId, currency), available + frozen + seq>`，按 userId 分 8 个写入 slot；
余额不落库、不落盘，进程重启清零。资金流水通过 `me.funds-batch` 归档到 QuestDB，
用于查询与审计。实现细节见下文「已实现：账户模块 M0（内存版）」。

## 仓库结构（单体）

```
Matching Engine/                     # 工作区根
├── matching-engine/                 # ★ 单体应用：一个 Maven 模块 → 一个 Spring Boot jar
│   ├── pom.xml                      # spring-boot-starter-parent 4.1.2 · Java 21
│   ├── src/main/java/com/me/
│   │   ├── MeApplication.java       # 唯一启动类（@SpringBootApplication）
│   │   ├── shared/                  # 公共类型：ID、金额、事件、错误码（无业务逻辑）
│   │   ├── account/                 # ① 账户模块（内存账本 / 冻结回执 / 资金流水）
│   │   ├── currency/                # ② 币币模块（币种 / 交易对 / REST 下单入口）
│   │   ├── order/                   # ③④ 委托与撮合模块（消费委托命令 / 状态机 / 进程内撮合编排）
│   │   ├── matching/                #     委托模块内部的纯 JDK 撮合内核（订单簿，零 Spring、无 Kafka）
│   │   ├── settlement/              # ⑤ 结算中心（当前仅包结构，结算逻辑在 account / order 内）
│   │   ├── marketdata/              # ⑥ 行情模块（成交 / K 线 / QuestDB 查询）
│   │   ├── admin/                   # 管理端（充值 / 流水管理 / K 线管理）
│   │   ├── kafka/                   # 裸 kafka-clients 基础设施（Fory 序列化 / 批量消费 / 事务）
│   │   └── config/                  # Spring 配置（数据源 / 撮合引擎 Bean）
│   └── src/main/resources/          # application.yml
├── doc/
│   └── mysql/                       # 币种 / 交易对建表与种子数据
├── loadtest/                        # 压测报告
│   └── TEST-REPORT.md               # 合并后的撮合引擎压测报告
├── web/                             # React + Vite 演示前端（币币交易 + 后台管理）
└── README.md
```

> 五个业务模块之间**没有 Maven 坐标依赖**，只有包依赖：`com.me.matching` 是委托模块内部的纯 JDK 子包（逻辑合并），不再单独算一个可拆分的业务模块。
>
> 每个模块固定七层；`matching` 只保留只读 `controller`（订单簿查询）与空的 `mapper`，**没有 `mq` 适配层**——撮合内核不消费、不生产 Kafka。

## 环境

- JDK 21（已验证 21.0.12；基线 21，下限 17）
- Maven 3.6.3+（已验证 3.9.11）
- Kafka **4.4.0**（KRaft，本地单节点安装目录记为 `<KAFKA_HOME>`；4.x 起无 ZooKeeper、要求 JDK 17+）
- MySQL **8.0.17**（本地 `localhost:3306`，库 `db_exchange`，`utf8mb4`）
- 开发与当前压测在 Windows 4C/8T 本机；Linux 物理机验收为后续计划

## 本地 MySQL 与 MyBatis-Plus

数据库接入配置在 `matching-engine/src/main/resources/application.yml`：

| 项 | 值 |
|---|---|
| URL | `jdbc:mysql://localhost:3306/db_exchange?useUnicode=true&characterEncoding=utf8&connectionTimeZone=Asia/Shanghai&allowPublicKeyRetrieval=true&useSSL=false` |
| 用户名 / 密码 | `admin` / `admin`，可用 `DB_USERNAME` / `DB_PASSWORD` 环境变量覆盖；URL 用 `DB_URL` 覆盖 |
| 连接池 | HikariCP：`minimum-idle=2`、`maximum-pool-size=10`、`connection-timeout=3000` |
| 依赖 | `mybatis-plus-spring-boot4-starter:3.5.18.1`（Spring Boot 4 专用；Boot 3 用 `mybatis-plus-spring-boot3-starter`） |
| 驱动 | `com.mysql:mysql-connector-j`（版本由 Spring Boot 4.1.2 BOM 管理，9.7.0） |

- 启动类 `com.me.MeApplication` 使用 `@MapperScan("com.me.**.mapper")`，Mapper 统一放各模块 `mapper` 包。
- 本地库 `db_exchange` 使用 `utf8mb4 / utf8mb4_0900_ai_ci`；建表与种子数据脚本见 `doc/mysql/`。

## 本地 Kafka 环境（4.4.0）

**版本**：Apache Kafka **4.4.0** —— 官网 downloads 页 `Supported releases` 首位，**2026-10-22 发布**，当前最新稳定版（同列 4.3.2、4.2.2）。来源：
<https://kafka.apache.org/community/downloads/> · 二进制包 `kafka_2.13-4.4.0.tgz`（129 MB，SHA512 已与官网核对一致）。

> 4.x 起只用 **KRaft**（ZooKeeper 已在 4.0 移除），Broker 要求 **JDK 17+**；本机 JDK 21.0.12 满足。

**安装位置**：`<KAFKA_HOME>`（放在仓库外：官方发行版约 134 MB，跑起来后 `data` 目录约 1.5 GB——每个分区的 offset index 预分配 10 MB——不适合进 Git）

> 下文 `<KAFKA_HOME>` / `<QUESTDB_HOME>` 表示本地安装目录，按实际路径替换。

```
<KAFKA_HOME>\
├── kafka_2.13-4.4.0\                  # 官方发行版（原样解压）
│   └── config\kraft-local.properties  # 本地单节点配置：log.dirs、默认 8 分区
├── data\kraft-combined-logs\          # KRaft 元数据 + 各分区日志
├── logs\                              # broker / 工具日志（server.log、broker.err.log）
├── downloads\                         # 安装包
├── bin-local\kafka-tool.cmd           # 命令行工具入口（替代官方 bat）
├── start-kafka.ps1 / stop-kafka.ps1   # 启停
└── cluster.id                         # 格式化时生成的 cluster id
```

**启停**：

```powershell
cd <KAFKA_HOME>
pwsh -File .\start-kafka.ps1     # 就绪即监听 localhost:9092，PID 写入 kafka.pid
pwsh -File .\stop-kafka.ps1
```

**常用命令**：

```powershell
# 列表 / 建 topic
.\bin-local\kafka-tool.cmd org.apache.kafka.tools.TopicCommand --list --bootstrap-server localhost:9092
.\bin-local\kafka-tool.cmd org.apache.kafka.tools.TopicCommand --create --topic me.order.freeze-results-batch --partitions 8 --replication-factor 1 --bootstrap-server localhost:9092
# 生产 / 消费（value 是 Fory 二进制，ConsoleConsumer 只用于确认消息到达，不可读内容）
.\bin-local\kafka-tool.cmd org.apache.kafka.tools.ConsoleProducer --bootstrap-server localhost:9092 --topic me.marketdata-batch
.\bin-local\kafka-tool.cmd org.apache.kafka.tools.consumer.ConsoleConsumer --bootstrap-server localhost:9092 --topic me.marketdata-batch --from-beginning
# KRaft 元数据 quorum 状态（4.x 没有 ZooKeeper，元数据由 controller quorum 管）
.\bin-local\kafka-tool.cmd org.apache.kafka.tools.MetadataQuorumCommand --bootstrap-server localhost:9092 describe --status
```

**已建 topic**（本地 RF=1；生产规划为 RF=3、minISR=2）：

| topic | 分区 | 用途 |
|---|---|---|
| `me.order.commands-batch` | 8 | 委托命令批量消息（`key=symbol`，每个 symbol 一条，body=`OrderCommandBatch`；币币 REST 经 Disruptor 按「2000 条或 20ms」聚合，外部生产者需按同样格式写入；委托模块唯一接单入口，批内按列表顺序处理） |
| `me.account.commands` | 8 | 充值 / 扣账等**非冻结**资金命令（`key=userId`，`logType` 指定业务日志类型；逐条消息） |
| `me.account.order-freeze-commands-batch` | 8 | 下单冻结命令批量消息（`logType=ORDER_FREEZE`，`key=symbol`；每个 symbol 一条，body=`OrderFreezeCommandsBatch`；账户冻结按 symbol 分区并行，同一 symbol 内保序） |
| `me.order.freeze-results-batch` | 8 | 下单冻结回执批量消息（`key=symbol`，每个 symbol 一条，body=`OrderFreezeResultBatch`；触发进程内撮合） |
| `me.marketdata-batch` | 8 | 行情批量消息（`key=symbol`，每个 symbol 一条，body=`MarketDataBatch`；行情模块消费并落 QuestDB） |
| `me.account.settlements-batch` | 8 | 单用户成交结算批量消息（`key=symbol`，每个 symbol 一条，body=`TradeSettlementBatch`；每笔成交拆成买方 `-b`、卖方 `-s` 两条命令，每个元素只含一个用户的资金腿） |
| `me.funds-batch` | 8 | 资金流水批量消息（按 userId 哈希分区，每个分区每批一条，body=`FundsFlowBatch`；8 个 admin consumer 并行归档 QuestDB） |
| `me.depth` | 8 | 深度 / BBO 事件（规划；当前代码未使用） |

> 冻结回执、行情、结算按 symbol 分组（`key=symbol`），同一 symbol 的批次落在同一分区并保持顺序；资金流水按 userId 哈希分区，每个分区每批聚合一条批量消息。单条批量消息最多封装 2000 个元素，超过时拆成多条。旧的 `me.account.order-freeze-commands` / `me.order.freeze-results` / `me.marketdata` / `me.account.settlements` / `me.funds` topic 已由上述 `-batch` topic 取代，broker 中可能保留历史消息。

> `me.orders` / `me.trades` 已从撮合链路退役（撮合改为委托模块进程内调用），broker 中可能仍保留历史消息；不再有消费者，可按需在停 broker 后清理。

**验证记录**：`kafka-storage format --standalone` 生成 cluster id `ZodMednSQJeE9XkyrTsBaA`，metadata.version = **4.4-IV2**；日志出现 `Kafka version: 4.4.0` / `Kafka Server started`；3 条消息生产→消费回读一致；`me.order.freeze-results` / `me.marketdata` 等 topic 8 个分区 leader/ISR 均正常；重启 broker 后 topic 与数据保留。KRaft 生效的直接证据：`MetadataQuorumCommand --bootstrap-server localhost:9092 describe --status` 返回 `LeaderId: 1`、`CurrentVoters: [{"id": 1, ..., "endpoints": ["CONTROLLER://localhost:9093"]}]`、`MaxFollowerLag: 0`。

**Windows 上踩到的坑（本地开发注意）**：

1. **官方 `bin\windows\*.bat` 在 4.4.0 上直接不可用**，报 `The input line is too long. The syntax of the command is incorrect.`——110 个 jar 拼出的 classpath 加引号后超过 cmd.exe 的 8191 字符上限。绕法：用 JVM 的 `-cp "libs\*"` 通配 classpath，已封装为 `bin-local\kafka-tool.cmd`，官方脚本保持原样不动。
2. **不要在 Windows 上删 topic**：删除要把 `topic-partition` 目录重命名成 `*-delete`，Windows 不允许重命名仍有打开句柄的目录，Kafka 会把整个 log dir 标记 failed 并**直接关闭 broker**（`Shutdown broker because all log dirs ... have failed`）。需要重建就停 broker 后手工删 `data\kraft-combined-logs\<topic>-*` 再启。
3. 单节点 RF 只能 1，`acks=all` 的语义与 3 副本不同；吞吐/延迟的生产验收计划按 Linux 物理机口径（当前为本机 4C/8T 压测）。

## 已实现：下单入口 Disruptor 批量聚合（2000 条或 20ms）

`POST /api/v1/currency/orders` 不再直接调用 producer，而是把命令非阻塞写入
`OrderIngressRing`（LMAX Disruptor，多生产者 / 单 handler）：

1. HTTP 线程：`tryPublishEvent` 入队，ring 满或未启动时返回 503，不阻塞、不无限排队；
2. `order-ingress-handler` 单线程：把命令攒进待发送列表，达到
   `app.currency.ingress.batch-size`（默认 **2000**）立即 flush；定时 tick 每
   `app.currency.ingress.flush-interval-ms`（默认 **20ms**）触发一次 flush；
3. flush 时按 symbol 分组，每个 symbol 一条 `OrderCommandBatch`（`key=symbol`），
   由单实例幂等 producer 异步发送到 `me.order.commands-batch`，不等待 broker ack。

同一 symbol 的命令在批次内保序，批次之间也按聚合线程的发送顺序进入同一分区；
不同 symbol 仍可按分区并行消费，价格-时间优先不变。ring 槽位默认 65536，必须是 2 的幂。

> 停机时先发布一个屏障事件，等 handler 处理完屏障之前的命令并 flush，再 `shutdown`；
> 屏障等待上限 5s，超时则 halt 并跳过最终 flush。

## 已实现：Kafka 批量消费（批量 topic 10 条 / 50ms / 一批一事务）

七条消费链路直接使用 `org.apache.kafka:kafka-clients`（4.2.2），不依赖 Spring Kafka。
`KafkaConsumerConfiguration` 默认为每条链路创建一个 `KafkaConsumerRunner`，单线程 poll；
`order-command`、`account-order-freeze`、`order-freeze-result`、`funds-flow` 链路分别通过
`app.kafka.consumer.concurrency.order-command=8`、`account-order-freeze=8`、`order-freeze-result=2`、
`funds-flow=8` 起多个 runner，同组内按分区分配、并行消费。前三条按 symbol 分区，同一 symbol 内顺序不变；
资金流水按 userId 哈希分区，8 个 consumer 并行写 QuestDB。撮合产出的行情与结算也按 symbol 分组发送。
处理方法与原来一致，接收 `List<ConsumerRecord<...>>`，按顺序处理一个 poll 批次；
下单 topic 的每条 record 是 `OrderCommandBatch`，批内再按 `msgList` 顺序逐条保存委托：

| 配置 | 值 | 说明 |
|---|---|---|
| `app.kafka.consumer.max-poll-records` | `2000` | 逐条 topic（仅 `me.account.commands`）单次 poll 最多 2000 条 |
| `app.kafka.consumer.batch-max-poll-records` | `10` | 六个 `-batch` topic（下单入口 + 五个下游）单次 poll 最多 10 条批量消息（每条含 `msgList`） |
| `app.kafka.consumer.fetch-max-wait-ms` | `50` | broker 等待 `fetch.min.bytes` 的最长时间 |
| `app.kafka.consumer.fetch-min-size-bytes` | `1048576` | 约 2000 条消息的字节量，凑够或等 50ms 即返回 |
| `app.kafka.consumer.max-poll-interval-ms` | `600000` | 批量处理阻塞 poll 的余量 |
| `app.kafka.producer.transaction-timeout-ms` | `180000` | 一批最多 10 条批量消息（每条 ≤2000 元素），给足事务时间 |

> 一个 poll 批次 = 一次 Kafka 事务：`initTransactions`（启动一次）→ `beginTransaction`
> → 处理（收集整批流水/回执/行情/结算，一次 `sendBatch` 入队，不逐条 `.get()`）
> → `sendOffsetsToTransaction(offsets, consumer.groupMetadata())` → `commitTransaction`。
> 发送失败由 `commitTransaction` 统一暴露。poll 只返回“当前可用的记录”，不会跨 poll 精确凑满上限
> （逐条 topic 2000 条，批量 topic 10 条）。

**错误处理与幂等**：

- `KafkaConsumerRunner`：可重试异常（业务处理失败、`TransactionAbortableException`、
  `CommitFailedException`）→ `abortTransaction`，精确 seek 回本批第一条 offset，1s 退避后整批重投；
  `ApplicationRecoverableException` / 其他 `KafkaException` → 关闭并重建 client；
  `InvalidConfigurationException` / 反序列化失败 → 记录 ERROR 并停止该 runner（fail-stop）。
- `AccountCommandListener`（`me.account.commands`）：按 `(partition, offset)` 缓存拒绝结果；`AccountFreezeCommandListener`（冻结批量链路）：按批量消息内位置 `(partition, offset, index)` 缓存。批量监听器在拒绝后仍继续处理后续记录，后者可能改变余额；整批重投时复现原拒绝。
- `AccountSettlementListener`：同样按记录身份缓存拒绝，避免其他冻结命令在重投期间改变余额、把原拒绝“变成功”。
- `FundsFlowConsumer`：按 `eventId` 去重（`funds_flow` 无 QuestDB DEDUP），整批 `insertBatch` 成功后才记录本批 eventId，整批重投不产生重复行；极端情况下 batch 已写入前缀行后才失败，重投可能重复该前缀。
- `OrderFreezeResultListener`：演示版不缓存撮合输出；重复回执由撮合引擎按 `orderId` 返回 duplicate（`DUPLICATE_IGNORED`），不会重复产生行情/结算；若上一次事务未提交，重投可能丢输出（后续由备份消息/内存恢复解决）。
- `MarketDataListener`：`md_trade` DEDUP 与 K 线 offset 水位保证批量重投幂等。
- `OrderCommandListener`：每个批次只查一次币种表，并按 symbol 在批内缓存交易对，消除逐条 2 次 MySQL 查询。

## 已实现：账户模块 M0（内存版）+ Admin 流水归档

当前已落地一条最小可运行的账户资金链路（不依赖 MySQL 持久化）：

- **账户模块（充值 / 扣账链路）**：`com.me.account` 消费 `me.account.commands`（key=userId），在内存
  `ConcurrentHashMap<(userId, currency), available + frozen + seq>` 中执行资金操作。
  命令携带日志类型 `logType`（如 `DEPOSIT` / `WITHDRAW` / `ORDER_FREEZE` / `ORDER_UNFREEZE` /
  `TRADE_SETTLE_IN` / `TRADE_SETTLE_OUT` / `FEE` / `FROZEN_ADJUST_IN` / `FROZEN_ADJUST_OUT`），账户服务只按日志类型
  映射出的六种原子操作执行：**可用入账、可用扣账、冻结入账、冻结扣账、可用转冻结、冻结转可用**，
  不按具体日志类型分支。可用 / 冻结余额不足分别返回
  `INSUFFICIENT_AVAILABLE_BALANCE` / `INSUFFICIENT_FROZEN_BALANCE`，且不产生流水。
- **资金流水**：成功操作生成 `FundsFlowEvent`，封装进 `FundsFlowBatch` 发送到
  `me.funds-batch`（按 userId 哈希分区），事件携带日志类型、可用/冻结余额前后值。
- **下单冻结回执**：`ORDER_FREEZE` 命令处理完（无论成功还是余额不足）都会向委托模块的
  `me.order.freeze-results-batch` 回执 `OrderFreezeResultEvent`（封装进 `OrderFreezeResultBatch`）；
  回执中的 `orderId` 来自命令的 `refId`，与消费 offset、`me.funds-batch` 流水在同一个
  Kafka 事务内提交。
- **Kafka 事务（消费 offset + 产出流水原子提交）**：每条消费链路一个事务型 `KafkaProducer`
  （`transactional.id = 前缀 + runner + instance-id`）；`initTransactions()` 后每个 poll 批次
  `beginTransaction → 发送流水/回执 → sendOffsetsToTransaction → commitTransaction`，
  `me.account.commands` 的消费 offset 与发往 `me.funds-batch` 的流水在同一个事务中提交或回滚，
  消费端使用 `isolation.level=read_committed`、关闭自动提交。演示版账户服务不做 `requestId`
  防重，Kafka 重投会再次执行；后续由持久化备份与内存状态恢复解决。
  事务回滚后的命令重投会再次执行，可能重复修改余额（演示版取舍）；runner 对可重试失败整批重投（1s 间隔），
  不会跳过记录导致「offset 已提交、流水丢失」。
- **消费起点**：`KafkaRebalanceListener` 在分区分配时查询本消费组已提交的 offset 并显式 `seek`
  到该位置；没有提交记录则从最早位置开始。
- **admin 模块**：`com.me.admin` 消费 `me.funds-batch`，默认通过 QuestDB 官方 Java 客户端的
  **QWP**（WebSocket 9000，`app.questdb.write-mode=client`）批量写入 `funds_flow`；
  PostgreSQL wire（8812）只用于查询、DDL 与 `write-mode=jdbc` 回退。
  `user_id` / `currency` / `log_type` 建 SYMBOL 索引，按天分区，WAL；
  同时保存 `op`（六种原子操作）和日志描述。QWP 每批等待服务端 ack 后才返回；
  `me.marketdata-batch` 的 `md_trade` / `md_kline` / 锚点写入同样走 QWP。
- **QuestDB 写入配置**：`app.questdb.ingest-conf-string` 为官方客户端连接串
  （默认 `ws::addr=localhost:9000;...`），`app.questdb.write-mode=client|jdbc` 控制
  QWP / JDBC batch；已建 WAL 表在磁盘满时会 `suspended`，需用
  `ALTER TABLE <t> RESUME WAL` 恢复，否则行只进 WAL、查询不可见。
- **QuestDB 本地安装**：`<QUESTDB_HOME>`（10.1.0），Web Console
  <http://localhost:9000>，数据目录 `<QUESTDB_HOME>\data`。
  启动：`pwsh -File <QUESTDB_HOME>\start-questdb.ps1 -Background`；
  停止：`pwsh -File <QUESTDB_HOME>\stop-questdb.ps1`。
- **持久化边界**：账户余额不持久化，进程重启清零；QuestDB 中的流水用于查询/审计。

### 本地验证

```powershell
# 1) 向 me.account.commands 发送命令（key=1001）
#    logType: DEPOSIT / WITHDRAW / ORDER_FREEZE / ORDER_UNFREEZE / TRADE_SETTLE_OUT / FROZEN_ADJUST_IN ...
# 2) 查询 QuestDB
curl -G http://localhost:9000/exec `
  --data-urlencode "query=SELECT * FROM funds_flow WHERE user_id = '1001' ORDER BY occurred_at DESC"
```

预期（示例）：依次发送 `DEPOSIT` / `ORDER_FREEZE` / `ORDER_UNFREEZE` / `WITHDRAW` / `TRADE_SETTLE_OUT` /
`FROZEN_ADJUST_IN` 六条命令后，QuestDB 中应有 6 条流水，覆盖六种原子操作；余额不足的命令
（如超额 `WITHDRAW`）被拒绝且不产生流水。

### 管理端接口（流水 / K 线）

- **流水列表**：`GET /api/v1/admin/funds-flows?userId=1001&currency=USDT&logType=DEPOSIT&limit=100`
  - 四个参数都可选；`currency` / `logType` 不区分大小写，结果按 `occurred_at` 倒序。
  - `limit` 缺省 100，服务端上限 1000；响应体 `flows` 是流水列表，`userId` / `currency` /
    `logType` 回显实际生效的筛选条件。筛选参数非法时返回 400 和对应错误码
    （`USER_ID_INVALID` / `CURRENCY_INVALID` / `LOG_TYPE_INVALID` / `LIMIT_INVALID`）。
- **一键删除流水**：`DELETE /api/v1/admin/funds-flows`
  - QuestDB 不支持行级 `DELETE FROM`，实现为 `TRUNCATE TABLE funds_flow`，返回清空前的
    记录数 `deletedCount`；表结构与索引保留，之后新产生的流水继续归档。
  - 只清空 QuestDB 中的查询投影，不影响 `me.funds-batch` topic 与账户内存余额。
- **K 线列表**：`GET /api/v1/admin/klines?symbol=BTC-USDT&interval=1m&limit=100`
  - `symbol` / `interval` 可选，结果按 `bucket_start` 倒序；`interval` 支持
    `1m / 5m / 15m / 30m / 1h / 4h / 1d`，`limit` 缺省 100、上限 1000。
- **一键删除 K 线**：`DELETE /api/v1/admin/klines`
  - 实现为 `TRUNCATE TABLE md_kline` + `TRUNCATE TABLE md_kline_anchor`，并清空内存聚合状态；
    返回删除前的 K 线条数；成交明细 `md_trade` 不受影响。

```powershell
curl "http://localhost:8080/api/v1/admin/funds-flows?userId=1001&limit=50"
curl -X DELETE "http://localhost:8080/api/v1/admin/funds-flows"
curl "http://localhost:8080/api/v1/admin/klines?symbol=BTC-USDT&interval=1m&limit=50"
curl -X DELETE "http://localhost:8080/api/v1/admin/klines"
```

### 交易端用户历史流水

- `GET /api/v1/currency/funds-flows?userId=1001&currency=USDT&limit=50`
  - `userId` 必填；`currency` 可选（不区分大小写）；`limit` 缺省 50、服务端上限 1000。
  - 返回该用户的历史流水，按 `occurred_at` 倒序（最新记录在前），字段与后台流水列表一致。
  - 币币模块通过 shared 的 `FundsFlowHistoryQueryPort` 读取；QuestDB 访问层与
    `funds_flow` 表仍归 admin 模块，币币模块不直接访问表。
  - 参数非法返回 400（`USER_ID_REQUIRED` / `USER_ID_INVALID` / `CURRENCY_INVALID` /
    `LIMIT_INVALID`）；QuestDB 不可用时返回 503 `QUERY_FAILED`。

```powershell
curl "http://localhost:8080/api/v1/currency/funds-flows?userId=1001&limit=50"
```

前端 `web/` 顶栏提供「币币交易 / 后台管理」两个并排栏目；进入后台管理后左侧子栏目为
「流水管理 / K 线管理」，分别对接上面两组接口。

> `admin` 是新增的支撑模块（独立包），不在五个业务模块链路中；后续可把查询投影
> 从 QuestDB 扩展到 ClickHouse / 运营接口。

## 已实现：委托与撮合 M1（保存 → 冻结 → 已支付 → 进程内撮合）

委托模块完成「接单 → 资金冻结 → 进程内撮合 → 成交清算」的闭环（余额、委托与订单簿仍是内存态，进程重启后丢失）：

1. **接单入口**：币币模块 `POST /api/v1/currency/orders` 只做同步校验，把命令写入
   `OrderIngressRing`，由聚合线程按「2000 条或 20ms」组装成 `OrderCommandBatch`
   投递 `me.order.commands-batch`（`key=symbol`），不直接调用委托模块；委托模块
   `OrderCommandListener` 消费该 topic，是**唯一的下单方式与入口**。消费后按批内列表顺序
   逐条生成委托并保存在内存 `ConcurrentHashMap<orderId, Order>`，初始状态 `ACCEPTED`（`seq=1`）。
2. **发出冻结命令**：委托模块按 symbol 分组，向 `me.account.order-freeze-commands-batch`
   （`key=symbol`）发送 `OrderFreezeCommandsBatch`（每个 symbol 一条，元素为 `FundsCommand`）：
   - `logType=ORDER_FREEZE`，`requestId=order-freeze:<orderId>`，`refId=orderId`；
   - 限价买单按 `price × quantity × 10^quoteScale / 10^(priceScale + quantityScale)` 换算成
     计价币最小单位后冻结；卖单冻结基础币 `quantity`；
   - 市价买入冻结前端提交的 `amount`（卖一价，没有卖一则最新价 × 1.05 × 数量）。
     撮合时按实际卖价把预算换算成可买数量，未用完的金额在撮合结束后退回。
3. **账户回执**：账户模块处理完冻结命令后，把成功 / 失败原因作为 `OrderFreezeResultEvent`
   按 symbol 分组回执到 `me.order.freeze-results-batch`（`key=symbol`），与消费 offset、
   `me.funds-batch` 流水同事务提交。
4. **更新为已支付**：委托模块消费回执，成功时 `ACCEPTED → PAID`（`seq+1`）；
   余额不足时 `ACCEPTED → REJECTED`，不撮合。
5. **进程内撮合**：已支付委托以 `OrderPlacedEvent` 由 `OrderMatchingService` **直接调用**
   `MatchingEngine.place()`，同步拿到 `MatchResult` 成交对；不再写 `me.orders`，也不再由 matching
   模块消费 Kafka。撮合返回后委托模块立即做成交清算，并在同一个冻结回执监听事务里发布
   `me.marketdata-batch` 与 `me.account.settlements-batch`。
   - 冻结回执重复是正常路径：账户模块对重复 `requestId` 会再次发布回执，并可能再次执行冻结
     （演示版不做 requestId 防重）。委托已 `PAID` / `PARTIALLY_FILLED` / `FILLED` 时仍返回
     `placedEvent`，由撮合引擎按 `orderId` 返回 duplicate，**不再产生行情/结算**
     （演示版不做记录身份重放缓存）。
   - 回滚重投若上一次撮合输出未提交，可能丢失该输出；后续用备份消息 + 内存状态恢复解决。

- **命令字段**：`requestId`（命令标识；演示版不做 requestId 级防重）、`orderId`、`userId`、`symbol`（`BASE-QUOTE`）、
  `side`（`BUY` / `SELL`）、`type`（`LIMIT` / `MARKET`）、`price`、`quantity`、`amount`，以及
  `priceScale` / `quantityScale` / `quoteScale`。价格按 `10^-priceScale`、数量按
  `10^-quantityScale` 存储；金额按计价币 `10^-quoteScale` 换算（见 `com.me.shared.money.Amounts`）。
- **参数校验**：`requestId` / `orderId` / `userId` / `symbol` / `side` / `type` 必填且合法；
  `symbol` 必须是 `BASE-QUOTE` 形式；限价单 `price > 0`，市价单 `price = 0`，`quantity > 0`；
  市价买入必须携带 `amount > 0`（计价币冻结预算），其他委托类型不允许携带 `amount`。
- **重投语义（演示版）**：不做 requestId / 成交对 / 撮合输出的内存防重缓存。同一
  `orderId + requestId` 重投返回已有委托的当前状态；同一 `orderId` 换 `requestId` 返回
  `ORDER_ID_ALREADY_EXISTS`；成交重投由订单状态拒绝。冻结命令 `requestId` 固定，但账户模块
  不按 `requestId` 去重，重投会再次执行。后续补备份消息与内存状态恢复，再替换为持久化幂等/重放。
- **查询**：`OrderService.view(orderId)`，委托不存在返回 `null`；`OrderView` 携带状态、
  委托内状态序号 `seq`，以及冻结的币种和金额。`OrderService.openOrders(userId, symbol, limit)`
  返回用户当前未完成委托。
- **未实现**：撤单 / 改单、手续费缓冲、委托记录持久化、日终对账。

### 查询用户当前委托

`GET /api/v1/orders?userId=1001&symbol=BTC-USDT&limit=100`

- `userId` 必填；`symbol` 可选，不传时返回用户全部交易对的当前委托；
- 只返回未完成委托：`ACCEPTED`（等待冻结）/ `PAID`（撮合中）/ `PARTIALLY_FILLED`（部分成交），
  已成交 / 已拒绝不返回；
- 按创建时间倒序（最新在前），`limit` 缺省 100、上限 200；
- 响应中每笔委托包含 `orderId`、`userId`、`symbol`、`side`、`type`、`price`、`quantity`、
  `filledQuantity`、`frozenCurrency`、`frozenAmount`、`status`、`seq`、`createdAt`。

```bash
curl "http://localhost:8080/api/v1/orders?userId=1001&limit=20"
```

错误响应（400）：`USER_ID_REQUIRED` / `USER_ID_INVALID` / `SYMBOL_INVALID` / `LIMIT_INVALID`。

### 本地验证

先给用户充值，再下单。两种下单方式最终都写入同一个 `me.order.commands-batch` topic：

```powershell
# 1) 充值 100000 USDT（topic me.account.commands，key=1001）
#    {"requestId":"dep-1","userId":1001,"currency":"USDT","logType":"DEPOSIT","amount":100000,"refId":0}
# 2a) 推荐：走币币模块 REST 下单（写入 Disruptor 聚合，返回 202）
#     curl -X POST http://localhost:8080/api/v1/currency/orders -H "Content-Type: application/json" -d '{...}'
# 2b) 或直接向 topic 生产批量命令（适合压测 / 外部接入）
#     value 必须是 Fory 序列化的 OrderCommandBatch，key=symbol；
#     历史压测方法与结果见 loadtest/TEST-REPORT.md（压测工具已清理）
```

> 旧 topic `me.order.commands`（单条 `OrderCommand`）已由 `me.order.commands-batch`
> （批量 `OrderCommandBatch`）取代。升级时需删除 / 排空旧 topic；若消费组仍从旧 topic
> 的 offset 启动，会因消息格式不匹配而反序列化失败。

下单后的可观测信号：`me.marketdata-batch` 可消费到成交行情，`me.account.settlements-batch`
可消费到结算命令，`me.orders` / `me.trades` 不再有新增消息。未充值或余额不足时，委托状态为
`REJECTED`，不会撮合，可通过 `GET /api/v1/orders` 查询。
重复发送同一个 `requestId` 只保存一次；同一 `orderId` 换 `requestId` 重复提交返回
`ORDER_ID_ALREADY_EXISTS`。

## 已实现：撮合内核 M0（集合版订单簿，委托模块进程内）

`com.me.matching` 是**委托与撮合模块内部的纯 JDK 撮合内核**：冻结回执成功后，委托模块的
`OrderMatchingService` 直接调用 `MatchingEngine.place()`，同步拿回 `MatchResult` 成交对并立即清算。
撮合内核（`entity` / `service`）零 Spring、零 Kafka；`com.me.matching.mq` 已删除，只保留
`matching.controller` 只读订单簿查询接口。`MatchingEngine` bean 仍由
`com.me.config.MatchingConfig` 注册。

### 进程内化与重投语义（演示版）

- 去掉 `me.orders`、`me.trades` 两次 Kafka 往返和两个消费阶段；一单从“冻结回执”直接进入撮合。
- 冻结回执按 symbol 分区：同一 symbol 的消息固定落一个分区、由同一个 consumer 处理，
  按 symbol 过滤后的子序列与旧 `me.orders` 路径一致，价格-时间优先不变。
  不同 symbol 可以由多个 consumer 并行撮合（当前 `order-freeze-result=2`）。
- 演示版**不做防重/重放缓存**：
  - `OrderServiceImpl` 不再缓存 `appliedRequests` / `appliedTrades`；
  - `AccountServiceImpl` 不再缓存 `appliedRequests` / `appliedSettlements`；
  - `OrderMatchingServiceImpl` 不再缓存 `MatchResult` / 行情 / 结算输出；
  - 同一订单重复回执由 `MatchingEngine.place()` 按 `orderId` 返回 duplicate，
    结果为 `DUPLICATE_IGNORED`，不重复发行情/结算；
  - 成交重复调用由订单状态（已 `FILLED`）拒绝；**部分成交的重复回执不保证去重**。
- Kafka 事务仍保证：已提交事务内的输出与 offset 原子提交；事务回滚后的重投可能丢失上一次
  未提交的撮合输出，这是演示版的取舍。
- 代价：订单不再写入 Kafka 事实源；订单/订单簿/幂等状态都是内存态，进程重启丢失。

### 后续计划：备份消息 + 内存状态恢复

当前演示版不引入额外的防重/重放代码，后续补齐：

1. **备份消息**：把订单、订单簿/撮合结果、幂等键写入备份 topic 或 compacted state topic，
   与现有 Kafka 事务一起提交；
2. **内存状态恢复**：启动时从备份 topic / 快照恢复 `orders`、订单簿、账户余额、
   `appliedRequests`、`appliedTrades`、`appliedSettlements` 等，再按已提交 offset 继续消费；
3. **持久化幂等**：用恢复后的状态替代当前内存缓存，实现模块级 exactly-once/重投重放；
4. 在完成前，Kafka 重投按订单状态/撮合引擎 `orderId` 去重，不额外做防重缓存。

### 订单簿结构

| 类型 | 结构 | 作用 |
|---|---|---|
| 限价单 | `TreeMap<price, LinkedHashMap<orderId, BookOrder>>`，买卖各一 | TreeMap 价格优先（买盘降序、卖盘升序），LinkedHashMap 同价 FIFO |
| 市价单 | `LinkedHashMap<orderId, BookOrder>`，买卖各一 | 按到达顺序 FIFO；市价单没有价格 |

### 撮合规则

1. 新委托**先与对手方限价单撮合**：买吃最低卖价、卖吃最高买价；限价单价格不穿越时停止。
2. 限价簿撮合后仍有剩余，且新委托是**限价单**时，**再与对手方市价单 FIFO 撮合**。
3. **市价单不与市价单撮合**：市价新单只吃限价单；普通市价单剩余量挂入市价簿，等待后续限价单来吃。
   **市价买入（`frozenAmount > 0`）是金额驱动的即时单**：每笔成交量还要满足
   「剩余预算 ÷ 卖价」，预算买不起下一单位即停止，剩余量不挂单；撮合结束后未用完的预算
   以 `ORDER_UNFREEZE` 清算消息退回可用余额。
4. 每笔成交**以数量为基准**：成交量 = `min(新单剩余量, 挂单剩余量)`；金额 = 成交量 × 成交价，
   再按 `priceScale / quantityScale / quoteScale` 换算成计价币最小单位。
5. 成交价：限价对限价取挂单（maker）价格；限价对市价取限价单价格（市价单价格恒为 0）。

### 事件链路

- 委托模块冻结回执 consumer（group `order-freeze-result-consumer`，当前 2 个 runner，
  按 symbol 分区分配）调用 `OrderMatchingService.match(OrderPlacedEvent, partition, offset)`；
- `MatchingEngine.place()` 同步返回 `MatchResult`，按 `orderId` 幂等去重；
- 委托模块先按 symbol 分组发布 `me.marketdata-batch` 行情批量消息，再发布
  `me.account.settlements-batch` 结算批量消息（`amount` 已按交易对 / 币种精度换算成计价币最小单位）；
- 行情/结算发送与 `me.order.freeze-results-batch` 消费 offset 在同一个 Kafka 事务内提交；
  行情模块按独立消费组消费 `me.marketdata-batch`（见「行情模块 M0」）。

## 已实现：成交清算 + 双边资金结清

### 链路

1. **撮合出成交对**：`MatchingEngine.place()` 同步返回每笔成交（taker / maker、价格、数量、金额），
   不再写入 `me.trades`。
2. **委托清算**：`OrderMatchingService` 在冻结回执监听线程中逐笔调用 `OrderService.onTrade()`，
   不再消费 `me.trades`：
   - taker、maker 两笔委托同时推进 `filledQuantity`；
   - 状态 `PAID → PARTIALLY_FILLED → FILLED`，每次推进 `seq + 1`；
   - 成交对不做内存幂等缓存；重复清算由订单状态（已全部成交）拒绝，部分成交的重复回执不保证去重。
3. **两条单用户命令**：每笔成交拆成买方、卖方两条 `TradeSettlementCommand` 写入
   `me.account.settlements-batch`（group `account-trade-settlement-consumer`），
   `requestId` 分别为 `<结算ID>-b` / `<结算ID>-s`：
   - 买方：冻结计价币扣账（`TRADE_SETTLE_OUT`）、可用基础币到账（`TRADE_SETTLE_IN`）；
   - 卖方：冻结基础币扣账、可用计价币到账；
   - 买单全部成交且成交价优于限价时，买方命令附一条 `ORDER_UNFREEZE` 把多冻结的计价币释放回可用。
4. **账户按命令原子结算**：账户模块对每条单用户命令涉及的账户分片锁内、先在副本上顺序执行
   该命令的全部资金腿，全部腿成功后才写回余额；任一腿余额不足则该条命令不生效并抛错重试。
   买卖两侧是两条独立命令，不保证跨用户原子性，可能出现一侧已结算、另一侧重试中的短暂状态。

### 消息示例（`me.account.settlements-batch` 中的 `TradeSettlementCommand` 元素，每笔成交两条，key=symbol）

```json
[
  {
    "requestId": "1794466000000-b",
    "tradeId": 1,
    "symbol": "BTC-USDT",
    "takerOrderId": 201,
    "makerOrderId": 101,
    "legs": [
      {"userId": 9201, "currency": "USDT", "logType": "TRADE_SETTLE_OUT", "amount": 98000},
      {"userId": 9201, "currency": "BTC",  "logType": "TRADE_SETTLE_IN",  "amount": 2},
      {"userId": 9201, "currency": "USDT", "logType": "ORDER_UNFREEZE",  "amount": 2000}
    ]
  },
  {
    "requestId": "1794466000000-s",
    "tradeId": 1,
    "symbol": "BTC-USDT",
    "takerOrderId": 201,
    "makerOrderId": 101,
    "legs": [
      {"userId": 9101, "currency": "BTC",  "logType": "TRADE_SETTLE_OUT", "amount": 2},
      {"userId": 9101, "currency": "USDT", "logType": "TRADE_SETTLE_IN",  "amount": 98000}
    ]
  }
]
```

### 幂等与事务

- 成交对（演示版）：委托模块不缓存清算结果；重复回执由撮合引擎 `orderId` 去重，
  已全部成交的重复成交由订单状态拒绝；部分成交重投不保证去重。
- 结算消息：`requestId` 由委托模块的进程内 `AtomicLong` 计数器生成（当前毫秒时间戳起步，避免每笔成交做两次 Long 转字符串 + 拼接），仅用于日志/审计/资金流水；演示版账户模块不做 requestId 防重，重投会再次执行。计数器后续随内存状态持久化/恢复。
- `me.order.freeze-results-batch` 消费 offset、`me.marketdata-batch` 行情与结算批量消息同事务提交；发送失败回滚 offset，重投会重发同一批消息。
- 账户结算成功时每条资金腿生成一条 `me.funds-batch` 流水（4~5 条/成交），与消费 offset 同事务。

## 已实现：行情模块 M0（成交行情 + K 线落 QuestDB）

委托模块在进程内撮合完成后，把每笔成交封装为 `MarketDataEvent` 写入
`me.marketdata-batch`（key=symbol）；行情模块以独立消费组消费，生成 K 线并把近期成交（每个 poll 批次最后 20 条）保存到 QuestDB。
行情写库与 K 线聚合在独立消费组中异步进行，不占用撮合/清算线程；行情消息与
`me.account.settlements-batch` 结算批量消息、冻结回执消费 offset 在同一个 Kafka 事务内发送，
任一发送失败会一起回滚并重投。

### 链路

1. **撮合输出行情**：`OrderMatchingService` 为每笔成交生成 `MarketDataEvent`（tradeId、symbol、
   taker / maker orderId、方向、价格、数量、金额、成交时间），由 `OrderFreezeResultListener`
   与结算命令一起发布。
2. **消费行情**：`com.me.marketdata.mq.MarketDataListener` 消费 `me.marketdata-batch`
   （group `marketdata-trade-consumer`），把记录与 `partition/offset` 交给 `MarketDataService`。
3. **成交明细**：每个 poll 批次只取**最后 20 条**成交写入 QuestDB `md_trade`（近期成交查询口径，
   不为历史全量保留）。指定时间戳 `executed_at`，DEDUP 键
   `(executed_at, symbol, taker_order_id, maker_order_id)` 保证消费重投不产生重复行。
4. **K 线**：按 `(symbol, interval)` 在内存聚合。同一 bucket 首次出现时立即写入，
   之后每 `app.marketdata.kline-write-every-batches`（默认 5）个消费批次再更新一次；
   跨 bucket 时强制写上一根的最终快照并补齐空 bucket。这样同一分钟只有“首次 + 每 N 批 + 收尾”
   几次写入，而不是每批都写。`source_partition/source_offset` 取写入快照时批内最后一条成交的水位。
   `open/high/low/close/volume/amount/trade_count` 按 bucket 累加；DEDUP 键
   `(bucket_start, symbol, bar_interval)` 等价于按 bucket upsert。续线创建新 bucket 时，
   `open` 取上一根 K 线的 `close`（`high/low` 同时覆盖 open 与首笔成交价），只有新线第一根
   以成交价开盘，从而保证相邻 K 线首尾相接。
   每个周期另有一行**锚点**（`md_kline_anchor`），记录已经计算到的最新 bucket：缓存未命中
   （进程重启 / 再均衡）时按锚点恢复——锚点早于新成交 bucket 时，把中间没有成交的 bucket
   补成平线 K 线（OHLC 沿用上一根收盘价，量为 0）并把锚点推进到新 bucket；锚点与成交 bucket
   相等时取出已有快照更新。锚点缺失（锚点表刚上线 / 锚点被清空）时不会直接当成新线，而是
   读取该周期在 `md_kline` 里最新的一根 K 线作为上一根，同样补齐它到事件 bucket 之间的空柱，
   并补写锚点；只有库里一根 K 线都没有时才以当前成交新建。锚点晚于成交 bucket 的迟到成交
   不参与 K 线计算。重投或进程重启后从锚点 / QuestDB 的最新快照续算，不会用不完整快照覆盖
   已有数据。
5. **周期配置**：`app.marketdata.kline-intervals`，默认 `1m`；可写
   `1m,5m,15m,30m,1h,4h,1d`，每个周期每个 bucket 按 `kline-write-every-batches` 节流写入
   （默认每 5 批更新一次，跨分钟写最终快照）。

### 查询 K 线

`GET /api/v1/marketdata/klines?symbol=BTC-USDT&interval=1m&limit=100`

- `symbol` 必填，统一转大写；
- `interval` 缺省 `1m`，支持 `1m / 5m / 15m / 30m / 1h / 4h / 1d`（只有
  `app.marketdata.kline-intervals` 配置过的周期才有数据）；
- `limit` 缺省 100、上限 1000；
- 返回最近 N 条 K 线，按 `bucketStart` 升序，可直接用于画图；同一交易对首次成交之后，
  两次成交之间没有成交的周期会由平线 K 线补齐（首次成交之前的区间不回溯生成）。

```bash
curl "http://localhost:8080/api/v1/marketdata/klines?symbol=BTC-USDT&interval=1m&limit=2"
```

```json
{
  "success": true,
  "reason": null,
  "symbol": "BTC-USDT",
  "interval": "1m",
  "klines": [
    {
      "symbol": "BTC-USDT",
      "interval": "1m",
      "bucketStart": "2026-11-08T10:00:00Z",
      "openPrice": 50000,
      "highPrice": 50000,
      "lowPrice": 50000,
      "closePrice": 50000,
      "volume": 1,
      "amount": 50000,
      "tradeCount": 1
    }
  ]
}
```

错误响应（400）：`SYMBOL_REQUIRED` / `INTERVAL_INVALID` / `LIMIT_INVALID`，格式同上，
`success=false`、`klines=[]`。

### 查询最新成交

`GET /api/v1/marketdata/trades?symbol=BTC-USDT&limit=50`

- `symbol` 必填，统一转大写；
- `limit` 缺省 50、上限 500；
- 返回 QuestDB `md_trade` 中最近的成交明细，按 `executedAt` 倒序（最新在前），
  `takerSide` 是吃单方向，历史数据缺失时为 `null`。

```bash
curl "http://localhost:8080/api/v1/marketdata/trades?symbol=BTC-USDT&limit=2"
```

```json
{
  "success": true,
  "reason": null,
  "symbol": "BTC-USDT",
  "trades": [
    {
      "tradeId": 1,
      "symbol": "BTC-USDT",
      "price": 5560000,
      "quantity": 100000000,
      "amount": 55600000000,
      "takerSide": "BUY",
      "executedAt": "2026-11-07T14:16:13.791262Z"
    }
  ]
}
```

错误响应（400）：`SYMBOL_REQUIRED` / `LIMIT_INVALID`。

### 查询订单簿

`GET /api/v1/marketdata/orderbook?symbol=BTC-USDT&depth=20`

- 直接读取撮合引擎内存订单簿，不经过 QuestDB；`symbol` 必填并统一转大写；
- `depth` 缺省 20、上限 100，返回买卖两边各最多 `depth` 个价位；
- `bids` 价格降序（买一在前）、`asks` 价格升序（卖一在前）；每个价位是
  `price / quantity / orderCount` 的聚合；
- 市价单没有价格，不进入价格梯，用 `marketBidCount` / `marketAskCount` 报告数量；
- 没有委托时返回 `success=true` 和空列表，`bestBid` / `bestAsk` 为 `null`。

```bash
curl "http://localhost:8080/api/v1/marketdata/orderbook?symbol=BTC-USDT&depth=5"
```

```json
{
  "success": true,
  "reason": null,
  "symbol": "BTC-USDT",
  "depth": 5,
  "bids": [{ "price": 4990000, "quantity": 300000000, "orderCount": 1 }],
  "asks": [{ "price": 5000000, "quantity": 200000000, "orderCount": 1 }],
  "bestBid": 4990000,
  "bestAsk": 5000000,
  "marketBidCount": 0,
  "marketAskCount": 0,
  "timestamp": "2026-11-08T10:00:00Z"
}
```

错误响应（400）：`SYMBOL_REQUIRED` / `SYMBOL_INVALID` / `DEPTH_INVALID`。

实现说明：订单簿由委托模块进程内撮合线程写入；REST 查询与写入共用同一个监视器锁，
保证快照一致性（M0 简化，后续可换无锁快照）。

### 本地验证

```powershell
# 下两笔可撮合委托（见上文 REST 下单）后，查看行情 topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic me.marketdata-batch --from-beginning

# QuestDB：最新成交
curl -G http://localhost:9000/exec `
  --data-urlencode "query=SELECT * FROM md_trade WHERE symbol = 'BTC-USDT' ORDER BY executed_at DESC, trade_id DESC LIMIT 20"
# QuestDB：1m K 线
curl -G http://localhost:9000/exec `
  --data-urlencode "query=SELECT * FROM md_kline WHERE symbol = 'BTC-USDT' AND bar_interval = '1m' ORDER BY bucket_start DESC LIMIT 20"
# QuestDB：K 线锚点
curl -G http://localhost:9000/exec `
  --data-urlencode "query=SELECT * FROM md_kline_anchor ORDER BY symbol, bar_interval"
```

预期：`md_trade` 每个 poll 批次写入最后 20 条成交；`md_kline` 每个 bucket 一行，OHLC / volume / amount /
trade_count 与成交一致，无成交的间隔由平线 K 线补齐；`md_kline_anchor` 每个
`(symbol, interval)` 一行，指向最新已计算的 bucket；重复消费 / 重启后值不回退。

## 已实现：币币 REST API（交易对列表 + 下单 + 账户余额查询）

新增 `spring-boot-starter-webmvc`（Spring Boot 4 推荐的 MVC starter），单体启动后监听 `8080`。

### 交易对列表

`GET /api/v1/pairs` 返回全部上线交易对（`pair_symbol.status = 1`），按 `sort_order`、`id` 升序：

```bash
curl http://localhost:8080/api/v1/pairs
```

```json
[
  {
    "id": 1,
    "symbol": "BTC-USDT",
    "baseCurrency": "BTC",
    "quoteCurrency": "USDT",
    "priceScale": 2,
    "quantityScale": 8,
    "minOrderQuantity": 0,
    "minOrderAmount": 0,
    "makerFeeBps": 10,
    "takerFeeBps": 20,
    "status": 1,
    "sortOrder": 1
  }
]
```

演示数据见 [`doc/mysql/pair_seed.sql`](doc/mysql/pair_seed.sql)，建表脚本为
[`pair_currency.sql`](doc/mysql/pair_currency.sql) / [`pair_symbol.sql`](doc/mysql/pair_symbol.sql)：

```powershell
mysql --default-character-set=utf8mb4 -uadmin -padmin db_exchange < doc/mysql/pair_seed.sql
```

### 下单

`POST /api/v1/currency/orders`（币币模块）。该接口只做同步校验，并把委托命令非阻塞写入
Disruptor；聚合线程按「2000 条或 20ms」组装成 `OrderCommandBatch`，按 symbol 分组写入
Kafka `me.order.commands-batch`（`key=symbol`，**不同步创建委托、不冻结资金**）；委托模块的
`OrderCommandListener` 消费命令，是**唯一接单入口**。价格按 `10^-priceScale`、数量按
`10^-quantityScale` 的整数提交；冻结 / 成交金额由后端按
`priceScale / quantityScale / quoteScale` 换算成计价币最小单位：

```bash
curl -X POST http://localhost:8080/api/v1/currency/orders \
  -H "Content-Type: application/json" \
  -d '{"requestId":"req-1","orderId":5002,"userId":1001,
       "symbol":"BTC-USDT","side":"BUY","type":"LIMIT","price":50000,"quantity":2}'
```

成功返回 `202 Accepted`，body 是命令回执，不含订单快照：

```json
{
  "success": true,
  "reason": null,
  "requestId": "req-1",
  "orderId": 5002,
  "symbol": "BTC-USDT"
}
```

- **接口内同步部分**：基础字段校验 → 交易对存在且上线 → 最小下单量 / 金额 → 金额溢出 →
  `requestId` / `orderId` 缺省生成 → 非阻塞写入 Disruptor（**不等待 Kafka 聚合或
  broker ack**；202 只表示命令已进入 ring，ring 满时返回 503）。
- **异步部分**：Kafka broker ack、委托保存、`ACCEPTED → PAID / REJECTED`、进程内撮合、
  成交清算全部由委托模块消费 `me.order.commands-batch` 后推进。重复 `requestId`、同一 `orderId`
  换 `requestId`（`ORDER_ID_ALREADY_EXISTS`）、余额不足（`REJECTED`）、成交状态都不会通过
  本接口返回，可通过 `GET /api/v1/orders` 观察。
- **模块边界**：币币模块不 import / 调用任何 `com.me.order` 类型或服务；
  `OrderCommandListener` 是唯一接单入口。
- **字段**：`requestId` 缺省时服务端生成 UUID，客户端重试应带上；`orderId` 缺省时服务端生成，
  也可显式传入（压测 / 对账场景）。
- **校验错误**（`400 Bad Request`）：`SYMBOL_NOT_FOUND`、`SYMBOL_NOT_TRADING`、
  `BELOW_MIN_ORDER_QUANTITY`、`BELOW_MIN_ORDER_AMOUNT`、`PRICE_REQUIRED_FOR_LIMIT`、
  `PRICE_NOT_ALLOWED_FOR_MARKET`、`QUANTITY_INVALID`、`AMOUNT_OVERFLOW`、
  `AMOUNT_REQUIRED_FOR_MARKET_BUY`、`AMOUNT_NOT_ALLOWED` 等。
- **同步入队失败**（producer 已关闭、序列化失败、缓冲区满等）返回
  `503 Service Unavailable` + `ORDER_COMMAND_PUBLISH_FAILED`；入队后的 broker 侧发送失败
  只在服务端记录错误日志，不会返回给调用方。客户端可用同一 `requestId` 重试；演示版委托模块只在
  同一 `orderId + requestId` 时返回已有委托，不做 requestId 级别缓存。
- **REST 线程没有消费批次事务**：`OrderCommandPublisher` 通过 `KafkaRestPublisher` 的幂等、
  **非事务** producer 池异步发送（`enable.idempotence=true` + `acks=all`，只调用 `send`
  入队、不等待 ack）。下游消费端仍为 `read_committed`：非事务消息对 read_committed 消费者可见，
  不冲突；只有排在未提交事务之后的非事务消息会等到该事务结束（LSO），不会丢消息或乱序。
- **未实现**：撤单、鉴权、小数价格 / 数量的精度换算（接口只收最小单位整数）。

### 账户余额查询

`GET /api/v1/accounts/{userId}/balances` 按用户 ID 返回该用户全部币种的余额，按币种升序；
用户没有任何账户时返回空列表。金额都是币种最小单位的整数，`available` / `frozen`
分别为可用、冻结余额，`seq` 是账户变更序号：

```bash
curl http://localhost:8080/api/v1/accounts/1001/balances
```

```json
{
  "success": true,
  "reason": null,
  "userId": 1001,
  "balances": [
    { "userId": 1001, "currency": "BTC", "available": 2, "frozen": 0, "seq": 1 },
    { "userId": 1001, "currency": "USDT", "available": 0, "frozen": 0, "seq": 3 }
  ]
}
```

`userId <= 0` 返回 `400 Bad Request` + `USER_ID_INVALID`。余额是内存态，进程重启后清零。

### 币种列表

`GET /api/v1/currencies` 返回启用中的币种及其精度（`pair_currency.scale`），按展示顺序升序。
前端用 `scale` 把余额、价格、数量等最小单位整数换算成展示值：

```bash
curl http://localhost:8080/api/v1/currencies
```

```json
[
  { "id": 1, "code": "BTC", "name": "比特币", "scale": 8, "status": 1, "sortOrder": 1 },
  { "id": 3, "code": "USDT", "name": "泰达币", "scale": 6, "status": 1, "sortOrder": 3 }
]
```

### 管理端充值（模拟登录）

`POST /api/v1/admin/recharges` 接收用户 ID、币种和**展示金额**（按币种单位计的整数，
缺省 1,000,000，即 100 万），按 `pair_currency.scale` 换算成账本最小单位后，
向 `me.account.commands` 投递一条 `DEPOSIT` 命令：

```bash
curl -X POST http://localhost:8080/api/v1/admin/recharges \
  -H "Content-Type: application/json" \
  -d '{"userId":1001,"currency":"USDT","amount":1000000}'
```

成功返回 `202 Accepted`（充值异步生效，余额以账户查询接口为准）：

```json
{
  "success": true,
  "reason": null,
  "requestId": "admin-deposit-921c705c-b06d-4a67-855d-f239e8f6b6e0",
  "userId": 1001,
  "currency": "USDT",
  "amount": 1000000000000,
  "displayAmount": 1000000,
  "scale": 6
}
```

- `amount` 是实际写入账本的最小单位整数；`displayAmount` 是请求里的展示金额；
- `currency` 必须是 `pair_currency` 里启用的币种；
- 错误（400）：`USER_ID_REQUIRED` / `USER_ID_INVALID` / `CURRENCY_REQUIRED` /
  `CURRENCY_INVALID` / `CURRENCY_NOT_FOUND` / `AMOUNT_INVALID` / `AMOUNT_OVERFLOW`；
  Kafka 投递失败返回 503 + `RECHARGE_PUBLISH_FAILED`；
- **演示接口，没有鉴权**，部署到公网前必须加权限控制。

## 演示前端 `web/`

`web/` 是一个 React 19 + TypeScript + Vite 的单页应用，顶栏并排「币币交易 / 后台管理」
两个栏目；交易图表使用 [lightweight-charts](https://github.com/tradingview/lightweight-charts)：

| 区域 | 对接接口 | 刷新 |
|---|---|---|
| 交易对列表 / 最新价 | `GET /api/v1/pairs` + 每个交易对 `GET /api/v1/marketdata/klines?limit=1` | 30s / 15s |
| 1m K 线 | `GET /api/v1/marketdata/klines?symbol=...&interval=1m&limit=180` | 5s |
| 订单簿 | `GET /api/v1/marketdata/orderbook?symbol=...&depth=10` | 3s |
| 最新成交 | `GET /api/v1/marketdata/trades?symbol=...&limit=50` | 3s |
| 当前委托 | `GET /api/v1/orders?userId=...&limit=100` | 3s |
| 资产余额 | `GET /api/v1/accounts/{userId}/balances` | 5s |
| 币种精度 | `GET /api/v1/currencies` | 60s |
| 下单委托 | `POST /api/v1/currency/orders`（202，异步提交） | 手动 |
| 模拟登录 / 充值 | `POST /api/v1/admin/recharges` | 手动 |
| 用户历史流水 | `GET /api/v1/currency/funds-flows` | 5s |
| 后台流水管理 | `GET/DELETE /api/v1/admin/funds-flows` | 手动 |
| 后台 K 线管理 | `GET/DELETE /api/v1/admin/klines` | 手动 |

前端功能：

- **币币交易**：点击 BTC/USDT 标题旁的弹框搜索 / 切换交易对；下单表单左侧为
  「订单簿 / 最新成交」共享栏目，通过顶部左右标签切换（订单簿展示价格档位与
  累计深度条，中间行展示最新成交价、买卖盘上下各半空间；最新成交展示价格 / 数量 / 时间，
  均 3 秒轮询）；1m 蜡烛图 + 成交量柱
  （缩放 / 拖拽、十字光标 OHLC 图例）；图表下方「当前委托 / 历史流水」面板通过顶部标签
  切换：当前委托显示方向、价格、数量、已成交与状态（3 秒轮询），历史流水展示当前用户
  QuestDB `funds_flow` 记录，按发生时间倒序、最新在前（5 秒轮询）；资产余额
  （可用 / 冻结，按币种精度换算）；限价 / 市价、
  买入 / 卖出表单（25% / 50% / 75% / 100% 快捷填量，提交前校验最小下单量与最小金额，
  提交前还用轮询到的余额快照校验冻结金额 / 数量：不足时展示「可用余额不足」及差额，
  并主动刷新余额后让用户重试；最终仍以后端冻结回执为准）；自动生成 `requestId` 仅作命令
  标识，演示版不用于防重。
- **后台管理**：左侧为「流水管理 / K 线管理」子栏目。流水管理按用户 / 币种 / 日志类型
  查询 QuestDB `funds_flow`，展示金额、余额前后值、原子操作与 `seq`；K 线管理按交易对 /
  周期查询 `md_kline`，展示 OHLC、成交量、成交额与笔数。两个子栏目都提供「一键删除」，
  执行前二次确认，使用 `TRUNCATE TABLE` 清空对应投影。
- 「模拟登录 / 充值」弹框：「确认充值」调用 `POST /api/v1/admin/recharges` 为目标用户
  入账并切换为当前用户，弹框保持打开，可继续选择其他币种充值；「登录」只切换用户、
  不充值。资产面板提供「刷新余额」按钮。

### 启动

先启动后端（默认 `8080`，需要 MySQL / Kafka / QuestDB 就绪），再启动前端：

```bash
cd web
npm install
npm run dev
```

打开 <http://localhost:5173>。Vite 把 `/api` 代理到 `http://localhost:8080`，可用环境变量
`VITE_PROXY_TARGET` 覆盖后端地址；生产构建：

```bash
npm run build      # 产物在 web/dist
```

> 余额是内存态，重启后端会清零。本地演示可向 Kafka `me.account.commands` 发送
> `DEPOSIT` 命令充值（见上文「本地验证」），前端 5 秒内会刷新出来。
>
> 价格按交易对 `priceScale`、数量按 `quantityScale`、余额 / 冻结 / 成交金额按币种 `scale`
> 的整数口径传输。前端负责展示值和小数输入的换算；订单冻结与成交金额按
> `price × quantity × 10^quoteScale / 10^(priceScale + quantityScale)` 统一换算。
