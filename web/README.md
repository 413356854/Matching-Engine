# ME Exchange · 币币交易 / 后台管理前端

React 19 + TypeScript + Vite 的单页应用，对接 `matching-engine` 的 `/api/v1` REST 接口。
顶栏并排两个栏目：**币币交易** 与 **后台管理**。

## 功能

### 币币交易

- 点击 BTC/USDT 标题旁的弹框搜索 / 切换交易对，展示最新价（30s / 15s 轮询）
- 订单簿：与最新成交共享下单表单左侧的栏目，价格档位、同价聚合数量与累计深度条；
  中间行展示最新成交价（按最近一笔成交方向着色），卖盘在上、买盘在下，上下各占一半空间
  （3s 轮询）
- 1m K 线蜡烛图 + 成交量柱（`lightweight-charts`，5s 轮询，支持缩放 / 拖拽 / 十字光标）
- 最新成交：与订单簿同一栏目，通过顶部「订单簿 / 最新成交」标签左右切换，展示价格 /
  数量 / 时间（3s 轮询）
- 当前委托：图表下方独立面板，展示用户未完成委托的方向、价格、数量、已成交和状态
  （3s 轮询）
- 资产余额：可用 / 冻结，按 `pair_currency.scale` 换算；面板右上角提供「刷新余额」按钮
- 下单：限价 / 市价，买入 / 卖出，25% / 50% / 75% / 100% 快捷填量，
  最小下单量 / 最小金额前端校验；余额由后端冻结时校验（前端余额是轮询快照，避免刚充值/
  切换用户时误拦截）。下单接口只把命令写入 Kafka，返回 202 后委托状态由「当前委托」轮询展示
- 右上角「模拟登录 / 充值」弹框：「确认充值」按金额为目标用户入账并切换为当前用户；
  「登录」只切换用户、不充值
- `requestId` 幂等重试；下单命令提交后自动刷新余额和行情

### 后台管理

左侧子栏目切换，内容区独立滚动：

- **流水管理**：按用户 ID / 币种 / 日志类型查询 QuestDB `funds_flow`，按发生时间倒序展示
  金额、可用 / 冻结余额前后值、原子操作与 `seq`；支持一键清空全部流水。
- **K 线管理**：按交易对 / 周期查询 QuestDB `md_kline`，按 bucket 起点倒序展示 OHLC、
  成交量、成交额与笔数；支持一键清空全部 K 线（成交明细 `md_trade` 不受影响）。

删除操作使用 QuestDB `TRUNCATE TABLE`，执行前会二次确认，清空后不可恢复。

## 接口

| 接口 | 用途 |
|---|---|
| `GET /api/v1/pairs` | 交易对列表 |
| `GET /api/v1/currencies` | 币种精度（余额 / 数量换算） |
| `GET /api/v1/accounts/{userId}/balances` | 账户余额 |
| `GET /api/v1/marketdata/klines?symbol=&interval=1m&limit=180` | 1m K 线 |
| `GET /api/v1/marketdata/orderbook?symbol=&depth=10` | 内存订单簿深度（3s 轮询） |
| `GET /api/v1/marketdata/trades?symbol=&limit=50` | 最新成交 |
| `POST /api/v1/currency/orders` | 提交下单命令（202，异步写入 `me.order.commands`） |
| `GET /api/v1/orders?userId=&symbol=&limit=100` | 用户当前未完成委托（3s 轮询） |
| `POST /api/v1/admin/recharges` | 模拟登录 / 管理端充值（`amount` 为展示金额，默认 100 万） |
| `GET /api/v1/admin/funds-flows` | 后台流水列表（`userId` / `currency` / `logType` / `limit` 可选） |
| `DELETE /api/v1/admin/funds-flows` | 后台一键清空全部流水 |
| `GET /api/v1/admin/klines` | 后台 K 线列表（`symbol` / `interval` / `limit` 可选） |
| `DELETE /api/v1/admin/klines` | 后台一键清空全部 K 线 |

## 启动

后端（MySQL / Kafka / QuestDB 就绪）启动在 `http://localhost:8080` 后：

```bash
cd web
npm install
npm run dev
```

打开 <http://localhost:5173>。开发服务器把 `/api` 代理到 `http://localhost:8080`。

可选环境变量（`web/.env.local`）：

```bash
# 后端地址（默认 http://localhost:8080）
VITE_PROXY_TARGET=http://localhost:8080

# 不走 Vite 代理、直接请求完整 API 前缀时使用
VITE_API_BASE=http://localhost:8080
```

## 构建

```bash
npm run build     # tsc 类型检查 + vite build，产物在 dist/
npm run preview   # 本地预览 dist/
```

## 数值口径

后端返回的数值都是整数，但精度口径不同：

- 价格：`10^-priceScale`，例如 `5560000` + `priceScale=2` → `55,600.00`；
- 数量：`10^-quantityScale`，例如 `100000000` + `quantityScale=8` → `1`；
- 账户余额 / 冻结 / 成交金额：币种 `pair_currency.scale` 位小数的最小单位；
- 价格 × 数量的金额换算：`price × quantity × 10^quoteScale / 10^(priceScale + quantityScale)`，
  前端 `quoteAmount()`（`src/format.ts`）与后端 `com.me.shared.money.Amounts` 使用同一公式。

前端用 BigInt 做十进制 ↔ 整数单位的精确换算，输入的小数位不能超过交易对精度。
账户冻结余额以后端账本口径为准，界面统一显示为「冻结」。

## 目录

```
src/
├── api.ts                 # REST 客户端与类型
├── format.ts              # 最小单位 / 十进制格式化（BigInt）
├── errors.ts              # 后端错误码 → 中文提示
├── hooks/usePolling.ts    # 带 AbortController 的轮询
├── components/
│   ├── Header.tsx         # 顶栏、栏目切换、用户切换、连接状态
│   ├── MarketPickerModal.tsx # 交易对选择弹框
│   ├── BookTradesPanel.tsx # 订单簿 / 最新成交切换栏目
│   ├── OrderBookPanel.tsx # 订单簿深度内容（3s 轮询）
│   ├── LatestTradesPanel.tsx # 最新成交内容（3s 轮询）
│   ├── ChartPanel.tsx     # 1m K 线 + 成交量
│   ├── OrderForm.tsx      # 下单表单
│   ├── BalancesPanel.tsx  # 资产余额
│   ├── RechargeModal.tsx  # 模拟登录 / 充值弹框
│   ├── AdminPage.tsx      # 后台管理左右布局与子栏目
│   ├── FundsFlowAdmin.tsx # 流水管理（查询 / 一键删除）
│   ├── KlineAdmin.tsx     # K 线管理（查询 / 一键删除）
│   └── ToastStack.tsx     # 操作提示
├── App.tsx                # 数据轮询、栏目切换与页面布局
└── styles.css             # 深色交易终端样式
```
