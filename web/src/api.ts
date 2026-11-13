/**
 * 后端 REST API 客户端。
 *
 * 开发环境下所有请求都走 Vite 代理（`/api` → `http://localhost:8080`），
 * 也可以通过 `VITE_API_BASE` 指定完整的 API 前缀。
 *
 * 约定：价格、数量、金额都是**最小单位整数**，展示前必须按对应的 scale 换算。
 */

export type OrderSide = 'BUY' | 'SELL';
export type OrderType = 'LIMIT' | 'MARKET';
export type OrderStatus =
  | 'ACCEPTED'
  | 'PAID'
  | 'PARTIALLY_FILLED'
  | 'FILLED'
  | 'CANCELED'
  | 'REJECTED';

export interface TradingPair {
  id: number;
  symbol: string;
  baseCurrency: string;
  quoteCurrency: string;
  /** 价格小数位数。 */
  priceScale: number;
  /** 数量小数位数。 */
  quantityScale: number;
  /** 最小下单量，基础币最小单位。 */
  minOrderQuantity: number;
  /** 最小下单金额，计价币最小单位。 */
  minOrderAmount: number;
  makerFeeBps: number;
  takerFeeBps: number;
  status: number;
  sortOrder: number;
}

export interface Currency {
  id: number;
  code: string;
  name: string;
  /** 小数位数：1 BTC = 10^8 最小单位。 */
  scale: number;
  status: number;
  sortOrder: number;
}

export interface AccountBalance {
  userId: number;
  currency: string;
  /** 可用余额，币种最小单位。 */
  available: number;
  /** 冻结余额，币种最小单位。 */
  frozen: number;
  seq: number;
}

interface AccountBalancesResponse {
  success: boolean;
  reason: string | null;
  userId: number;
  balances: AccountBalance[];
}

export interface Kline {
  symbol: string;
  interval: string;
  /** ISO-8601，UTC。 */
  bucketStart: string;
  openPrice: number;
  highPrice: number;
  lowPrice: number;
  closePrice: number;
  /** 基础币成交量，最小单位。 */
  volume: number;
  /** 计价币成交额，最小单位。 */
  amount: number;
  tradeCount: number;
}

interface KlineQueryResponse {
  success: boolean;
  reason: string | null;
  symbol: string;
  interval: string;
  klines: Kline[];
}

/** 订单簿中的一个价格档位（同价挂单聚合）。 */
export interface DepthLevel {
  price: number;
  quantity: number;
  orderCount: number;
}

/** 撮合引擎内存订单簿的只读快照。 */
export interface OrderBookDepth {
  symbol: string;
  depth: number;
  /** 买盘，价格降序（买一在前）。 */
  bids: DepthLevel[];
  /** 卖盘，价格升序（卖一在前）。 */
  asks: DepthLevel[];
  bestBid: number | null;
  bestAsk: number | null;
  /** 无价格的市价挂单数量，不进入价格梯。 */
  marketBidCount: number;
  marketAskCount: number;
  /** ISO-8601，UTC。 */
  timestamp: string;
}

interface OrderBookQueryResponse extends OrderBookDepth {
  success: boolean;
  reason: string | null;
}

/** 支持的后台 K 线周期（与后端 KlineInterval 对齐）。 */
export const KLINE_INTERVALS = ['1m', '5m', '15m', '30m', '1h', '4h', '1d'] as const;

export type FundsLogType =
  | 'DEPOSIT'
  | 'WITHDRAW'
  | 'ORDER_FREEZE'
  | 'ORDER_UNFREEZE'
  | 'TRADE_SETTLE_IN'
  | 'TRADE_SETTLE_OUT'
  | 'FEE'
  | 'FROZEN_ADJUST_IN'
  | 'FROZEN_ADJUST_OUT';

/** 资金日志类型及中文描述，用于后台筛选下拉框。 */
export const FUNDS_LOG_TYPES: ReadonlyArray<{ value: FundsLogType; label: string }> = [
  { value: 'DEPOSIT', label: '充值（可用入账）' },
  { value: 'WITHDRAW', label: '提现/扣账（可用扣账）' },
  { value: 'ORDER_FREEZE', label: '下单冻结（可用转冻结）' },
  { value: 'ORDER_UNFREEZE', label: '撤单解冻（冻结转可用）' },
  { value: 'TRADE_SETTLE_IN', label: '成交收入（可用入账）' },
  { value: 'TRADE_SETTLE_OUT', label: '成交支出（冻结扣账）' },
  { value: 'FEE', label: '手续费（可用扣账）' },
  { value: 'FROZEN_ADJUST_IN', label: '冻结调增（冻结入账）' },
  { value: 'FROZEN_ADJUST_OUT', label: '冻结调减（冻结扣账）' },
];

/** 一条资金流水（QuestDB funds_flow）。 */
export interface FundsFlow {
  eventId: string;
  requestId: string;
  userId: number;
  currency: string;
  logType: FundsLogType;
  /** 原子操作：AVAILABLE_CREDIT / AVAILABLE_DEBIT / ... */
  op: string;
  logDesc: string;
  /** 金额与余额都是币种最小单位的整数。 */
  amount: number;
  availableBefore: number;
  availableAfter: number;
  frozenBefore: number;
  frozenAfter: number;
  seq: number;
  /** ISO-8601，UTC。 */
  occurredAt: string;
}

export interface FundsFlowQuery {
  userId?: number | null;
  currency?: string;
  logType?: string;
  limit?: number;
}

interface FundsFlowQueryResponse {
  success: boolean;
  reason: string | null;
  userId: number | null;
  currency: string | null;
  logType: string | null;
  flows: FundsFlow[];
}

interface UserFundsFlowQueryResponse {
  success: boolean;
  reason: string | null;
  userId: number | null;
  currency: string | null;
  flows: FundsFlow[];
}

interface FundsFlowClearResponse {
  success: boolean;
  reason: string | null;
  deletedCount: number;
}

export interface AdminKlineQuery {
  symbol?: string;
  interval?: string;
  limit?: number;
}

interface AdminKlineQueryResponse {
  success: boolean;
  reason: string | null;
  symbol: string | null;
  interval: string | null;
  klines: Kline[];
}

interface AdminKlineClearResponse {
  success: boolean;
  reason: string | null;
  deletedCount: number;
}

export interface Trade {
  tradeId: number;
  symbol: string;
  price: number;
  quantity: number;
  amount: number;
  /** 吃单方向；历史数据缺失时为 null。 */
  takerSide: OrderSide | null;
  /** ISO-8601，UTC。 */
  executedAt: string;
}

interface TradeQueryResponse {
  success: boolean;
  reason: string | null;
  symbol: string;
  trades: Trade[];
}

export interface PlaceOrderRequest {
  requestId: string;
  userId: number;
  symbol: string;
  side: OrderSide;
  type: OrderType;
  /** 限价单必填（> 0），市价单传 0 或省略。 */
  price: number;
  quantity: number;
  /**
   * 市价买入必填：冻结预算（计价币最小单位）。
   * 前端按卖一价（没有则最新价）× 1.05 × 数量计算；其他委托省略。
   */
  amount?: number;
}

export interface OrderView {
  orderId: number;
  userId: number;
  symbol: string;
  side: OrderSide;
  type: OrderType;
  price: number;
  quantity: number;
  filledQuantity: number;
  frozenCurrency: string;
  frozenAmount: number;
  status: OrderStatus;
  seq: number;
  createdAt: string;
}

export interface PlaceOrderResponse {
  success: boolean;
  reason: string | null;
  requestId: string | null;
  orderId: number | null;
  symbol: string | null;
}

/** 撤单请求：symbol 与 orderId 由前端提供，orderId 定位待撤销委托。 */
export interface CancelOrderRequest {
  requestId: string;
  userId: number;
  symbol: string;
  orderId: number;
}

export interface CancelOrderResponse {
  success: boolean;
  reason: string | null;
  requestId: string | null;
  orderId: number | null;
  symbol: string | null;
}

interface OrderQueryResponse {
  success: boolean;
  reason: string | null;
  userId: number;
  symbol: string | null;
  orders: OrderView[];
}

/**
 * 一条历史委托（QuestDB ord_history）：只包含撤单（CANCELED）与撮合完成（FILLED）。
 * 价格、数量、金额均为对应精度的最小单位整数。
 */
export interface OrderHistory {
  orderId: number;
  requestId: string;
  userId: number;
  symbol: string;
  side: OrderSide;
  type: OrderType;
  status: OrderStatus;
  price: number;
  quantity: number;
  filledQuantity: number;
  frozenCurrency: string;
  frozenAmount: number;
  priceScale: number;
  quantityScale: number;
  quoteScale: number;
  seq: number;
  /** ISO-8601，UTC。 */
  createdAt: string;
  /** 委托进入终态的时间，ISO-8601，UTC。 */
  finalAt: string;
}

interface OrderHistoryQueryResponse {
  success: boolean;
  reason: string | null;
  userId: number;
  symbol: string | null;
  orders: OrderHistory[];
}

export interface AdminOrderHistoryQuery {
  userId?: number | null;
  symbol?: string;
  status?: 'FILLED' | 'CANCELED';
  limit?: number;
}

interface AdminOrderHistoryQueryResponse {
  success: boolean;
  reason: string | null;
  userId: number | null;
  symbol: string | null;
  status: string | null;
  orders: OrderHistory[];
}

/** 委托历史支持的终态筛选（与后端 ord_history.status 对齐）。 */
export const ORDER_HISTORY_STATUSES: ReadonlyArray<{
  value: 'FILLED' | 'CANCELED';
  label: string;
}> = [
  { value: 'FILLED', label: '撮合完成' },
  { value: 'CANCELED', label: '已撤销' },
];

export interface RechargeResponse {
  success: boolean;
  reason: string | null;
  requestId: string | null;
  userId: number;
  currency: string;
  /** 实际写入账本的最小单位整数。 */
  amount: number;
  /** 请求里的展示金额（币种单位，整数）。 */
  displayAmount: number;
  /** 币种精度。 */
  scale: number;
}

export class ApiError extends Error {
  readonly reason: string;
  readonly status: number;
  readonly payload: unknown;

  constructor(reason: string, status: number, payload?: unknown) {
    super(reason);
    this.name = 'ApiError';
    this.reason = reason;
    this.status = status;
    this.payload = payload;
  }
}

const API_BASE = (import.meta.env.VITE_API_BASE ?? '').replace(/\/$/, '');

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${API_BASE}${path}`, {
      ...init,
      headers: {
        Accept: 'application/json',
        ...(init.body ? { 'Content-Type': 'application/json' } : {}),
        ...init.headers,
      },
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw error;
    }
    throw new ApiError('NETWORK_ERROR', 0);
  }

  const text = await response.text();
  let body: unknown = null;
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = text;
    }
  }

  if (!response.ok) {
    const reason = extractReason(body) ?? `HTTP_${response.status}`;
    throw new ApiError(reason, response.status, body);
  }

  return body as T;
}

function extractReason(body: unknown): string | null {
  if (body && typeof body === 'object' && 'reason' in body) {
    const reason = (body as { reason?: unknown }).reason;
    return typeof reason === 'string' && reason.length > 0 ? reason : null;
  }
  return null;
}

export function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'AbortError';
}

export async function fetchPairs(signal?: AbortSignal): Promise<TradingPair[]> {
  const body = await request<TradingPair[] | { reason?: string }>(
    '/api/v1/pairs',
    { signal },
  );
  if (!Array.isArray(body)) {
    throw new ApiError(extractReason(body) ?? 'PAIRS_UNAVAILABLE', 200, body);
  }
  return body;
}

export async function fetchCurrencies(signal?: AbortSignal): Promise<Currency[]> {
  const body = await request<Currency[]>('/api/v1/currencies', { signal });
  if (!Array.isArray(body)) {
    throw new ApiError('CURRENCIES_UNAVAILABLE', 200, body);
  }
  return body;
}

export async function fetchBalances(
  userId: number,
  signal?: AbortSignal,
): Promise<AccountBalance[]> {
  const body = await request<AccountBalancesResponse>(
    `/api/v1/accounts/${encodeURIComponent(String(userId))}/balances`,
    { signal },
  );
  if (!body.success) {
    throw new ApiError(body.reason ?? 'BALANCES_REJECTED', 200, body);
  }
  return body.balances ?? [];
}

export async function fetchKlines(
  symbol: string,
  limit = 180,
  signal?: AbortSignal,
): Promise<Kline[]> {
  const query = new URLSearchParams({
    symbol,
    interval: '1m',
    limit: String(limit),
  });
  const body = await request<KlineQueryResponse>(
    `/api/v1/marketdata/klines?${query.toString()}`,
    { signal },
  );
  if (!body.success) {
    throw new ApiError(body.reason ?? 'KLINES_REJECTED', 200, body);
  }
  return body.klines ?? [];
}

/** 查询撮合引擎内存订单簿，depth 为买卖两边各自最多返回的价位数量。 */
export async function fetchOrderBook(
  symbol: string,
  depth = 20,
  signal?: AbortSignal,
): Promise<OrderBookDepth> {
  const query = new URLSearchParams({ symbol, depth: String(depth) });
  const body = await request<OrderBookQueryResponse>(
    `/api/v1/marketdata/orderbook?${query.toString()}`,
    { signal },
  );
  if (!body.success) {
    throw new ApiError(body.reason ?? 'ORDER_BOOK_REJECTED', 200, body);
  }
  return body;
}

export async function fetchTrades(
  symbol: string,
  limit = 50,
  signal?: AbortSignal,
): Promise<Trade[]> {
  const query = new URLSearchParams({ symbol, limit: String(limit) });
  const body = await request<TradeQueryResponse>(
    `/api/v1/marketdata/trades?${query.toString()}`,
    { signal },
  );
  if (!body.success) {
    throw new ApiError(body.reason ?? 'TRADES_REJECTED', 200, body);
  }
  return body.trades ?? [];
}

/**
 * 提交下单命令：只写入 Kafka 并返回 202 命令回执；委托状态由「当前委托」轮询获取。
 */
export function placeOrder(
  order: PlaceOrderRequest,
  signal?: AbortSignal,
): Promise<PlaceOrderResponse> {
  return request<PlaceOrderResponse>('/api/v1/currency/orders', {
    method: 'POST',
    body: JSON.stringify(order),
    signal,
  });
}

/**
 * 提交撤单命令：只写入独立撤单 Disruptor 并返回 202 命令回执；
 * 委托是否可撤、解冻结果由「当前委托」与「资产余额」轮询观察。
 */
export function cancelOrder(
  cancel: CancelOrderRequest,
  signal?: AbortSignal,
): Promise<CancelOrderResponse> {
  return request<CancelOrderResponse>('/api/v1/currency/orders/cancel', {
    method: 'POST',
    body: JSON.stringify(cancel),
    signal,
  });
}

/** 查询用户当前未完成委托（等待冻结 / 撮合中 / 部分成交），最新在前。 */
export async function fetchOpenOrders(
  userId: number,
  limit = 100,
  signal?: AbortSignal,
): Promise<OrderView[]> {
  const query = new URLSearchParams({ userId: String(userId), limit: String(limit) });
  const body = await request<OrderQueryResponse>(
    `/api/v1/orders?${query.toString()}`,
    { signal },
  );
  if (!body.success) {
    throw new ApiError(body.reason ?? 'ORDER_LIST_REJECTED', 200, body);
  }
  return body.orders ?? [];
}

/**
 * 查询当前用户的历史委托（QuestDB ord_history）：已撤单 / 已撮合完成，按最终时间倒序。
 * symbol 可选；不传时返回该用户全部交易对的历史。
 */
export async function fetchOrderHistory(
  userId: number,
  symbol?: string,
  limit = 50,
  signal?: AbortSignal,
): Promise<OrderHistory[]> {
  const params = new URLSearchParams({
    userId: String(userId),
    limit: String(limit),
  });
  if (symbol) {
    params.set('symbol', symbol);
  }
  const body = await request<OrderHistoryQueryResponse>(
    `/api/v1/orders/history?${params.toString()}`,
    { signal },
  );
  if (!body.success) {
    throw new ApiError(body.reason ?? 'ORDER_HISTORY_REJECTED', 200, body);
  }
  return body.orders ?? [];
}

/**
 * 查询当前用户的历史资金流水（QuestDB funds_flow），按发生时间倒序（最新在前）。
 * 只返回该用户的记录，供交易页面的「历史流水」面板展示。
 */
export async function fetchUserFundsFlows(
  userId: number,
  limit = 50,
  signal?: AbortSignal,
): Promise<FundsFlow[]> {
  const params = new URLSearchParams({
    userId: String(userId),
    limit: String(limit),
  });
  const body = await request<UserFundsFlowQueryResponse>(
    `/api/v1/currency/funds-flows?${params.toString()}`,
    { signal },
  );
  if (!body.success) {
    throw new ApiError(body.reason ?? 'FUNDS_FLOWS_REJECTED', 200, body);
  }
  return body.flows ?? [];
}

/**
 * 模拟登录 / 充值：amount 是展示金额（币种单位，整数），后端按币种精度换算成最小单位。
 */
export function rechargeAccount(
  userId: number,
  currency: string,
  amount: number,
  signal?: AbortSignal,
): Promise<RechargeResponse> {
  return request<RechargeResponse>('/api/v1/admin/recharges', {
    method: 'POST',
    body: JSON.stringify({ userId, currency, amount }),
    signal,
  });
}

/** 后台：查询资金流水（按发生时间倒序）。 */
export async function fetchFundsFlows(
  query: FundsFlowQuery = {},
  signal?: AbortSignal,
): Promise<FundsFlow[]> {
  const params = new URLSearchParams();
  if (query.userId != null) {
    params.set('userId', String(query.userId));
  }
  if (query.currency) {
    params.set('currency', query.currency);
  }
  if (query.logType) {
    params.set('logType', query.logType);
  }
  params.set('limit', String(query.limit ?? 100));

  const body = await request<FundsFlowQueryResponse>(
    `/api/v1/admin/funds-flows?${params.toString()}`,
    { signal },
  );
  if (!body.success) {
    throw new ApiError(body.reason ?? 'FUNDS_FLOWS_REJECTED', 200, body);
  }
  return body.flows ?? [];
}

/** 后台：一键清空全部资金流水，返回清空前的记录数。 */
export async function clearFundsFlows(signal?: AbortSignal): Promise<number> {
  const body = await request<FundsFlowClearResponse>('/api/v1/admin/funds-flows', {
    method: 'DELETE',
    signal,
  });
  if (!body.success) {
    throw new ApiError(body.reason ?? 'CLEAR_FAILED', 200, body);
  }
  return body.deletedCount ?? 0;
}

/** 后台：查询 K 线（按 bucket 起点倒序）。 */
export async function fetchAdminKlines(
  query: AdminKlineQuery = {},
  signal?: AbortSignal,
): Promise<Kline[]> {
  const params = new URLSearchParams();
  if (query.symbol) {
    params.set('symbol', query.symbol);
  }
  if (query.interval) {
    params.set('interval', query.interval);
  }
  params.set('limit', String(query.limit ?? 100));

  const body = await request<AdminKlineQueryResponse>(
    `/api/v1/admin/klines?${params.toString()}`,
    { signal },
  );
  if (!body.success) {
    throw new ApiError(body.reason ?? 'ADMIN_KLINES_REJECTED', 200, body);
  }
  return body.klines ?? [];
}

/** 后台：查询委托历史（撤单 / 撮合完成，按最终时间倒序）。 */
export async function fetchAdminOrderHistory(
  query: AdminOrderHistoryQuery = {},
  signal?: AbortSignal,
): Promise<OrderHistory[]> {
  const params = new URLSearchParams();
  if (query.userId != null) {
    params.set('userId', String(query.userId));
  }
  if (query.symbol) {
    params.set('symbol', query.symbol);
  }
  if (query.status) {
    params.set('status', query.status);
  }
  params.set('limit', String(query.limit ?? 100));

  const body = await request<AdminOrderHistoryQueryResponse>(
    `/api/v1/admin/order-history?${params.toString()}`,
    { signal },
  );
  if (!body.success) {
    throw new ApiError(body.reason ?? 'ORDER_HISTORY_REJECTED', 200, body);
  }
  return body.orders ?? [];
}

/** 后台：一键清空全部 K 线，返回清空前的记录数。 */
export async function clearAllKlines(signal?: AbortSignal): Promise<number> {
  const body = await request<AdminKlineClearResponse>('/api/v1/admin/klines', {
    method: 'DELETE',
    signal,
  });
  if (!body.success) {
    throw new ApiError(body.reason ?? 'CLEAR_FAILED', 200, body);
  }
  return body.deletedCount ?? 0;
}
