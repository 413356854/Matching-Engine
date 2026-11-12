import { ApiError, isAbortError } from './api';
import type { OrderStatus } from './api';

const REASON_TEXT: Record<string, string> = {
  NETWORK_ERROR: '无法连接后端服务，请确认 Spring Boot 已在 8080 端口启动',
  HTTP_400: '请求参数有误',
  HTTP_401: '未授权',
  HTTP_403: '没有权限',
  HTTP_404: '接口不存在',
  HTTP_500: '服务端内部错误',
  HTTP_502: '网关错误',
  HTTP_503: '服务暂不可用',

  REQUEST_REQUIRED: '请求不能为空',
  USER_ID_REQUIRED: '请填写用户 ID',
  USER_ID_INVALID: '用户 ID 必须是正整数',
  SYMBOL_REQUIRED: '请选择交易对',
  SYMBOL_NOT_FOUND: '交易对不存在',
  SYMBOL_NOT_TRADING: '交易对已下线，无法下单',
  CURRENCY_REQUIRED: '请选择币种',
  CURRENCY_INVALID: '币种代码无效',
  CURRENCY_NOT_FOUND: '币种不存在或未启用',
  AMOUNT_INVALID: '充值金额必须大于 0',
  RECHARGE_PUBLISH_FAILED: '充值指令投递失败，请稍后重试',
  SIDE_REQUIRED: '请选择买入或卖出',
  TYPE_REQUIRED: '请选择委托类型',
  QUANTITY_REQUIRED: '请填写数量',
  QUANTITY_INVALID: '数量必须大于 0',
  PRICE_REQUIRED_FOR_LIMIT: '限价单必须填写价格',
  PRICE_NOT_ALLOWED_FOR_MARKET: '市价单不能填写价格',
  ORDER_ID_INVALID: '委托号无效',
  ORDER_ID_ALREADY_EXISTS: '委托号已存在',
  BELOW_MIN_ORDER_QUANTITY: '低于交易对的最小下单量',
  BELOW_MIN_ORDER_AMOUNT: '低于交易对的最小下单金额',
  AMOUNT_OVERFLOW: '金额超出可表示范围',
  AMOUNT_REQUIRED_FOR_MARKET_BUY: '市价买入缺少冻结金额，请刷新行情后重试',
  AMOUNT_NOT_ALLOWED: '该委托类型不允许携带金额',
  MARKET_PRICE_UNAVAILABLE: '暂无卖一价或最新成交价，无法计算市价买入金额',
  INSUFFICIENT_AVAILABLE_BALANCE: '可用余额不足',
  ORDER_COMMAND_PUBLISH_FAILED: '下单指令投递失败，可使用同一请求重试',
  ORDER_REJECTED: '下单被拒绝',

  INTERVAL_INVALID: 'K 线周期无效',
  LIMIT_INVALID: '查询条数无效',
  KLINES_REJECTED: 'K 线查询失败',
  TRADES_REJECTED: '成交查询失败',
  ORDER_BOOK_REJECTED: '订单簿查询失败',
  DEPTH_INVALID: '订单簿深度档位无效',
  ORDER_LIST_REJECTED: '当前委托查询失败',
  BALANCES_REJECTED: '余额查询失败',
  PAIRS_UNAVAILABLE: '交易对列表不可用',
  CURRENCIES_UNAVAILABLE: '币种精度不可用',

  SYMBOL_INVALID: '交易对格式无效',
  LOG_TYPE_INVALID: '日志类型无效',
  QUERY_FAILED: 'QuestDB 查询失败，请确认数据库已启动后重试',
  CLEAR_FAILED: '清空失败，请确认 QuestDB 已启动后重试',
  FUNDS_FLOWS_REJECTED: '资金流水查询失败',
  ADMIN_KLINES_REJECTED: 'K 线查询失败',
};

export function describeReason(reason: string): string {
  if (REASON_TEXT[reason]) {
    return REASON_TEXT[reason];
  }
  if (reason.startsWith('HTTP_')) {
    return `请求失败（HTTP ${reason.slice(5)}）`;
  }
  return `操作失败：${reason}`;
}

export function describeError(error: unknown): string {
  if (isAbortError(error)) {
    return '';
  }
  if (error instanceof ApiError) {
    return describeReason(error.reason);
  }
  if (error instanceof Error && error.message) {
    return error.message;
  }
  return '未知错误';
}

export function describeOrderStatus(status: OrderStatus): string {
  switch (status) {
    case 'ACCEPTED':
      return '已受理（等待资金冻结）';
    case 'PAID':
      return '已支付（已进入撮合）';
    case 'PARTIALLY_FILLED':
      return '部分成交';
    case 'FILLED':
      return '全部成交';
    case 'CANCELED':
      return '已撤销';
    case 'REJECTED':
      return '已拒绝';
    default:
      return status;
  }
}
