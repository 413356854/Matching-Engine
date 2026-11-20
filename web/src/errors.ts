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
  QUANTITY_STEP_INVALID: '数量必须是最小数量步长的整数倍',
  AMOUNT_NOT_REPRESENTABLE: '当前价格 × 数量无法精确换算为计价币最小单位',
  PRICE_SCALE_EXCEEDS_RAW_SCALE: '价格精度不能高于计价币 raw_scale，否则 amount 无法换算回原始整数',
  AMOUNT_OVERFLOW: '金额超出可表示范围',
  AMOUNT_REQUIRED_FOR_MARKET_BUY: '市价买入缺少冻结金额，请刷新行情后重试',
  AMOUNT_NOT_ALLOWED: '该委托类型不允许携带金额',
  MARKET_PRICE_UNAVAILABLE: '暂无卖一价或最新成交价，无法计算市价买入金额',
  INSUFFICIENT_AVAILABLE_BALANCE: '可用余额不足',
  ORDER_COMMAND_PUBLISH_FAILED: '下单指令投递失败，可使用同一请求重试',
  ORDER_REJECTED: '下单被拒绝',
  CANCEL_COMMAND_PUBLISH_FAILED: '撤单指令投递失败，请稍后重试',
  CANCEL_REJECTED: '撤单被拒绝',
  ORDER_ID_REQUIRED: '缺少委托号，无法撤单',
  REQUEST_ID_REQUIRED: '缺少请求标识',
  REQUEST_ID_INVALID: '请求标识无效',
  ORDER_NOT_FOUND: '委托不存在',
  ORDER_NOT_PAID_YET: '委托尚未完成资金冻结，请稍后重试',
  ORDER_ALREADY_FILLED: '委托已全部成交，无法撤单',
  ORDER_ALREADY_COMPLETED: '委托已结束（部分成交剩余已撤销），无法撤单',
  ORDER_ALREADY_CANCELED: '委托已撤销',
  SYMBOL_MISMATCH: '交易对与委托不匹配',
  USER_MISMATCH: '无权撤销该委托',

  INTERVAL_INVALID: 'K 线周期无效',
  LIMIT_INVALID: '查询条数无效',
  KLINES_REJECTED: 'K 线查询失败',
  TRADES_REJECTED: '成交查询失败',
  ORDER_BOOK_REJECTED: '订单簿查询失败',
  ORDER_BOOK_STREAM_ERROR: '盘口推送异常',
  MESSAGE_INVALID: '消息格式无效',
  ACTION_INVALID: '不支持的操作',
  MESSAGE_TOO_LARGE: '交易消息过大',
  UNSUPPORTED_VERSION: '交易协议版本不兼容',
  ACTION_REQUIRED: '缺少操作类型',
  UNAUTHORIZED: '请先登录后再交易',
  ALREADY_AUTHENTICATED: '该连接已登录其他用户',
  CONNECTION_NOT_REGISTERED: '交易连接未注册，请刷新页面',
  RATE_LIMITED: '提交过于频繁，请稍后重试',
  SERVER_BUSY: '交易通道繁忙，请稍后重试',
  TRANSPORT_UNAVAILABLE: '交易通道未连接，请稍后重试',
  ENGINE_UNAVAILABLE: '撮合引擎暂不可用，请稍后重试',
  RECOVERY_IN_PROGRESS: '撮合引擎正在恢复，请稍后重试',
  INTERNAL_ERROR: '交易通道内部错误',
  ACK_TIMEOUT: '交易通道回执超时，请查询委托确认结果',
  REQUEST_ID_IN_PROGRESS: '相同 requestId 的请求仍在等待回执',
  TRADE_SOCKET_NOT_READY: '交易通道未就绪，请稍后重试',
  TRADE_SOCKET_CLOSED: '交易通道已断开，请查询委托后重试',
  TRADE_SOCKET_SEND_FAILED: '交易通道发送失败，请查询委托后重试',
  DEPTH_INVALID: '订单簿深度档位无效',
  ORDER_LIST_REJECTED: '当前委托查询失败',
  BALANCES_REJECTED: '余额查询失败',
  PAIRS_UNAVAILABLE: '交易对列表不可用',
  CURRENCIES_UNAVAILABLE: '币种精度不可用',

  SYMBOL_INVALID: '交易对格式无效',
  SYMBOL_TOO_LONG: '交易对代码过长（最多 32 个字符）',
  SYMBOL_ALREADY_EXISTS: '交易对已存在',
  BASE_CURRENCY_INVALID: '基础币种格式无效',
  QUOTE_CURRENCY_INVALID: '计价币种格式无效',
  CURRENCY_PAIR_INVALID: '基础币种和计价币种不能相同',
  BASE_CURRENCY_NOT_FOUND: '基础币种不存在或未启用',
  QUOTE_CURRENCY_NOT_FOUND: '计价币种不存在或未启用',
  PRICE_SCALE_INVALID: '价格精度必须在 0~18 之间',
  QUANTITY_SCALE_INVALID: '数量精度必须在 0~18 之间',
  QUANTITY_SCALE_MISMATCH: '数量精度必须等于基础币种精度',
  MIN_ORDER_QUANTITY_INVALID: '最小下单量不能为负',
  MIN_ORDER_AMOUNT_INVALID: '最小下单金额不能为负',
  MAKER_FEE_INVALID: 'maker 费率必须在 -10000~10000 bps 之间',
  TAKER_FEE_INVALID: 'taker 费率必须在 -10000~10000 bps 之间',
  STATUS_INVALID: '交易对状态无效',
  PAIR_NOT_FOUND: '交易对不存在',
  PAIR_ONLINE: '交易对仍在线，请先下线再删除',
  PAIR_HAS_OPEN_ORDERS: '订单簿仍有挂单，请先撤销全部委托再删除',
  SYMBOL_BOOK_NOT_EMPTY: '该交易对仍有挂单，无法重建',
  SCALE_IMMUTABLE: '价格 / 数量精度创建后不可修改',
  ID_INVALID: 'ID 无效',
  WRITE_FAILED: '交易对写入失败，请确认 MySQL 已启动后重试',
  LOG_TYPE_INVALID: '日志类型无效',
  QUERY_FAILED: '查询失败，请确认数据库已启动后重试',
  CLEAR_FAILED: '清空失败，请确认 QuestDB 已启动后重试',
  FUNDS_FLOWS_REJECTED: '资金流水查询失败',
  ADMIN_KLINES_REJECTED: 'K 线查询失败',

  BOT_DISABLED: '机器人模块未启用（app.bot.enabled=false）',
  BINANCE_UNREACHABLE: '开启需要配置科学联网币安（币安测试接口不可达）',
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
