import type { OrderStatus, OrderType } from './api';

interface OrderStatusInfo {
  type: OrderType;
  status: OrderStatus;
  filledQuantity: number;
}

/**
 * 市价单按 IOC 处理：未全部成交时以 PARTIALLY_FILLED 终态展示为「部分成交」；
 * 完全没有成交时终态为 CANCELED，展示为「未成交」而不是「已撤销」。
 * 同时兼容旧数据（CANCELED 且有成交量）按「部分成交」展示。
 * 返回 null 表示不是该场景，调用方继续使用常规状态文案。
 */
export function marketIocStatusLabel(order: OrderStatusInfo): string | null {
  if (order.type !== 'MARKET' || order.status !== 'CANCELED') {
    return null;
  }
  return order.filledQuantity > 0 ? '部分成交' : '未成交';
}

/** 与 {@link marketIocStatusLabel} 配套的状态配色。返回 null 表示用常规配色。 */
export function marketIocStatusClass(order: OrderStatusInfo): string | null {
  if (order.type !== 'MARKET' || order.status !== 'CANCELED') {
    return null;
  }
  return order.filledQuantity > 0 ? 'partial' : 'flat';
}
