package com.me.matching.vo;

import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderType;

/**
 * 订单簿中一笔挂单的快照。
 *
 * <p>{@code placedAt} 用 ISO-8601 字符串保存，避免序列化依赖日期模块。
 */
public record BookOrderSnapshot(
        long orderId,
        long userId,
        String symbol,
        OrderSide side,
        OrderType type,
        long price,
        long quantity,
        long remainingQuantity,
        String placedAt) {
}
