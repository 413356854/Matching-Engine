package com.me.matching.vo;

/**
 * 订单簿的一个价格档位：同价挂单的数量聚合。
 *
 * <p>{@code price} / {@code quantity} 都是交易对最小单位的整数；
 * {@code orderCount} 是该价位仍挂在簿中的订单数。
 */
public record DepthLevel(long price, long quantity, int orderCount) {
}
