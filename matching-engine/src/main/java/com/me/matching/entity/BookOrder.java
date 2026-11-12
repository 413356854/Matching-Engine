package com.me.matching.entity;

import com.me.shared.order.OrderPlacedEvent;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderType;
import java.time.Instant;

/**
 * 订单簿中的挂单。
 *
 * <p>订单进入订单簿后，价格、方向、类型等不变，只有剩余数量随成交递减。
 * 限价单 {@code price > 0}，市价单 {@code price = 0}。
 *
 * <p>纯 JDK，无框架依赖。
 */
public final class BookOrder {

    private final long orderId;
    private final long userId;
    private final String symbol;
    private final OrderSide side;
    private final OrderType type;
    private final long price;
    private final long quantity;
    private final Instant placedAt;

    private long remainingQuantity;

    public BookOrder(OrderPlacedEvent order, long remainingQuantity) {
        if (order == null) {
            throw new IllegalArgumentException("order must not be null");
        }
        if (remainingQuantity <= 0 || remainingQuantity > order.quantity()) {
            throw new IllegalArgumentException(
                    "remainingQuantity out of range: " + remainingQuantity + ", quantity=" + order.quantity());
        }
        this.orderId = order.orderId();
        this.userId = order.userId();
        this.symbol = order.symbol();
        this.side = order.side();
        this.type = order.type();
        this.price = order.price();
        this.quantity = order.quantity();
        this.placedAt = order.placedAt();
        this.remainingQuantity = remainingQuantity;
    }

    public long orderId() {
        return orderId;
    }

    public long userId() {
        return userId;
    }

    public String symbol() {
        return symbol;
    }

    public OrderSide side() {
        return side;
    }

    public OrderType type() {
        return type;
    }

    public long price() {
        return price;
    }

    public long quantity() {
        return quantity;
    }

    public Instant placedAt() {
        return placedAt;
    }

    public long remainingQuantity() {
        return remainingQuantity;
    }

    /**
     * 扣减成交量。
     *
     * @return 扣减后的剩余数量
     */
    public long reduce(long quantity) {
        if (quantity <= 0 || quantity > remainingQuantity) {
            throw new IllegalArgumentException(
                    "reduce quantity out of range: " + quantity + ", remaining=" + remainingQuantity);
        }
        remainingQuantity -= quantity;
        return remainingQuantity;
    }

    public boolean filled() {
        return remainingQuantity == 0;
    }
}
