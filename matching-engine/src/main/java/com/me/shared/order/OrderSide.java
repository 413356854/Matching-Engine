package com.me.shared.order;

/**
 * 委托方向：买入或卖出。
 */
public enum OrderSide {

    /** 买入 */
    BUY("买入"),

    /** 卖出 */
    SELL("卖出");

    private final String description;

    OrderSide(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}
