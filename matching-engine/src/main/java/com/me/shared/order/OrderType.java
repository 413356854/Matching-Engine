package com.me.shared.order;

/**
 * 委托类型。
 *
 * <p>限价单必须携带大于 0 的价格；市价单价格固定为 0，由撮合时按对手价成交。
 */
public enum OrderType {

    /** 限价单 */
    LIMIT("限价单"),

    /** 市价单 */
    MARKET("市价单");

    private final String description;

    OrderType(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}
