package com.me.shared.order;

/**
 * 委托状态。
 *
 * <p>委托通过校验并保存后为 {@link #ACCEPTED}；账户模块回执冻结成功后为 {@link #PAID}，
 * 冻结失败为 {@link #REJECTED}。其余状态预留给撮合、撤单与结算。
 */
public enum OrderStatus {

    /** 已受理：委托已保存，等待资金冻结回执 */
    ACCEPTED("已受理"),

    /** 已支付：资金冻结成功，已投递给撮合引擎 */
    PAID("已支付"),

    /**
     * 部分成交。既可能是进行中的状态（委托仍在订单簿中等待后续成交），也可能是终态：
     * 剩余部分已撤销并退款后（市价单 IOC 收尾、部分成交后撤单），状态保持该值，
     * 由 {@code finalAt} 区分——进行中时 {@code finalAt == null}，终态时非空。
     */
    PARTIALLY_FILLED("部分成交"),

    /**
     * 全部成交：{@code filledQuantity == quantity}。
     */
    FILLED("已成交"),

    /**
     * 已撤销且没有任何成交；撤单前有部分成交时委托以 {@link #PARTIALLY_FILLED} 终态结束。
     */
    CANCELED("已撤销"),

    /** 已拒绝 */
    REJECTED("已拒绝");

    private final String description;

    OrderStatus(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}
