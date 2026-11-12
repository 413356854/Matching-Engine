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

    /** 部分成交 */
    PARTIALLY_FILLED("部分成交"),

    /**
     * 全部成交；金额驱动的市价买入表示撮合完成。
     * 后者预算耗尽时已成交数量可能小于委托数量，未成交部分不再挂单。
     */
    FILLED("已成交"),

    /** 已撤销 */
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
