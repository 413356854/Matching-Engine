package com.me.shared.event;

/**
 * 账户余额的六种原子操作类型。
 *
 * <p>业务日志类型（{@link FundsLogType}）负责把业务语义映射到这些原子操作；
 * 账户操作逻辑只依赖本枚举，不依赖具体日志类型。
 */
public enum FundsOp {

    /** 可用入账：可用余额增加 */
    AVAILABLE_CREDIT("可用入账"),

    /** 可用扣账：可用余额减少 */
    AVAILABLE_DEBIT("可用扣账"),

    /** 冻结入账：冻结余额增加 */
    FROZEN_CREDIT("冻结入账"),

    /** 冻结扣账：冻结余额减少 */
    FROZEN_DEBIT("冻结扣账"),

    /** 可用转冻结：可用余额减少、冻结余额增加 */
    AVAILABLE_TO_FROZEN("可用转冻结"),

    /** 冻结转可用：冻结余额减少、可用余额增加 */
    FROZEN_TO_AVAILABLE("冻结转可用");

    private final String description;

    FundsOp(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}
