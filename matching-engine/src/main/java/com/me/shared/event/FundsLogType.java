package com.me.shared.event;

/**
 * 资金日志类型：业务语义日志到原子余额操作的映射。
 *
 * <p>命名规则：<b>业务场景 + 余额方向</b>，避免 {@code FREEZE} / {@code UNFREEZE} 这类
 * 无法判断资金流向的歧义名称。
 *
 * <p>每个日志类型固定包含三部分：操作类型 {@link #op()}、日志类型编码 {@link #type()}、
 * 日志描述 {@link #description()}。
 *
 * <p><b>账户操作只读取 {@link #op()}，不按具体日志类型分支。</b>新增业务日志类型时只需在这里
 * 增加一行映射，账户操作逻辑保持不变。
 */
public enum FundsLogType {

    /** 充值：可用入账 */
    DEPOSIT(FundsOp.AVAILABLE_CREDIT, "DEPOSIT", "充值（可用入账）"),

    /** 提现 / 直接扣账：可用扣账 */
    WITHDRAW(FundsOp.AVAILABLE_DEBIT, "WITHDRAW", "提现/扣账（可用扣账）"),

    /** 下单冻结：可用转冻结 */
    ORDER_FREEZE(FundsOp.AVAILABLE_TO_FROZEN, "ORDER_FREEZE", "下单冻结（可用转冻结）"),

    /** 撤单 / 拒单解冻：冻结转可用 */
    ORDER_UNFREEZE(FundsOp.FROZEN_TO_AVAILABLE, "ORDER_UNFREEZE", "撤单解冻（冻结转可用）"),

    /** 成交收入：可用入账 */
    TRADE_SETTLE_IN(FundsOp.AVAILABLE_CREDIT, "TRADE_SETTLE_IN", "成交收入（可用入账）"),

    /** 成交支出：冻结扣账 */
    TRADE_SETTLE_OUT(FundsOp.FROZEN_DEBIT, "TRADE_SETTLE_OUT", "成交支出（冻结扣账）"),

    /** 手续费：可用扣账 */
    FEE(FundsOp.AVAILABLE_DEBIT, "FEE", "手续费（可用扣账）"),

    /** 冻结调增：冻结入账 */
    FROZEN_ADJUST_IN(FundsOp.FROZEN_CREDIT, "FROZEN_ADJUST_IN", "冻结调增（冻结入账）"),

    /** 冻结调减：冻结扣账 */
    FROZEN_ADJUST_OUT(FundsOp.FROZEN_DEBIT, "FROZEN_ADJUST_OUT", "冻结调减（冻结扣账）");

    private final FundsOp op;
    private final String type;
    private final String description;

    FundsLogType(FundsOp op, String type, String description) {
        this.op = op;
        this.type = type;
        this.description = description;
    }

    /** 该日志类型对应的原子余额操作。 */
    public FundsOp op() {
        return op;
    }

    /** 日志类型编码，用于消息、存储和查询。 */
    public String type() {
        return type;
    }

    /** 日志描述，用于审计展示。 */
    public String description() {
        return description;
    }
}
