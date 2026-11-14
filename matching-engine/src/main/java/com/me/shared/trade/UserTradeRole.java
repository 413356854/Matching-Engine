package com.me.shared.trade;

/**
 * 用户成交记录中的角色：每笔撮合对拆成 taker / maker 两条记录。
 */
public enum UserTradeRole {

    /** 主动成交方。 */
    TAKER,

    /** 被动成交方。 */
    MAKER
}
