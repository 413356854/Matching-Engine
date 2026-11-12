package com.me.account.vo;

/**
 * 账户余额视图：可用余额、冻结余额和账户变更序号。
 */
public record AccountView(long userId, String currency, long available, long frozen, long seq) {
}
