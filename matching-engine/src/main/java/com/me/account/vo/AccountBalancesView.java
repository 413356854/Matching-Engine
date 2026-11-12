package com.me.account.vo;

import java.util.List;

/**
 * REST 余额查询结果。
 *
 * <p>成功时 {@code balances} 是该用户全部币种的余额快照，按币种升序；
 * 用户没有任何账户时为空列表。失败时 {@code reason} 是错误码。
 */
public record AccountBalancesView(
        boolean success,
        String reason,
        long userId,
        List<AccountView> balances) {

    public AccountBalancesView {
        balances = balances == null ? List.of() : List.copyOf(balances);
    }

    public static AccountBalancesView success(long userId, List<AccountView> balances) {
        return new AccountBalancesView(true, null, userId, balances);
    }

    public static AccountBalancesView rejected(long userId, String reason) {
        return new AccountBalancesView(false, reason, userId, List.of());
    }
}
