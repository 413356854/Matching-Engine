package com.me.snapshot;

import com.me.account.vo.AccountView;
import com.me.order.dto.OrderSnapshot;
import com.me.order.dto.OrderStateSnapshot;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderStatus;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 快照一致性校验：每 {@code (userId, currency)} 的「未成交委托剩余冻结」必须等于
 * 账户冻结余额，且不存在仍在等待冻结回执的 {@code ACCEPTED} 委托。
 *
 * <p>只有真正静止的切点才能满足：ACCEPTED 委托说明内部 topic 还没排空；剩余冻结为负
 * 或两边不等，说明内存状态或 offset 切点有问题。快照写入前和快照加载时都复用本校验。
 */
public final class SnapshotInvariantValidator {

    private SnapshotInvariantValidator() {
    }

    /**
     * @throws IllegalStateException 校验失败，message 中带具体订单 / 用户
     */
    public static void validate(List<AccountView> accounts, OrderStateSnapshot orderState) {
        if (accounts == null) {
            throw new IllegalStateException("snapshot invariant violated: accounts is null");
        }
        if (orderState == null || orderState.orders() == null) {
            throw new IllegalStateException("snapshot invariant violated: order state is null");
        }

        Map<FrozenKey, Long> accountFrozen = new HashMap<>();
        for (AccountView account : accounts) {
            accountFrozen.merge(
                    new FrozenKey(account.userId(), normalizeCurrency(account.currency())),
                    account.frozen(), Long::sum);
        }

        Map<FrozenKey, Long> orderFrozen = new HashMap<>();
        int acceptedOrders = 0;
        for (OrderSnapshot order : orderState.orders()) {
            if (order.status() == OrderStatus.ACCEPTED) {
                acceptedOrders++;
                continue;
            }
            if (order.status() != OrderStatus.PAID
                    && order.status() != OrderStatus.PARTIALLY_FILLED) {
                continue;
            }
            long remaining = order.side() == OrderSide.BUY
                    ? order.frozenAmount() - order.settledQuoteAmount()
                    : order.frozenAmount() - order.filledQuantity();
            if (remaining < 0) {
                throw new IllegalStateException("snapshot invariant violated: order "
                        + order.orderId() + " has negative remaining frozen amount "
                        + remaining);
            }
            orderFrozen.merge(
                    new FrozenKey(order.userId(), normalizeCurrency(order.frozenCurrency())),
                    remaining, Long::sum);
        }

        if (acceptedOrders > 0) {
            throw new IllegalStateException("snapshot is not quiescent: "
                    + acceptedOrders + " order(s) still in ACCEPTED state");
        }

        Set<FrozenKey> keys = new HashSet<>(accountFrozen.keySet());
        keys.addAll(orderFrozen.keySet());
        for (FrozenKey key : keys) {
            long fromAccounts = accountFrozen.getOrDefault(key, 0L);
            long fromOrders = orderFrozen.getOrDefault(key, 0L);
            if (fromAccounts != fromOrders) {
                throw new IllegalStateException("snapshot invariant violated for user "
                        + key.userId() + " " + key.currency() + ": accountFrozen="
                        + fromAccounts + ", orderFrozen=" + fromOrders);
            }
        }
    }

    private static String normalizeCurrency(String currency) {
        return currency == null ? null : currency.strip().toUpperCase(Locale.ROOT);
    }

    private record FrozenKey(long userId, String currency) {
    }
}
