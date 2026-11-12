package com.me.account.service.impl;

import com.me.account.dto.AccountApplyResult;
import com.me.account.dto.FundsCommand;
import com.me.account.dto.TradeSettleResult;
import com.me.account.dto.TradeSettlementCommand;
import com.me.account.service.AccountService;
import com.me.account.sharding.AccountSharding;
import com.me.account.vo.AccountView;
import com.me.shared.event.FundsFlowEvent;
import com.me.shared.event.FundsLogType;
import com.me.shared.event.FundsOp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.springframework.stereotype.Service;

/**
 * 内存账户实现。
 *
 * <p>余额按 (userId, currency) 保存在 {@link ConcurrentHashMap} 中，每个账户包含
 * {@code available} / {@code frozen} / {@code seq}，不落库、不落盘；进程重启后余额清零。
 *
 * <p>账户操作只根据 {@link FundsLogType#op()} 得到的 {@link FundsOp} 执行，
 * 不按具体日志类型分支；新增业务日志类型无需修改本类。
 *
 * <p><b>演示版重投语义</b>：不做 requestId 级别的防重缓存。Kafka 事务回滚后的重投会再次
 * 执行命令；同一充值/冻结可能二次入账，同一结算可能因余额已变化而被拒绝或重复扣减。
 * 这是演示版的取舍，后续补持久化备份与内存状态恢复后再实现幂等/重放。
 */
@Service
public class AccountServiceImpl implements AccountService {

    /**
     * 按 userId slot 分片的账户写锁，共 {@link AccountSharding#SLOTS} 把。
     *
     * <p>apply 只锁 {@code slot(userId)}；settle 拆成单用户命令后同样只锁一个 slot，
     * 防御性地支持一条命令涉及多个 slot（按升序加锁、降序解锁）。同一用户的冻结、
     * 清算、充值都使用同一个 slot，保证不会并发修改同一账户。
     */
    private final ReentrantLock[] accountLocks = newLocks();

    private final ConcurrentHashMap<AccountKey, Balance> balances = new ConcurrentHashMap<>();

    /** userId → 已开立账户的币种集合，用于按用户查询余额时避免全量扫描。 */
    private final ConcurrentHashMap<Long, Set<String>> currenciesByUser = new ConcurrentHashMap<>();

    @Override
    public AccountApplyResult apply(FundsCommand command) {
        String invalidReason = validate(command);
        if (invalidReason != null) {
            return AccountApplyResult.rejected(invalidReason);
        }

        AccountKey key = new AccountKey(command.userId(), command.currency());
        ReentrantLock lock = accountLocks[AccountSharding.slot(command.userId())];
        lock.lock();
        try {
            return applyLocked(command, key);
        } finally {
            lock.unlock();
        }
    }

    private AccountApplyResult applyLocked(FundsCommand command, AccountKey key) {
        FundsLogType logType = command.logType();
        FundsOp op = logType.op();
        AtomicReference<Balance> beforeRef = new AtomicReference<>();
        AtomicReference<Balance> afterRef = new AtomicReference<>();

        try {
            // compute 对同一 key 的读改写是原子的；不同 key 可以并行。
            balances.compute(key, (k, current) -> {
                Balance before = current == null ? Balance.ZERO : current;
                Balance after = applyOp(before, op, command.amount());
                beforeRef.set(before);
                afterRef.set(after);
                return after;
            });
        } catch (InsufficientAvailableBalanceException e) {
            return AccountApplyResult.rejected("INSUFFICIENT_AVAILABLE_BALANCE");
        } catch (InsufficientFrozenBalanceException e) {
            return AccountApplyResult.rejected("INSUFFICIENT_FROZEN_BALANCE");
        } catch (ArithmeticException e) {
            return AccountApplyResult.rejected("AMOUNT_OVERFLOW");
        }

        Balance before = beforeRef.get();
        Balance after = afterRef.get();
        currenciesByUser.computeIfAbsent(command.userId(), userId -> ConcurrentHashMap.newKeySet())
                .add(key.currency());

        FundsFlowEvent event = new FundsFlowEvent(
                // 一个账户命令只产生一条流水，requestId 本身就是稳定、唯一的 eventId；
                // 不再为每条流水生成 UUID（重投时也保持同一 id，便于 admin 端去重）。
                command.requestId(),
                command.requestId(),
                command.userId(),
                command.currency(),
                logType,
                command.amount(),
                before.available(),
                after.available(),
                before.frozen(),
                after.frozen(),
                after.seq(),
                Instant.now());
        return AccountApplyResult.success(event);
    }

    @Override
    public TradeSettleResult settle(TradeSettlementCommand command) {
        String invalidReason = validateSettlement(command);
        if (invalidReason != null) {
            return TradeSettleResult.rejected(invalidReason);
        }

        int[] slots = settlementSlots(command);
        lockSlots(slots);
        try {
            return settleLocked(command);
        } finally {
            unlockSlots(slots);
        }
    }

    private static ReentrantLock[] newLocks() {
        ReentrantLock[] locks = new ReentrantLock[AccountSharding.SLOTS];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new ReentrantLock();
        }
        return locks;
    }

    /**
     * 命令涉及的去重 slot，按升序排列。
     *
     * <p>拆分后的清算命令只含一个用户，通常返回 1 个 slot；保留多 slot 逻辑是为了
     * 对未拆分的命令仍然保持"升序加锁、降序解锁"的死锁约束。
     */
    private static int[] settlementSlots(TradeSettlementCommand command) {
        int[] slots = new int[command.legs().size()];
        int count = 0;
        for (TradeSettlementCommand.SettlementLeg leg : command.legs()) {
            int slot = AccountSharding.slot(leg.userId());
            int pos = 0;
            while (pos < count && slots[pos] < slot) {
                pos++;
            }
            if (pos < count && slots[pos] == slot) {
                continue;
            }
            System.arraycopy(slots, pos, slots, pos + 1, count - pos);
            slots[pos] = slot;
            count++;
        }
        return Arrays.copyOf(slots, count);
    }

    private void lockSlots(int[] slots) {
        for (int slot : slots) {
            accountLocks[slot].lock();
        }
    }

    private void unlockSlots(int[] slots) {
        for (int i = slots.length - 1; i >= 0; i--) {
            accountLocks[slots[i]].unlock();
        }
    }

    /**
     * 在涉及的账户锁内顺序执行一条结算消息的全部资金腿。
     *
     * <p>先在 {@code working} 副本上执行，全部成功后才写回余额，
     * 因此任一腿余额不足时整条结算消息不会产生任何余额变化。
     */
    private TradeSettleResult settleLocked(TradeSettlementCommand command) {
        Map<AccountKey, Balance> working = new LinkedHashMap<>();
        List<FundsFlowEvent> flowEvents = new ArrayList<>(command.legs().size());
        Instant occurredAt = Instant.now();

        int legIndex = 0;
        for (TradeSettlementCommand.SettlementLeg leg : command.legs()) {
            AccountKey key = new AccountKey(leg.userId(), leg.currency());
            Balance before = working.get(key);
            if (before == null) {
                before = balances.getOrDefault(key, Balance.ZERO);
            }

            Balance after;
            try {
                after = applyOp(before, leg.logType().op(), leg.amount());
            } catch (InsufficientAvailableBalanceException e) {
                return TradeSettleResult.rejected("INSUFFICIENT_AVAILABLE_BALANCE");
            } catch (InsufficientFrozenBalanceException e) {
                return TradeSettleResult.rejected("INSUFFICIENT_FROZEN_BALANCE");
            } catch (ArithmeticException e) {
                return TradeSettleResult.rejected("AMOUNT_OVERFLOW");
            }

            working.put(key, after);
            flowEvents.add(new FundsFlowEvent(
                    // 结算 requestId 唯一，腿序号稳定；拼出的 eventId 在重投时不变，
                    // 无需为每条腿生成 UUID。
                    command.requestId() + ":" + legIndex,
                    command.requestId(),
                    leg.userId(),
                    leg.currency(),
                    leg.logType(),
                    leg.amount(),
                    before.available(),
                    after.available(),
                    before.frozen(),
                    after.frozen(),
                    after.seq(),
                    occurredAt));
            legIndex++;
        }

        working.forEach(balances::put);

        for (AccountKey key : working.keySet()) {
            currenciesByUser.computeIfAbsent(key.userId(), userId -> ConcurrentHashMap.newKeySet())
                    .add(key.currency());
        }

        return TradeSettleResult.success(flowEvents);
    }

    private String validateSettlement(TradeSettlementCommand command) {
        if (command == null) {
            return "COMMAND_IS_NULL";
        }
        if (command.requestId() == null || command.requestId().isBlank()) {
            return "REQUEST_ID_REQUIRED";
        }
        // 市价买入预算退款只关联一笔委托：tradeId / makerOrderId 为 0 表示非成交清算。
        if (command.tradeId() < 0) {
            return "TRADE_ID_INVALID";
        }
        if (command.symbol() == null || command.symbol().isBlank()) {
            return "SYMBOL_REQUIRED";
        }
        if (command.takerOrderId() <= 0 || command.makerOrderId() < 0) {
            return "ORDER_ID_INVALID";
        }
        if (command.legs().isEmpty()) {
            return "LEGS_REQUIRED";
        }
        for (TradeSettlementCommand.SettlementLeg leg : command.legs()) {
            if (leg == null) {
                return "LEG_IS_NULL";
            }
            if (leg.userId() <= 0) {
                return "USER_ID_INVALID";
            }
            if (leg.currency() == null || leg.currency().isBlank()) {
                return "CURRENCY_REQUIRED";
            }
            if (leg.logType() == null) {
                return "LOG_TYPE_REQUIRED";
            }
            if (leg.amount() <= 0) {
                return "AMOUNT_INVALID";
            }
        }
        return null;
    }

    @Override
    public AccountView view(long userId, String currency) {
        String normalized = normalizeCurrency(currency);
        Balance balance = normalized == null ? null : balances.get(new AccountKey(userId, normalized));
        if (balance == null) {
            return new AccountView(userId, normalized, 0L, 0L, 0L);
        }
        return new AccountView(userId, normalized, balance.available(), balance.frozen(), balance.seq());
    }

    @Override
    public List<AccountView> listBalances(long userId) {
        Set<String> currencies = currenciesByUser.get(userId);
        if (currencies == null) {
            return List.of();
        }
        return currencies.stream()
                .map(currency -> view(userId, currency))
                .sorted(Comparator.comparing(AccountView::currency))
                .toList();
    }

    /**
     * 按原子操作类型修改余额；余额不足时抛出异常，compute 不会写入变更。
     */
    private Balance applyOp(Balance before, FundsOp op, long amount) {
        long available = before.available();
        long frozen = before.frozen();

        switch (op) {
            case AVAILABLE_CREDIT -> available = Math.addExact(available, amount);
            case AVAILABLE_DEBIT -> {
                requireAvailable(available, amount);
                available = Math.subtractExact(available, amount);
            }
            case FROZEN_CREDIT -> frozen = Math.addExact(frozen, amount);
            case FROZEN_DEBIT -> {
                requireFrozen(frozen, amount);
                frozen = Math.subtractExact(frozen, amount);
            }
            case AVAILABLE_TO_FROZEN -> {
                requireAvailable(available, amount);
                available = Math.subtractExact(available, amount);
                frozen = Math.addExact(frozen, amount);
            }
            case FROZEN_TO_AVAILABLE -> {
                requireFrozen(frozen, amount);
                frozen = Math.subtractExact(frozen, amount);
                available = Math.addExact(available, amount);
            }
            default -> throw new IllegalArgumentException("unsupported funds op: " + op);
        }
        return new Balance(available, frozen, before.seq() + 1);
    }

    private void requireAvailable(long available, long amount) {
        if (available < amount) {
            throw new InsufficientAvailableBalanceException();
        }
    }

    private void requireFrozen(long frozen, long amount) {
        if (frozen < amount) {
            throw new InsufficientFrozenBalanceException();
        }
    }

    private String validate(FundsCommand command) {
        if (command == null) {
            return "COMMAND_IS_NULL";
        }
        if (command.requestId() == null || command.requestId().isBlank()) {
            return "REQUEST_ID_REQUIRED";
        }
        if (command.userId() <= 0) {
            return "USER_ID_INVALID";
        }
        if (command.currency() == null || command.currency().isBlank()) {
            return "CURRENCY_REQUIRED";
        }
        if (command.logType() == null) {
            return "LOG_TYPE_REQUIRED";
        }
        if (command.amount() <= 0) {
            return "AMOUNT_INVALID";
        }
        return null;
    }

    private String normalizeCurrency(String currency) {
        return currency == null ? null : currency.strip().toUpperCase(Locale.ROOT);
    }

    private record AccountKey(long userId, String currency) {
    }

    private record Balance(long available, long frozen, long seq) {
        private static final Balance ZERO = new Balance(0L, 0L, 0L);
    }

    private static final class InsufficientAvailableBalanceException extends RuntimeException {
        private InsufficientAvailableBalanceException() {
            super(null, null, false, false);
        }
    }

    private static final class InsufficientFrozenBalanceException extends RuntimeException {
        private InsufficientFrozenBalanceException() {
            super(null, null, false, false);
        }
    }
}
