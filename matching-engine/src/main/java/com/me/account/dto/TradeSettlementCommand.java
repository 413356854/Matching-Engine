package com.me.account.dto;

import com.me.shared.event.FundsLogType;
import java.util.List;
import java.util.Locale;

/**
 * 双边成交结算命令，作为 {@link TradeSettlementBatch#getMsgList()} 的元素发往
 * Kafka topic {@code me.account.settlements-batch}。
 *
 * <p>一个成交对（taker / maker）拆成买方、卖方<b>两条命令</b>，每条只携带一个用户的资金腿：
 * <ul>
 *   <li>买方：付计价币（{@link FundsLogType#TRADE_SETTLE_OUT}，冻结扣账）、
 *       收基础币（{@link FundsLogType#TRADE_SETTLE_IN}，可用入账）；</li>
 *   <li>卖方：付基础币（冻结扣账）、收计价币（可用入账）；</li>
 *   <li>买单全部成交且成交价优于限价时，买方命令附带一条剩余冻结释放腿（{@link FundsLogType#ORDER_UNFREEZE}）。</li>
 * </ul>
 *
 * <p>账户模块把每条命令的<b>所有腿作为一个原子操作</b>执行：任一腿余额不足，该条命令不生效。
 * 买卖两侧是两条独立命令，不保证跨用户原子性。
 *
 * <p><b>非成交清算</b>：市价买入撮合结束后退回未用完的冻结预算，只携带一条
 * {@link FundsLogType#ORDER_UNFREEZE} 腿；此时 {@code tradeId} 与 {@code makerOrderId} 为 0，
 * {@code takerOrderId} 仍是产生退款的市价买入委托。
 *
 * <p>{@code requestId} 是结算命令的唯一标识（演示版由委托模块的进程内计数器生成，
 * 后续随内存状态持久化/恢复）；仅用于日志、审计与资金流水，不再作为账户幂等键。
 * 币种统一转为大写。
 */
public record TradeSettlementCommand(
        String requestId,
        long tradeId,
        String symbol,
        long takerOrderId,
        long makerOrderId,
        List<SettlementLeg> legs) {

    public TradeSettlementCommand {
        if (symbol != null) {
            symbol = symbol.strip().toUpperCase(Locale.ROOT);
        }
        legs = legs == null ? List.of() : List.copyOf(legs);
    }

    /**
     * 一条资金腿：对 {@code userId} 的 {@code currency} 余额执行 {@code logType} 对应的原子操作。
     */
    public record SettlementLeg(long userId, String currency, FundsLogType logType, long amount) {

        public SettlementLeg {
            if (currency != null) {
                currency = currency.strip().toUpperCase(Locale.ROOT);
            }
        }
    }
}
