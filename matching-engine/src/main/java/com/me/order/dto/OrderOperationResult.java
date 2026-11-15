package com.me.order.dto;

import com.me.account.dto.TradeSettlementCommand;
import com.me.shared.event.MatchPairEvent;
import com.me.shared.order.OrderHistoryEvent;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 一个委托操作批次的执行结果。
 *
 * <p>{@code orderSeq} 是 {@link com.me.order.service.OrderOperationService} 在全局
 * {@code orderLock} 内为本批次分配的全局递增序号；{@code offsets} 是本批每条输入消费
 * 记录与该序号的对应关系，由 listener 在同一个 Kafka 事务内发往
 * {@code me.order.offset-seq-batch}。
 *
 * <p>不同链路的输出分别落在对应字段：
 * <ul>
 *   <li>下单命令：{@code freezeCommands}（发往账户冻结 topic）；</li>
 *   <li>冻结回执：{@code matchPairsBySymbol} / {@code settlementCommands} /
 *       {@code historyEvents}；</li>
 *   <li>撤单：{@code settlementCommands} / {@code historyEvents}。</li>
 * </ul>
 */
public record OrderOperationResult(
        long orderSeq,
        List<OrderOffsetSeq> offsets,
        List<FreezeCommand> freezeCommands,
        Map<String, List<MatchPairEvent>> matchPairsBySymbol,
        List<TradeSettlementCommand> settlementCommands,
        List<OrderHistoryEvent> historyEvents) {

    public OrderOperationResult {
        offsets = offsets == null ? List.of() : List.copyOf(offsets);
        freezeCommands = freezeCommands == null ? List.of() : List.copyOf(freezeCommands);
        matchPairsBySymbol = matchPairsBySymbol == null || matchPairsBySymbol.isEmpty()
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(matchPairsBySymbol));
        settlementCommands = settlementCommands == null
                ? List.of() : List.copyOf(settlementCommands);
        historyEvents = historyEvents == null ? List.of() : List.copyOf(historyEvents);
    }

    /** 没有可执行命令或没有产生输出时的空结果。 */
    public static OrderOperationResult empty(long orderSeq) {
        return new OrderOperationResult(
                orderSeq, List.of(), List.of(), Map.of(), List.of(), List.of());
    }
}
