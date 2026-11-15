package com.me.order.service.impl;

import com.me.account.dto.FundsCommand;
import com.me.account.dto.TradeSettlementCommand;
import com.me.currency.vo.CurrencyView;
import com.me.currency.vo.TradingPairView;
import com.me.kafka.KafkaSink;
import com.me.matching.dto.CancelResult;
import com.me.matching.dto.MatchResult;
import com.me.matching.service.MatchingEngine;
import com.me.order.dto.FreezeCommand;
import com.me.order.dto.OrderApplyResult;
import com.me.order.dto.OrderCancelApplyResult;
import com.me.order.dto.OrderCancelContext;
import com.me.order.dto.OrderCancelMatchResult;
import com.me.order.dto.OrderCancelPrepareResult;
import com.me.order.dto.OrderFreezeApplyResult;
import com.me.order.dto.OrderMatchResult;
import com.me.order.dto.OrderOffsetSeq;
import com.me.order.dto.OrderOperationResult;
import com.me.order.service.OrderConfigCache;
import com.me.order.service.OrderMatchingService;
import com.me.order.service.OrderOperationService;
import com.me.order.service.OrderService;
import com.me.shared.event.MatchPairEvent;
import com.me.shared.event.OrderFreezeResultBatch;
import com.me.shared.event.OrderFreezeResultEvent;
import com.me.shared.order.OrderCancelBatch;
import com.me.shared.order.OrderCancelCommand;
import com.me.shared.order.OrderCommand;
import com.me.shared.order.OrderCommandBatch;
import com.me.shared.order.OrderHistoryEvent;
import com.me.shared.order.OrderPlacedEvent;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * 委托操作执行器实现。
 *
 * <p><b>批次串行 + 批内按原定义并发</b>：下单命令、冻结回执、撤单三条链路的所有批次
 * 先竞争 {@link #orderLock}，拿到锁后自增 {@link #orderSeq} 并把该序号写入本批每条
 * 输入的 offset-seq 映射。锁内保持各链路原有的并发方式：
 * <ul>
 *   <li>下单命令：逐条提交线程池，按原命令顺序收集结果；</li>
 *   <li>冻结回执：并发更新委托 → 引擎对象锁内聚合撮合 → 并发清算 → 聚合输出；</li>
 *   <li>撤单：并发查询 → 引擎对象锁内聚合撤销 → 并发更新 → 聚合输出。</li>
 * </ul>
 * 三条链路共用 {@code orderOperationExecutor}；{@code orderLock} 保证同一时刻只有一个
 * 批次、一个批次只使用一条链路的并发阶段，因此共享池不改变每批并发度，也不会跨池等待。
 *
 * <p><b>为什么整批加锁</b>：offset-seq 的价值在于「orderSeq 的顺序就是委托状态推进的
 * 实际顺序」。如果只锁单条命令，不同 consumer 的批次会交错，seq 无法用于快照恢复后的
 * 确定性重放。代价是三条委托链路的跨链路并发退化为「批内并发」，吞吐上限由单批大小和
 * 各链路线程池决定。
 *
 * <p><b>演示版重投语义</b>：不缓存撮合输出/重放。Kafka 事务回滚后的重投可能重复执行或
 * 丢失上一次未提交的输出；订单状态与撮合引擎的 orderId 去重保证重复回执不会重复撮合。
 */
@Service
public class OrderOperationServiceImpl implements OrderOperationService {

    private static final Logger log = LoggerFactory.getLogger(OrderOperationServiceImpl.class);

    private final OrderService orderService;
    private final OrderConfigCache configCache;
    private final OrderMatchingService orderMatchingService;
    private final MatchingEngine matchingEngine;

    /** 三条链路共用的执行池；orderLock 保证同一时刻只有一个批次在使用它。 */
    private final ExecutorService workerPool;

    /** 委托操作的全局序号；每个批次在 {@link #orderLock} 内自增一次。 */
    private final AtomicLong orderSeq = new AtomicLong();

    /**
     * 批次级全局锁：下单命令、冻结回执、撤单三条链路的批次在这里串行，
     * 保证 orderSeq 顺序与实际委托状态推进顺序一致。
     */
    private final ReentrantLock orderLock = new ReentrantLock();

    public OrderOperationServiceImpl(
            OrderService orderService,
            OrderConfigCache configCache,
            OrderMatchingService orderMatchingService,
            MatchingEngine matchingEngine,
            @Qualifier("orderOperationExecutor") ExecutorService workerPool) {
        this.orderService = orderService;
        this.configCache = configCache;
        this.orderMatchingService = orderMatchingService;
        this.matchingEngine = matchingEngine;
        this.workerPool = workerPool;
    }

    // ==================================================================
    // 下单命令
    // ==================================================================

    @Override
    public OrderOperationResult executeCommands(
            List<ConsumerRecord<String, OrderCommandBatch>> records) {
        if (records == null || records.isEmpty()) {
            return OrderOperationResult.empty(orderSeq.get());
        }
        orderLock.lock();
        try {
            long seq = orderSeq.incrementAndGet();
            List<PendingOrder> pending = flattenCommands(records);
            List<FreezeCommand> freezeCommands = saveConcurrently(pending);
            return new OrderOperationResult(seq, offsets(seq, records), freezeCommands,
                    Map.of(), List.of(), List.of());
        } finally {
            orderLock.unlock();
        }
    }

    private List<FreezeCommand> saveConcurrently(List<PendingOrder> pending) {
        if (pending.isEmpty()) {
            return List.of();
        }
        List<Future<SaveOutcome>> futures = new ArrayList<>(pending.size());
        for (PendingOrder order : pending) {
            futures.add(workerPool.submit(() -> save(order)));
        }

        // 按原命令顺序收集结果，保证同一 symbol 的冻结命令顺序与下单顺序一致。
        List<FreezeCommand> freezeCommands = new ArrayList<>(pending.size());
        for (int position = 0; position < pending.size(); position++) {
            PendingOrder source = pending.get(position);
            SaveOutcome outcome = await(futures.get(position), "save order command");
            switch (outcome.status()) {
                case NULL_COMMAND -> log.warn("null order command ignored: partition={}, "
                                + "offset={}, index={}, key={}",
                        source.record().partition(), source.record().offset(), source.index(),
                        source.record().key());
                case UNKNOWN_SYMBOL -> log.warn("order command rejected: unknown or offline "
                                + "symbol: partition={}, offset={}, index={}, key={}, command={}",
                        source.record().partition(), source.record().offset(), source.index(),
                        source.record().key(), source.command());
                case REJECTED -> log.warn("order command rejected: partition={}, offset={}, "
                                + "index={}, key={}, reason={}, command={}",
                        source.record().partition(), source.record().offset(), source.index(),
                        source.record().key(), outcome.result().reason(), source.command());
                case SAVED -> {
                    FundsCommand freezeCommand = outcome.result().freezeCommand();
                    if (freezeCommand != null) {
                        freezeCommands.add(new FreezeCommand(
                                outcome.result().order().symbol(), freezeCommand));
                    }
                }
            }
        }
        return freezeCommands;
    }

    private SaveOutcome save(PendingOrder pending) {
        OrderCommand command = pending.command();
        if (command == null) {
            return new SaveOutcome(SaveStatus.NULL_COMMAND, null);
        }
        TradingPairView pair = configCache.pair(command.symbol());
        if (pair == null) {
            return new SaveOutcome(SaveStatus.UNKNOWN_SYMBOL, null);
        }
        CurrencyView quoteCurrency = configCache.currency(pair.quoteCurrency());
        int quoteScale = quoteCurrency == null ? 0 : quoteCurrency.scale();
        OrderCommand scaled = command.withScales(
                pair.priceScale(), pair.quantityScale(), quoteScale);

        OrderApplyResult result = orderService.apply(scaled);
        return result.success()
                ? new SaveOutcome(SaveStatus.SAVED, result)
                : new SaveOutcome(SaveStatus.REJECTED, result);
    }

    private static List<PendingOrder> flattenCommands(
            List<ConsumerRecord<String, OrderCommandBatch>> records) {
        List<PendingOrder> pending = new ArrayList<>();
        for (ConsumerRecord<String, OrderCommandBatch> record : records) {
            OrderCommandBatch batch = record.value();
            if (batch == null) {
                continue;
            }
            List<OrderCommand> commands = batch.getMsgList();
            for (int index = 0; index < commands.size(); index++) {
                pending.add(new PendingOrder(record, index, commands.get(index)));
            }
        }
        return pending;
    }

    // ==================================================================
    // 冻结回执（撮合 + 清算）
    // ==================================================================

    @Override
    public OrderOperationResult executeFreezeResults(
            List<ConsumerRecord<String, OrderFreezeResultBatch>> records) {
        if (records == null || records.isEmpty()) {
            return OrderOperationResult.empty(orderSeq.get());
        }
        orderLock.lock();
        try {
            long seq = orderSeq.incrementAndGet();
            List<PendingFreeze> pending = flattenFreezeResults(records);
            List<PlacedOrder> placed = applyFreezeResults(pending);

            List<OrderMatchResult> matches = List.of();
            if (!placed.isEmpty()) {
                // 引擎对象锁一直持有到清算完成，撤单的引擎阶段会等待，避免与清算交错。
                synchronized (matchingEngine) {
                    List<OrderPlacedEvent> orders = new ArrayList<>(placed.size());
                    for (PlacedOrder order : placed) {
                        orders.add(order.event());
                    }
                    List<MatchResult> engineResults = orderMatchingService.matchBatch(orders);
                    matches = clearConcurrently(placed, engineResults);
                }
            }

            Outputs outputs = assembleOutputs(placed, matches);
            return new OrderOperationResult(seq, offsets(seq, records), List.of(),
                    outputs.matchPairsBySymbol(), outputs.settlementCommands(),
                    outputs.historyEvents());
        } finally {
            orderLock.unlock();
        }
    }

    private static List<PendingFreeze> flattenFreezeResults(
            List<ConsumerRecord<String, OrderFreezeResultBatch>> records) {
        List<PendingFreeze> pending = new ArrayList<>();
        for (ConsumerRecord<String, OrderFreezeResultBatch> record : records) {
            OrderFreezeResultBatch batch = record.value();
            if (batch == null) {
                continue;
            }
            List<OrderFreezeResultEvent> events = batch.getMsgList();
            for (int index = 0; index < events.size(); index++) {
                pending.add(new PendingFreeze(record, index, events.get(index)));
            }
        }
        return pending;
    }

    private List<PlacedOrder> applyFreezeResults(List<PendingFreeze> pending) {
        List<Future<OrderFreezeApplyResult>> futures = new ArrayList<>(pending.size());
        for (PendingFreeze item : pending) {
            futures.add(workerPool.submit(
                    () -> orderService.onFreezeResult(item.event())));
        }

        List<PlacedOrder> placed = new ArrayList<>(pending.size());
        for (int index = 0; index < pending.size(); index++) {
            PendingFreeze source = pending.get(index);
            OrderFreezeApplyResult result = await(futures.get(index), "apply freeze result");
            if (!result.handled()) {
                log.warn("order freeze result ignored: partition={}, offset={}, index={}, key={}, "
                                + "reason={}, event={}",
                        source.record().partition(), source.record().offset(), source.index(),
                        source.record().key(), result.reason(), source.event());
                continue;
            }
            if (result.placedEvent() == null) {
                log.warn("order rejected after freeze failure: partition={}, offset={}, index={}, "
                                + "orderId={}, reason={}",
                        source.record().partition(), source.record().offset(), source.index(),
                        source.event() == null ? null : source.event().orderId(),
                        result.reason());
                continue;
            }
            placed.add(new PlacedOrder(source, result.placedEvent()));
        }
        return placed;
    }

    private List<OrderMatchResult> clearConcurrently(List<PlacedOrder> placed,
                                                     List<MatchResult> engineResults) {
        if (engineResults.size() != placed.size()) {
            throw new IllegalStateException("engine result count " + engineResults.size()
                    + " does not match placed order count " + placed.size());
        }
        List<Future<OrderMatchResult>> futures = new ArrayList<>(placed.size());
        for (int index = 0; index < placed.size(); index++) {
            PlacedOrder order = placed.get(index);
            MatchResult result = engineResults.get(index);
            if (result.duplicate()) {
                log.warn("matching duplicate ignored without replay: orderId={}, symbol={}, "
                                + "partition={}, offset={}, index={}",
                        order.event().orderId(), order.event().symbol(),
                        order.source().record().partition(),
                        order.source().record().offset(), order.source().index());
            }
            futures.add(workerPool.submit(
                    () -> orderMatchingService.clear(order.event(), result)));
        }

        List<OrderMatchResult> matches = new ArrayList<>(placed.size());
        for (Future<OrderMatchResult> future : futures) {
            matches.add(await(future, "clear trade"));
        }
        return matches;
    }

    private Outputs assembleOutputs(List<PlacedOrder> placed, List<OrderMatchResult> matches) {
        Map<String, List<MatchPairEvent>> matchPairsBySymbol = new LinkedHashMap<>();
        List<TradeSettlementCommand> settlementCommands = new ArrayList<>();
        List<OrderHistoryEvent> historyEvents = new ArrayList<>();
        for (int index = 0; index < matches.size(); index++) {
            OrderMatchResult match = matches.get(index);
            PlacedOrder source = placed.get(index);
            if (!match.publishable()) {
                log.warn("in-process matching produced no publishable outputs: partition={}, "
                                + "offset={}, index={}, orderId={}, outcome={}",
                        source.source().record().partition(), source.source().record().offset(),
                        source.source().index(), match.orderId(), match.outcome());
                continue;
            }
            String symbol = symbolKey(source.source().record());
            if (!match.matchPairEvents().isEmpty()) {
                matchPairsBySymbol.computeIfAbsent(symbol, ignored -> new ArrayList<>())
                        .addAll(match.matchPairEvents());
            }
            if (!match.settlementCommands().isEmpty()) {
                settlementCommands.addAll(match.settlementCommands());
            }
            if (!match.historyEvents().isEmpty()) {
                historyEvents.addAll(match.historyEvents());
            }
            match.ignoredTrades().forEach(ignored ->
                    log.warn("trade clearing ignored: orderId={}, tradeId={}, reason={}",
                            match.orderId(), ignored.tradeId(), ignored.reason()));
        }
        return new Outputs(matchPairsBySymbol, settlementCommands, historyEvents);
    }

    /** 回执消息 key 即 symbol；兼容旧的固定 key。 */
    private static String symbolKey(ConsumerRecord<String, OrderFreezeResultBatch> record) {
        String key = record.key();
        return key == null || key.isBlank() ? KafkaSink.BATCH_KEY : key;
    }

    // ==================================================================
    // 撤单
    // ==================================================================

    @Override
    public OrderOperationResult executeCancels(
            List<ConsumerRecord<String, OrderCancelBatch>> records) {
        if (records == null || records.isEmpty()) {
            return OrderOperationResult.empty(orderSeq.get());
        }
        orderLock.lock();
        try {
            long seq = orderSeq.incrementAndGet();
            List<PendingCancel> pending = flattenCancels(records);
            List<PreparedCancel> prepared = prepareConcurrently(pending);
            List<OrderCancelContext> contexts = new ArrayList<>(prepared.size());
            for (PreparedCancel item : prepared) {
                OrderCancelPrepareResult result = item.prepareResult();
                if (result.cancellable()) {
                    contexts.add(result.context());
                    continue;
                }
                PendingCancel source = item.source();
                log.info("order cancel rejected: partition={}, offset={}, index={}, orderId={}, "
                                + "reason={}",
                        source.record().partition(), source.record().offset(), source.index(),
                        source.command() == null ? null : source.command().orderId(),
                        result.reason());
            }

            List<TradeSettlementCommand> settlements = new ArrayList<>();
            List<OrderHistoryEvent> historyEvents = new ArrayList<>();
            if (!contexts.isEmpty()) {
                cancelAndApply(contexts, settlements, historyEvents);
            }
            return new OrderOperationResult(seq, offsets(seq, records), List.of(), Map.of(),
                    settlements, historyEvents);
        } finally {
            orderLock.unlock();
        }
    }

    private void cancelAndApply(List<OrderCancelContext> contexts,
                                List<TradeSettlementCommand> settlements,
                                List<OrderHistoryEvent> historyEvents) {
        // 聚合撮合撤消单，调用撮合引擎（引擎对象锁），收集撤消挂单。
        List<OrderCancelMatchResult> matchResults = orderMatchingService.cancel(contexts);
        List<OrderCancelContext> cancelingContexts = new ArrayList<>(contexts.size());
        for (OrderCancelMatchResult result : matchResults) {
            CancelResult engineResult = result.engineResult();
            if (engineResult.removed()) {
                log.debug("order cancel removed from book: orderId={}, symbol={}, quantity={}",
                        engineResult.orderId(), engineResult.symbol(),
                        engineResult.removedQuantity());
            } else {
                log.info("order cancel not resting: orderId={}, symbol={}, reason={}",
                        engineResult.orderId(), engineResult.symbol(), engineResult.reason());
            }
            if (result.accepted()) {
                cancelingContexts.add(result.context());
            } else {
                log.info("order cancel begin rejected: orderId={}, symbol={}, reason={}",
                        result.context().orderId(), result.context().symbol(), result.reason());
            }
        }
        if (cancelingContexts.isEmpty()) {
            return;
        }

        List<OrderCancelApplyResult> applied = applyConcurrently(cancelingContexts);
        for (int index = 0; index < applied.size(); index++) {
            OrderCancelApplyResult result = applied.get(index);
            OrderCancelContext context = cancelingContexts.get(index);
            if (!result.handled()) {
                log.warn("order cancel update ignored: orderId={}, symbol={}, reason={}",
                        context.orderId(), context.symbol(), result.reason());
                continue;
            }
            if (result.refundAmount() > 0) {
                log.info("order cancel settled: orderId={}, symbol={}, refund={}",
                        context.orderId(), context.symbol(), result.refundAmount());
            }
            settlements.addAll(result.settlementCommands());
            if (result.historyEvent() != null) {
                historyEvents.add(result.historyEvent());
            }
        }
    }

    private static List<PendingCancel> flattenCancels(
            List<ConsumerRecord<String, OrderCancelBatch>> records) {
        List<PendingCancel> pending = new ArrayList<>();
        for (ConsumerRecord<String, OrderCancelBatch> record : records) {
            OrderCancelBatch batch = record.value();
            if (batch == null) {
                continue;
            }
            List<OrderCancelCommand> commands = batch.getMsgList();
            for (int index = 0; index < commands.size(); index++) {
                pending.add(new PendingCancel(record, index, commands.get(index)));
            }
        }
        return pending;
    }

    private List<PreparedCancel> prepareConcurrently(List<PendingCancel> pending) {
        List<Future<OrderCancelPrepareResult>> futures = new ArrayList<>(pending.size());
        for (PendingCancel item : pending) {
            futures.add(workerPool.submit(() -> prepare(item)));
        }
        List<PreparedCancel> prepared = new ArrayList<>(pending.size());
        for (int index = 0; index < pending.size(); index++) {
            prepared.add(new PreparedCancel(pending.get(index),
                    await(futures.get(index), "cancel order query")));
        }
        return prepared;
    }

    private OrderCancelPrepareResult prepare(PendingCancel item) {
        OrderCancelCommand command = item.command();
        if (command == null) {
            return OrderCancelPrepareResult.rejected("CANCEL_COMMAND_IS_NULL");
        }
        return orderService.prepareCancel(command.orderId(), command.userId(), command.symbol());
    }

    private List<OrderCancelApplyResult> applyConcurrently(List<OrderCancelContext> contexts) {
        List<Future<OrderCancelApplyResult>> futures = new ArrayList<>(contexts.size());
        for (OrderCancelContext context : contexts) {
            futures.add(workerPool.submit(() -> orderService.applyCancel(context)));
        }
        List<OrderCancelApplyResult> applied = new ArrayList<>(contexts.size());
        for (Future<OrderCancelApplyResult> future : futures) {
            applied.add(await(future, "cancel order update"));
        }
        return applied;
    }

    // ==================================================================
    // 公共辅助
    // ==================================================================

    @Override
    public long currentSeq() {
        return orderSeq.get();
    }

    /** 本批每条输入消费记录与批次序号的映射；按输入顺序排列。 */
    private static List<OrderOffsetSeq> offsets(
            long seq, List<? extends ConsumerRecord<?, ?>> records) {
        List<OrderOffsetSeq> offsets = new ArrayList<>(records.size());
        for (ConsumerRecord<?, ?> record : records) {
            offsets.add(new OrderOffsetSeq(
                    record.topic(), record.partition(), record.offset(), seq));
        }
        return offsets;
    }

    private static <T> T await(Future<T> future, String action) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(action + " interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new IllegalStateException(action + " failed", cause);
        }
    }

    /** 一条待保存订单及其在批量消息中的位置，用于按原顺序聚合与日志定位。 */
    private record PendingOrder(
            ConsumerRecord<String, OrderCommandBatch> record,
            int index,
            OrderCommand command) {
    }

    private enum SaveStatus {
        SAVED,
        NULL_COMMAND,
        UNKNOWN_SYMBOL,
        REJECTED
    }

    private record SaveOutcome(SaveStatus status, OrderApplyResult result) {
    }

    /** 一条待处理冻结回执及其在批量消息中的位置。 */
    private record PendingFreeze(
            ConsumerRecord<String, OrderFreezeResultBatch> record,
            int index,
            OrderFreezeResultEvent event) {
    }

    /** 已支付、可以进入撮合的委托及其来源位置。 */
    private record PlacedOrder(PendingFreeze source, OrderPlacedEvent event) {
    }

    /** 冻结回执链路聚合后的输出。 */
    private record Outputs(
            Map<String, List<MatchPairEvent>> matchPairsBySymbol,
            List<TradeSettlementCommand> settlementCommands,
            List<OrderHistoryEvent> historyEvents) {
    }

    /** 一条待处理撤单及其在批量消息中的位置，仅用于日志定位。 */
    private record PendingCancel(
            ConsumerRecord<String, OrderCancelBatch> record,
            int index,
            OrderCancelCommand command) {
    }

    private record PreparedCancel(PendingCancel source, OrderCancelPrepareResult prepareResult) {
    }
}
