package com.me.order.mq;

import com.me.account.dto.TradeSettlementCommand;
import com.me.kafka.KafkaSink;
import com.me.matching.dto.MatchResult;
import com.me.matching.service.MatchingEngine;
import com.me.order.dto.OrderFreezeApplyResult;
import com.me.order.dto.OrderMatchResult;
import com.me.order.service.OrderMatchingService;
import com.me.order.service.OrderService;
import com.me.shared.event.MatchPairEvent;
import com.me.shared.event.OrderFreezeResultBatch;
import com.me.shared.event.OrderFreezeResultEvent;
import com.me.shared.order.OrderHistoryEvent;
import com.me.shared.order.OrderPlacedEvent;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * 消费账户模块的冻结回执批量消息，完成「并发更新订单 → 聚合撮合 → 并发成交清算」。
 *
 * <p><b>处理阶段</b>：
 * <ol>
 *   <li><b>并发更新订单</b>：线程池并发调用 {@link OrderService#onFreezeResult}，把委托
 *       从 {@code ACCEPTED} 推进为 {@code PAID}（或冻结失败推进为 {@code REJECTED}），
 *       可撮合的委托生成撮合单（{@link OrderPlacedEvent}）；</li>
 *   <li><b>聚合撮合</b>：按原回执顺序把整批撮合单交给 {@link OrderMatchingService#matchBatch}，
 *       在引擎对象锁内依次调用撮合引擎，收集成交对；</li>
 *   <li><b>并发清算</b>：线程池并发调用 {@link OrderMatchingService#clear}，逐笔更新 taker /
 *       maker 委托并生成撮合对、结算命令（含市价买入完成退款）；</li>
 *   <li><b>聚合发送</b>：按 symbol 聚合撮合对，聚合清算命令后发送
 *       {@code me.account.settlements-batch}。</li>
 * </ol>
 *
 * <p><b>引擎对象锁</b>：第 2、3 阶段在同一个 {@code synchronized (matchingEngine)} 临界区内
 * 完成（清算任务本身不再获取引擎锁），因此撤单的引擎阶段会等待整批成交清算完成，
 * 不会出现“撤单已解冻、清算尚未推进委托”的竞争。清算任务只访问委托内存状态，可以并发执行。
 *
 * <p><b>批量消费</b>：一个 poll 批次一个 Kafka 事务；冻结回执 offset、
 * {@code me.match-pair-batch} 撮合对与 {@code me.account.settlements-batch} 结算命令一起提交/回滚。
 * 任一条失败都会回滚整批并重投。
 *
 * <p><b>演示版重投语义</b>：不缓存撮合输出。同一订单的重复（或回滚后重投）回执仍会交给
 * {@link OrderMatchingService}，撮合引擎按 orderId 返回 duplicate，因此不会重复产生撮合对/
 * 结算；若上一次事务未提交，这些输出会丢失。后续会补备份消息与内存状态恢复。
 *
 * <p><b>并发</b>：冻结回执批量消息按 symbol 分组、key=symbol，同一 symbol 的消息落在同一分区
 * 且保持生产顺序。{@code app.kafka.consumer.concurrency.order-freeze-result} 可以起多个
 * consumer：Kafka 按分区分配，保证同一 symbol 只由一个 consumer 处理。
 */
@Component
public class OrderFreezeResultListener {

    private static final Logger log = LoggerFactory.getLogger(OrderFreezeResultListener.class);

    private final OrderService orderService;
    private final OrderMatchingService orderMatchingService;
    private final MatchingEngine matchingEngine;
    private final MatchPairEventPublisher matchPairPublisher;
    private final TradeSettlementCommandPublisher settlementPublisher;
    private final OrderHistoryPublisher orderHistoryPublisher;
    private final ExecutorService workerPool;

    public OrderFreezeResultListener(
            OrderService orderService,
            OrderMatchingService orderMatchingService,
            MatchingEngine matchingEngine,
            MatchPairEventPublisher matchPairPublisher,
            TradeSettlementCommandPublisher settlementPublisher,
            OrderHistoryPublisher orderHistoryPublisher,
            @Qualifier("orderFreezeResultExecutor") ExecutorService workerPool) {
        this.orderService = orderService;
        this.orderMatchingService = orderMatchingService;
        this.matchingEngine = matchingEngine;
        this.matchPairPublisher = matchPairPublisher;
        this.settlementPublisher = settlementPublisher;
        this.orderHistoryPublisher = orderHistoryPublisher;
        this.workerPool = workerPool;
    }

    public void onFreezeResult(List<ConsumerRecord<String, OrderFreezeResultBatch>> records) {
        List<PendingFreeze> pending = flatten(records);
        if (pending.isEmpty()) {
            return;
        }

        // 1) 线程池并发更新订单，生成撮合单。
        List<PlacedOrder> placed = applyFreezeResults(pending);
        if (placed.isEmpty()) {
            return;
        }

        // 2) 聚合撮合单调用撮合引擎并收集成交对；3) 线程池并发更新订单生成清算单。
        //    引擎对象锁一直持有到清算完成，撤单的引擎阶段会等待，避免与清算交错。
        List<OrderMatchResult> matches;
        synchronized (matchingEngine) {
            List<OrderPlacedEvent> orders = new ArrayList<>(placed.size());
            for (PlacedOrder order : placed) {
                orders.add(order.event());
            }
            List<MatchResult> engineResults = orderMatchingService.matchBatch(orders);
            matches = clearConcurrently(placed, engineResults);
        }

        // 4) 聚合撮合对与清算单，在 poll 线程的 Kafka 事务内发送。
        publishOutputs(placed, matches);
    }

    private List<PlacedOrder> applyFreezeResults(List<PendingFreeze> pending) {
        List<Future<OrderFreezeApplyResult>> futures = new ArrayList<>(pending.size());
        for (PendingFreeze item : pending) {
            futures.add(workerPool.submit(() -> orderService.onFreezeResult(item.event())));
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

    private void publishOutputs(List<PlacedOrder> placed, List<OrderMatchResult> matches) {
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
        if (!matchPairsBySymbol.isEmpty()) {
            matchPairPublisher.publishBatch(matchPairsBySymbol);
        }
        if (!settlementCommands.isEmpty()) {
            settlementPublisher.publishBatch(settlementCommands);
        }
        if (!historyEvents.isEmpty()) {
            orderHistoryPublisher.publishBatch(historyEvents);
        }
    }

    private static List<PendingFreeze> flatten(
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

    /** 回执消息 key 即 symbol；兼容旧的固定 key。 */
    private static String symbolKey(ConsumerRecord<String, OrderFreezeResultBatch> record) {
        String key = record.key();
        return key == null || key.isBlank() ? KafkaSink.BATCH_KEY : key;
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
}
