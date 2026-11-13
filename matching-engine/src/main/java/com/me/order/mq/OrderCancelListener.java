package com.me.order.mq;

import com.me.account.dto.TradeSettlementCommand;
import com.me.matching.dto.CancelResult;
import com.me.order.dto.OrderCancelApplyResult;
import com.me.order.dto.OrderCancelContext;
import com.me.order.dto.OrderCancelMatchResult;
import com.me.order.dto.OrderCancelPrepareResult;
import com.me.order.service.OrderMatchingService;
import com.me.order.service.OrderService;
import com.me.shared.order.OrderCancelBatch;
import com.me.shared.order.OrderCancelCommand;
import com.me.shared.order.OrderHistoryEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * 消费 {@code me.order.cancels-batch}，完成撤单状态推进与解冻清算。
 *
 * <p>撤单流程与下单一致（REST → 独立 Disruptor → Kafka 批量消息 → 委托模块消费），
 * 消费端分四步：
 *
 * <ol>
 *   <li><b>并发查询</b>：把批内撤单提交到线程池，并发查询委托、校验归属与可撤状态，
 *       生成撮合撤消单（{@link OrderCancelContext}）；</li>
 *   <li><b>聚合撮合</b>：把整批可撤委托聚合后调用 {@link OrderMatchingService#cancel}，
 *       由撮合引擎对象锁保护，收集被撤销的挂单与剩余数量；</li>
 *   <li><b>并发更新</b>：把可撤委托提交到同一线程池，并发推进为 {@code CANCELED}，
 *       按成交进度生成解冻清算命令；</li>
 *   <li><b>聚合发送</b>：汇总清算命令，在 poll 线程的 Kafka 事务内发送
 *       {@code me.account.settlements-batch} 批量消息。</li>
 * </ol>
 *
 * <p><b>线程与事务</b>：前两步和第三步可以多线程执行，但只访问委托内存状态与撮合引擎；
 * 所有 Kafka 发送都留在 poll 线程（{@code KafkaSink} 的 producer 按线程绑定），
 * 与撤单 topic 的消费 offset 在同一个事务中提交或回滚。
 *
 * <p><b>幂等</b>：同一撤单命令重投时，委托已是 {@code CANCELED}，准备阶段返回
 * {@code ORDER_ALREADY_CANCELED}；即使准备阶段读取到的还是可撤状态，更新阶段的
 * {@code computeIfPresent} 也会拒绝重复推进，不会重复解冻。Kafka 事务回滚后的重投
 * 可能丢失上一次未提交的解冻命令（与成交清算相同的演示版取舍）。
 */
@Component
public class OrderCancelListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCancelListener.class);

    private final OrderService orderService;
    private final OrderMatchingService orderMatchingService;
    private final TradeSettlementCommandPublisher settlementPublisher;
    private final OrderHistoryPublisher orderHistoryPublisher;
    private final ExecutorService workerPool;

    public OrderCancelListener(
            OrderService orderService,
            OrderMatchingService orderMatchingService,
            TradeSettlementCommandPublisher settlementPublisher,
            OrderHistoryPublisher orderHistoryPublisher,
            @Qualifier("orderCancelExecutor") ExecutorService workerPool) {
        this.orderService = orderService;
        this.orderMatchingService = orderMatchingService;
        this.settlementPublisher = settlementPublisher;
        this.orderHistoryPublisher = orderHistoryPublisher;
        this.workerPool = workerPool;
    }

    public void onCancel(List<ConsumerRecord<String, OrderCancelBatch>> records) {
        List<PendingCancel> pending = flatten(records);
        if (pending.isEmpty()) {
            return;
        }

        // 1) 线程池并发查询订单并生成撮合撤消单。
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
        if (contexts.isEmpty()) {
            return;
        }

        // 2) 聚合撮合撤消单，调用撮合引擎（引擎对象锁），收集撤消挂单。
        //    同一临界区内标记“撤单中”，防止并发撮合把尚未挂单的委托重新挂回订单簿。
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

        // 3) 线程池并发更新订单并生成清算单。
        List<OrderCancelApplyResult> applied = applyConcurrently(cancelingContexts);

        // 4) 聚合清算单与委托历史，在 poll 线程的事务内发送到账户模块 / 委托历史 topic。
        List<TradeSettlementCommand> settlements = new ArrayList<>();
        List<OrderHistoryEvent> historyEvents = new ArrayList<>(applied.size());
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
        if (!settlements.isEmpty()) {
            settlementPublisher.publishBatch(settlements);
        }
        if (!historyEvents.isEmpty()) {
            orderHistoryPublisher.publishBatch(historyEvents);
        }
    }

    private static List<PendingCancel> flatten(
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

    /** 一条待处理撤单及其在批量消息中的位置，仅用于日志定位。 */
    private record PendingCancel(
            ConsumerRecord<String, OrderCancelBatch> record,
            int index,
            OrderCancelCommand command) {
    }

    private record PreparedCancel(PendingCancel source, OrderCancelPrepareResult prepareResult) {
    }
}
