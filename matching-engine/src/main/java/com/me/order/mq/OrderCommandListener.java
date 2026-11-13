package com.me.order.mq;

import com.me.account.dto.FundsCommand;
import com.me.currency.vo.CurrencyView;
import com.me.currency.vo.TradingPairView;
import com.me.order.dto.OrderApplyResult;
import com.me.order.service.OrderConfigCache;
import com.me.order.service.OrderService;
import com.me.shared.order.OrderCommand;
import com.me.shared.order.OrderCommandBatch;
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
 * 消费委托命令，并发保存委托并聚合发送冻结命令。
 *
 * <p><b>唯一接单入口</b>：币币模块的 {@code POST /api/v1/currency/orders} 只向
 * {@code me.order.commands-batch} 投递命令，不直接调用委托模块；本监听器消费该 topic，
 * 是系统唯一的下单方式与入口。
 *
 * <p>topic key 使用交易对 symbol，同一交易对的命令落在同一分区、按序处理。
 * 同一 orderId + requestId 重投由 {@link OrderService} 返回已有委托的当前状态（不做
 * requestId 缓存）；监听容器重试不会生成重复 orderId 的委托。交易对 / 币种精度来自
 * {@link OrderConfigCache}（启动时加载的不可变内存 Map），可在工作线程中安全并发读取。
 *
 * <p><b>并发保存</b>：本监听器把批内命令逐条提交到 {@code orderCommandExecutor} 线程池，
 * 工作线程完成交易对 / 币种精度补全并调用 {@link OrderService#apply} 保存委托、生成
 * 冻结命令。poll 线程按<b>原命令顺序</b>等待并收集结果，因此同一 symbol 的冻结命令顺序
 * 与下单顺序一致，价格-时间优先不被破坏。
 *
 * <p><b>批量消息</b>：{@code me.order.commands-batch} 的每条 Kafka 消息是一个
 * {@link OrderCommandBatch}（下单入口 Disruptor 把订单按 symbol 放入 {@code Map<String, List>}，
 * 任一 symbol 达到 2000 条或 50ms 窗口到达时把 map 中所有 symbol 分别封装发送）。一个 poll
 * 批次一个 Kafka 事务；冻结命令与「委托命令消费 offset」一起提交或回滚。发送失败会回滚整批，
 * 命令重投时重新发送；冻结命令 requestId 固定，账户模块按 requestId 幂等。
 *
 * <p>冻结命令按 symbol 分组后发往 {@code me.account.order-freeze-commands-batch}，key=symbol；
 * 充值等其它资金命令仍在 {@code me.account.commands}（key=userId）。这样账户冻结阶段可以
 * 按 symbol 分区并行，同一 symbol 的冻结仍由单 consumer 按序处理。
 */
@Component
public class OrderCommandListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCommandListener.class);

    private final OrderService orderService;
    private final OrderConfigCache configCache;
    private final OrderFreezeCommandPublisher freezeCommandPublisher;
    private final ExecutorService workerPool;

    public OrderCommandListener(
            OrderService orderService,
            OrderConfigCache configCache,
            OrderFreezeCommandPublisher freezeCommandPublisher,
            @Qualifier("orderCommandExecutor") ExecutorService workerPool) {
        this.orderService = orderService;
        this.configCache = configCache;
        this.freezeCommandPublisher = freezeCommandPublisher;
        this.workerPool = workerPool;
    }

    public void onCommand(List<ConsumerRecord<String, OrderCommandBatch>> records) {
        List<PendingOrder> pending = flatten(records);
        if (pending.isEmpty()) {
            return;
        }

        // 1) 线程池并发保存订单并生成冻结命令。
        List<Future<SaveOutcome>> futures = new ArrayList<>(pending.size());
        for (PendingOrder order : pending) {
            futures.add(workerPool.submit(() -> save(order)));
        }

        // 2) 按原命令顺序收集结果，保证同一 symbol 的冻结命令顺序与下单顺序一致。
        List<OrderFreezeCommandPublisher.FreezeCommand> freezeCommands =
                new ArrayList<>(pending.size());
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
                        freezeCommands.add(new OrderFreezeCommandPublisher.FreezeCommand(
                                outcome.result().order().symbol(), freezeCommand));
                    }
                }
            }
        }

        // 3) 聚合冻结命令，在 poll 线程的 Kafka 事务内按 symbol 分组发送 orderFreezeBatch。
        if (!freezeCommands.isEmpty()) {
            freezeCommandPublisher.publishBatch(freezeCommands);
        }
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

    private static List<PendingOrder> flatten(
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
}
