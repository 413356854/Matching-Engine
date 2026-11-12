package com.me.currency.ingress;

import com.lmax.disruptor.EventHandler;
import com.me.shared.order.OrderCommand;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 唯一的订单聚合线程：把 RingBuffer 中的命令攒成批量消息。
 *
 * <p>flush 触发条件：
 * <ul>
 *   <li>待发送命令达到 {@code app.currency.ingress.batch-size}（默认 2000）；</li>
 *   <li>收到定时 tick（默认每 20ms）且有待发送命令。</li>
 * </ul>
 *
 * <p>一次 flush 按 symbol 分组，每个 symbol 一条 {@code OrderCommandBatch}（{@code key=symbol}）。
 * 同一 symbol 的命令顺序在批次内保持不变，批次之间也按本线程的发送顺序进入同一分区，
 * 因此价格-时间优先依赖的下单顺序不变。
 *
 * <p>本线程只做内存聚合和 Kafka {@code send} 入队，不等待 broker ack；发送失败记日志后
 * 继续处理下一组，保证单组故障不会阻塞或丢失后续 symbol 的批次。
 */
final class OrderIngressHandler implements EventHandler<OrderIngressEvent> {

    private static final Logger log = LoggerFactory.getLogger(OrderIngressHandler.class);

    private final int batchSize;
    private final OrderBatchSender sender;
    private final List<PendingCommand> pending;

    OrderIngressHandler(OrderIngressProperties properties, OrderBatchSender sender) {
        this.batchSize = properties.batchSize();
        this.sender = sender;
        this.pending = new ArrayList<>(properties.batchSize());
    }

    @Override
    public void onEvent(OrderIngressEvent event, long sequence, boolean endOfBatch) {
        CompletableFuture<Void> stop = event.stop();
        if (stop != null) {
            event.clear();
            flush();
            stop.complete(null);
            return;
        }
        if (event.tick()) {
            event.clear();
            flush();
            return;
        }
        OrderCommand command = event.command();
        event.clear();
        if (command == null) {
            return;
        }
        pending.add(new PendingCommand(command));
        if (pending.size() >= batchSize) {
            flush();
        }
    }

    /** ring 停止后由 stop 线程调用，发完剩余命令。 */
    void flushRemaining() {
        flush();
    }

    /** 把当前待发送命令按 symbol 分组发送。 */
    void flush() {
        if (pending.isEmpty()) {
            return;
        }
        Map<String, Group> groups = new LinkedHashMap<>();
        for (PendingCommand pendingCommand : pending) {
            OrderCommand command = pendingCommand.command();
            groups.computeIfAbsent(command.symbol(), ignored -> new Group())
                    .add(command);
        }
        pending.clear();

        for (Map.Entry<String, Group> entry : groups.entrySet()) {
            Group group = entry.getValue();
            try {
                sender.send(entry.getKey(), group.commands());
            } catch (RuntimeException e) {
                log.error("order command batch send failed: symbol={}, size={}",
                        entry.getKey(), group.commands().size(), e);
            }
        }
    }

    private record PendingCommand(OrderCommand command) {
    }

    private static final class Group {

        private final List<OrderCommand> commands = new ArrayList<>();

        void add(OrderCommand command) {
            commands.add(command);
        }

        List<OrderCommand> commands() {
            return commands;
        }
    }
}
