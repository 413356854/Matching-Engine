package com.me.currency.ingress;

import com.lmax.disruptor.EventHandler;
import com.me.shared.order.OrderCancelCommand;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 撤单 Disruptor 的唯一聚合线程：把撤单按 symbol 攒进 {@code Map<String, List>}。
 *
 * <p>flush 触发条件（任一满足即把 map 中<b>所有 symbol</b> 的撤单打包发送）：
 * <ul>
 *   <li>任一 symbol 的待发送列表达到 {@code app.currency.cancel-ingress.batch-size}
 *       （默认 1000）；</li>
 *   <li>收到定时 tick（默认每 20ms）且 map 非空。</li>
 * </ul>
 *
 * <p>一次 flush 按 symbol 分组，每个 symbol 一条 {@code OrderCancelBatch}（{@code key=symbol}）。
 * 同一 symbol 的撤单顺序在批次内保持不变，批次之间也按本线程的发送顺序进入同一分区。
 *
 * <p>本线程只做内存聚合和 Kafka {@code send} 入队，不等待 broker ack；发送失败记日志后
 * 继续处理下一组，保证单组故障不会阻塞后续 symbol 的批次。
 */
final class OrderCancelIngressHandler implements EventHandler<OrderCancelIngressEvent> {

    private static final Logger log = LoggerFactory.getLogger(OrderCancelIngressHandler.class);

    private final int batchSize;
    private final OrderCancelBatchSender sender;
    private final Map<String, List<OrderCancelCommand>> pendingBySymbol = new LinkedHashMap<>();

    OrderCancelIngressHandler(OrderCancelIngressProperties properties,
                              OrderCancelBatchSender sender) {
        this.batchSize = properties.batchSize();
        this.sender = sender;
    }

    @Override
    public void onEvent(OrderCancelIngressEvent event, long sequence, boolean endOfBatch) {
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
        OrderCancelCommand command = event.command();
        event.clear();
        if (command == null) {
            return;
        }
        List<OrderCancelCommand> cancels = pendingBySymbol.computeIfAbsent(
                command.symbol(), ignored -> new ArrayList<>());
        cancels.add(command);
        if (cancels.size() >= batchSize) {
            flush();
        }
    }

    /** ring 停止后由 stop 线程调用，发完剩余撤单。 */
    void flushRemaining() {
        flush();
    }

    /** 把当前 map 中所有 symbol 的撤单按 symbol 分组发送，并清空 map。 */
    void flush() {
        if (pendingBySymbol.isEmpty()) {
            return;
        }
        Map<String, List<OrderCancelCommand>> batch = new LinkedHashMap<>(pendingBySymbol);
        pendingBySymbol.clear();

        for (Map.Entry<String, List<OrderCancelCommand>> entry : batch.entrySet()) {
            try {
                sender.send(entry.getKey(), entry.getValue());
            } catch (RuntimeException e) {
                log.error("order cancel batch send failed: symbol={}, size={}",
                        entry.getKey(), entry.getValue().size(), e);
            }
        }
    }
}
