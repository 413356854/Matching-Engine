package com.me.currency.ingress;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.me.shared.order.OrderCommand;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * 下单命令的 Disruptor 入口：HTTP 线程是生产者，单 handler 线程聚合后批量发 Kafka。
 *
 * <p><b>为什么批量</b>：逐条 {@code OrderCommand} 在高 TPS 下把大量记录压给消费者，
 * poll / 反序列化 / 事务提交的每记录成本成为瓶颈。handler 把订单按 symbol 放入
 * {@code Map<String, List>}，任一 symbol 达到 2000 条或 50ms 定时窗口到达时，把 map 中
 * 所有 symbol 的命令分别组装成 {@code OrderCommandBatch}，消费者一次处理一整批。
 *
 * <p><b>顺序</b>：flush 按 symbol 分组发送，key=symbol，同一 symbol 的批次始终进入同一
 * 分区；聚合线程是唯一发送者，批次之间也按命令入队顺序写入，因此价格-时间优先不变。
 *
 * <p><b>背压</b>：HTTP 线程用 {@code tryPublishEvent} 非阻塞入队，ring 满或未启动直接抛异常，
 * controller 返回 503；绝不无限排队。handler 线程只做聚合与 producer 入队，不等待 broker ack。
 *
 * <p>生命周期 phase = 50：早于 Web 服务器（{@code Integer.MAX_VALUE - 1}）启动、晚于它停止，
 * 保证请求到达时 ring 已就绪，停机时最后一个请求也能入队并 flush。
 */
public final class OrderIngressRing implements OrderIngress, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OrderIngressRing.class);

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final OrderIngressProperties properties;
    private final OrderIngressHandler handler;
    private final Disruptor<OrderIngressEvent> disruptor;

    private volatile RingBuffer<OrderIngressEvent> ringBuffer;
    private volatile ScheduledExecutorService ticker;
    private volatile Thread handlerThread;
    private volatile boolean running;

    public OrderIngressRing(OrderIngressProperties properties, OrderBatchSender sender) {
        this.properties = properties;
        this.handler = new OrderIngressHandler(properties, sender);
        this.disruptor = new Disruptor<>(
                OrderIngressEvent::new,
                properties.ringSize(),
                this::newHandlerThread,
                ProducerType.MULTI,
                new BlockingWaitStrategy());
        this.disruptor.handleEventsWith(handler);
        this.disruptor.setDefaultExceptionHandler(new LoggingExceptionHandler());
    }

    private Thread newHandlerThread(Runnable runnable) {
        Thread thread = new Thread(runnable, "order-ingress-handler");
        thread.setDaemon(true);
        handlerThread = thread;
        return thread;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        ringBuffer = disruptor.start();
        running = true;
        ticker = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "order-ingress-tick");
            thread.setDaemon(true);
            return thread;
        });
        ticker.scheduleAtFixedRate(this::publishTick,
                properties.flushIntervalMs(), properties.flushIntervalMs(),
                TimeUnit.MILLISECONDS);
    }

    /** 定时发布一个空事件，唤醒聚合线程检查 50ms 窗口。 */
    private void publishTick() {
        RingBuffer<OrderIngressEvent> buffer = ringBuffer;
        if (!running || buffer == null) {
            return;
        }
        try {
            buffer.tryPublishEvent((event, sequence) -> event.setTick());
        } catch (RuntimeException ignored) {
            // ring 满时跳过本次 tick，下一轮会再次触发
        }
    }

    @Override
    public void submit(OrderCommand command) {
        RingBuffer<OrderIngressEvent> buffer = ringBuffer;
        if (!running || buffer == null) {
            throw new IllegalStateException("order ingress ring is not running");
        }
        boolean published;
        try {
            published = buffer.tryPublishEvent(
                    (event, sequence, cmd) -> event.set(cmd), command);
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "failed to enqueue order command: orderId=" + command.orderId(), e);
        }
        if (!published) {
            throw new IllegalStateException("order ingress ring is full: capacity="
                    + properties.ringSize() + ", orderId=" + command.orderId());
        }
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        ScheduledExecutorService currentTicker = ticker;
        ticker = null;
        if (currentTicker != null) {
            currentTicker.shutdownNow();
        }
        // 先发布停机屏障：handler 处理到它时，之前的命令都已进入 pending 并 flush。
        // Disruptor 的 shutdown 不会处理未消费的事件，必须自己等屏障被处理完。
        RingBuffer<OrderIngressEvent> buffer = ringBuffer;
        if (buffer != null) {
            CompletableFuture<Void> drained = publishStopSignal(buffer);
            if (drained != null) {
                try {
                    drained.get(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException | TimeoutException e) {
                    log.warn("order ingress handler did not drain before stop", e);
                }
            }
        }
        try {
            disruptor.shutdown(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (com.lmax.disruptor.TimeoutException e) {
            log.warn("order ingress handler did not terminate within {}s; halting",
                    SHUTDOWN_TIMEOUT_SECONDS);
            disruptor.halt();
        }
        Thread thread = handlerThread;
        if (thread != null) {
            try {
                thread.join(TimeUnit.SECONDS.toMillis(SHUTDOWN_TIMEOUT_SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // 只在 handler 线程确实结束后才碰 pending，避免超时路径下的并发访问。
        if (thread == null || !thread.isAlive()) {
            handler.flushRemaining();
        } else {
            log.warn("order ingress handler still running; skipping final flush");
        }
    }

    /** 发布停机屏障并返回 handler 处理完它后完成的 future；ring 一直满时返回 null。 */
    private CompletableFuture<Void> publishStopSignal(RingBuffer<OrderIngressEvent> buffer) {
        CompletableFuture<Void> drained = new CompletableFuture<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SHUTDOWN_TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline) {
            boolean published;
            try {
                published = buffer.tryPublishEvent(
                        (event, sequence, future) -> event.setStop(future), drained);
            } catch (RuntimeException e) {
                log.warn("failed to publish order ingress stop signal", e);
                return null;
            }
            if (published) {
                return drained;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        log.warn("order ingress ring is full during stop; skipping drain barrier");
        return null;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 50;
    }

    private static final class LoggingExceptionHandler
            implements ExceptionHandler<OrderIngressEvent> {

        @Override
        public void handleEventException(Throwable ex, long sequence,
                                         OrderIngressEvent event) {
            log.error("order ingress handler failed at sequence {}", sequence, ex);
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            log.error("order ingress handler start failed", ex);
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            log.error("order ingress handler shutdown failed", ex);
        }
    }
}
