package com.me.account.ingress;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.me.account.mq.AccountCommandListener;
import com.me.account.mq.AccountSettlementListener;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * account 两条消费链路共用的 Disruptor 入口。
 *
 * <p>poll 线程是生产者：把整个 poll 批次封装成 {@link AccountIngressTask} 发布到 ring，
 * 然后等待 future。唯一的 handler 线程串行处理所有批次，因此账户写入是单写者模型。
 *
 * <p>生命周期 phase = 50：早于 {@code KafkaConsumerManager}（phase = 100）启动、晚于它停止，
 * 保证 consumer 停止时 ring 仍可完成在途批次。
 */
public final class AccountIngressRing implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(AccountIngressRing.class);

    private static final long STOP_JOIN_TIMEOUT_MS = 5_000;

    private final AccountIngressProperties properties;
    private final Disruptor<AccountIngressEvent> disruptor;
    private final Set<AccountIngressTask> inFlight = ConcurrentHashMap.newKeySet();

    private volatile RingBuffer<AccountIngressEvent> ringBuffer;
    private volatile Thread handlerThread;
    private volatile boolean running;

    public AccountIngressRing(AccountIngressProperties properties,
                              AccountCommandListener commandListener,
                              AccountSettlementListener settlementListener) {
        if (properties.ringSize() <= 0
                || Integer.bitCount(properties.ringSize()) != 1) {
            throw new IllegalArgumentException(
                    "app.account.ingress.ring-size must be a positive power of two: "
                            + properties.ringSize());
        }
        this.properties = properties;
        this.disruptor = new Disruptor<>(
                AccountIngressEvent::new,
                properties.ringSize(),
                this::newHandlerThread,
                ProducerType.MULTI,
                new BlockingWaitStrategy());
        this.disruptor.handleEventsWith(
                new AccountIngressHandler(commandListener, settlementListener));
        this.disruptor.setDefaultExceptionHandler(new LoggingExceptionHandler());
    }

    private Thread newHandlerThread(Runnable runnable) {
        Thread thread = new Thread(runnable, "account-ingress-handler");
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
    }

    /**
     * 由 poll 线程发布一个批次，立即返回。
     */
    AccountIngressTask submit(AccountIngressKind kind,
                              List<? extends ConsumerRecord<String, ?>> records) {
        RingBuffer<AccountIngressEvent> buffer = ringBuffer;
        if (!running || buffer == null) {
            throw new IllegalStateException("account ingress ring is not running");
        }
        AccountIngressTask task = new AccountIngressTask(kind, records);
        inFlight.add(task);
        CompletableFuture<AccountIngressOutcome> future = task.future();
        future.whenComplete((outcome, error) -> inFlight.remove(task));
        buffer.publishEvent((event, sequence, submitted) -> event.setTask(submitted), task);
        return task;
    }

    /**
     * 由 poll 线程等待批次处理完成，返回业务结果。
     */
    Object await(AccountIngressTask task) {
        try {
            AccountIngressOutcome outcome = task.future()
                    .get(properties.handlerTimeoutMs(), TimeUnit.MILLISECONDS);
            return outcome.result();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "interrupted while waiting for account ingress handler", e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("account ingress handler did not finish within "
                    + properties.handlerTimeoutMs() + "ms", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("account ingress handler failed", cause);
        }
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        disruptor.halt();
        IllegalStateException stopped =
                new IllegalStateException("account ingress ring stopped");
        for (AccountIngressTask task : inFlight) {
            task.future().completeExceptionally(stopped);
        }
        inFlight.clear();
        Thread thread = handlerThread;
        if (thread != null) {
            try {
                thread.join(STOP_JOIN_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
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
            implements ExceptionHandler<AccountIngressEvent> {

        @Override
        public void handleEventException(Throwable ex, long sequence,
                                         AccountIngressEvent event) {
            log.error("account ingress handler failed at sequence {}", sequence, ex);
            // 正常路径由 AccountIngressHandler 自己 complete future；这里兜底，避免等待方悬挂
            AccountIngressTask task = event.task();
            if (task != null) {
                event.setTask(null);
                task.future().completeExceptionally(ex);
            }
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            log.error("account ingress handler start failed", ex);
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            log.error("account ingress handler shutdown failed", ex);
        }
    }
}
