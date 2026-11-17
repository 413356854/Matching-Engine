package com.me.kafka;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * 启动 / 停止所有裸 consumer runner 线程。
 *
 * <p>一个 runner 一个平台线程：默认每个 stage 一个 runner；配置了
 * {@code app.kafka.consumer.concurrency.<stage>} 的 stage 会有多个 runner，它们在同一个
 * consumer group 内按分区分配，从而并行消费。{@code app.kafka.enabled=false} 时不创建
 * （{@link KafkaConsumerConfiguration} 同时关闭）。
 */
@Component
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaConsumerManager implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerManager.class);

    private final List<KafkaConsumerRunner<?>> runners;
    private final AtomicBoolean running = new AtomicBoolean();
    private ExecutorService executor;

    public KafkaConsumerManager(KafkaConsumerRunners runners, FatalState fatalState) {
        this.runners = runners.runners();
        fatalState.setShutdownAction(this::stopAllRunners);
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        if (runners.isEmpty()) {
            return;
        }
        executor = Executors.newFixedThreadPool(runners.size(), task -> {
            Thread thread = new Thread(task, "kafka-consumer");
            thread.setDaemon(true);
            return thread;
        });
        for (KafkaConsumerRunner<?> runner : runners) {
            executor.submit(() -> {
                Thread.currentThread().setName("kafka-consumer-" + runner.name());
                runner.run();
            });
        }
    }

    /**
     * 只给全部 runner 发停止信号，不等待线程结束。
     *
     * <p>fatal 时可能由某个 runner 线程调用本方法，不能在当前线程等待自己退出；
     * 各 runner 会在 {@code run()} 的 finally 中关闭自己的 client。
     */
    private void stopAllRunners() {
        for (KafkaConsumerRunner<?> runner : runners) {
            runner.stop();
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        stopAllRunners();
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.warn("kafka consumer runners did not stop within 30s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executor = null;
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return 100;
    }
}
