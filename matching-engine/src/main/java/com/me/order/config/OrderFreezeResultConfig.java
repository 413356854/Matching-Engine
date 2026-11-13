package com.me.order.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 冻结回执消费端的订单更新 / 成交清算线程池。
 *
 * <p>{@code OrderFreezeResultListener} 在 Kafka poll 线程内把冻结回执的订单推进与成交清算
 * 提交到该线程池：第一阶段并发推进委托状态，第三阶段并发清算成交并生成行情/结算命令。
 * 撮合引擎调用仍由 poll 线程在引擎对象锁内完成；工作线程不接触 Kafka 事务 producer。
 *
 * <p>线程数默认 8，可用 {@code app.order.freeze-result.worker-threads} 覆盖。
 */
@Configuration(proxyBeanMethods = false)
public class OrderFreezeResultConfig {

    @Bean(name = "orderFreezeResultExecutor", destroyMethod = "shutdown")
    public ExecutorService orderFreezeResultExecutor(
            @Value("${app.order.freeze-result.worker-threads:8}") int workerThreads) {
        int threads = Math.max(1, workerThreads);
        AtomicInteger sequence = new AtomicInteger();
        return Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable,
                    "order-freeze-result-worker-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }
}
