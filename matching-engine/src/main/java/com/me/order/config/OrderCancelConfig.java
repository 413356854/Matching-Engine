package com.me.order.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 撤单消费端的订单查询 / 更新线程池。
 *
 * <p>{@code OrderCancelListener} 在 Kafka poll 线程内把撤单任务提交到该线程池：第一阶段
 * 并发查询委托并生成撮合撤消单，第三阶段并发推进委托状态并生成解冻清算。Kafka 发送
 * 仍由 poll 线程完成，工作线程不接触事务 producer。
 *
 * <p>工作线程数默认 8，可用 {@code app.order.cancel.worker-threads} 覆盖；线程为守护线程，
 * 应用停机时由 Spring 调用 {@code shutdown()}。
 */
@Configuration(proxyBeanMethods = false)
public class OrderCancelConfig {

    @Bean(name = "orderCancelExecutor", destroyMethod = "shutdown")
    public ExecutorService orderCancelExecutor(
            @Value("${app.order.cancel.worker-threads:8}") int workerThreads) {
        int threads = Math.max(1, workerThreads);
        AtomicInteger sequence = new AtomicInteger();
        return Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable,
                    "order-cancel-worker-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }
}
