package com.me.order.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 下单消费端的订单保存线程池。
 *
 * <p>{@code OrderCommandListener} 在 Kafka poll 线程内把订单提交到该线程池并发保存，
 * 生成冻结命令后再由 poll 线程按原命令顺序聚合发送 {@code orderFreezeBatch}。工作线程
 * 不接触 Kafka 事务 producer；线程数默认 8，可用
 * {@code app.order.command.worker-threads} 覆盖。
 */
@Configuration(proxyBeanMethods = false)
public class OrderCommandConfig {

    @Bean(name = "orderCommandExecutor", destroyMethod = "shutdown")
    public ExecutorService orderCommandExecutor(
            @Value("${app.order.command.worker-threads:8}") int workerThreads) {
        int threads = Math.max(1, workerThreads);
        AtomicInteger sequence = new AtomicInteger();
        return Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable,
                    "order-command-worker-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }
}
