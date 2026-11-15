package com.me.order.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 委托操作执行线程池，下单 / 冻结回执 / 撤单三条输入链路共用。
 *
 * <p>{@code OrderOperationServiceImpl} 在全局 {@code orderLock} 内持有一个消费批次：
 * 三条链路共用一个批次锁，同一时刻只有一个批次在执行，且一个批次只会用到一条链路的
 * 并发阶段，因此三组任务不会同时占用线程池。合并成一个池后：
 * <ul>
 *   <li>每批的最大并行度是 {@code worker-threads}（默认 16）；</li>
 *   <li>线程总数从三组池（各 8 个）降为一组，空闲线程可被其他链路复用；</li>
 *   <li>锁内没有任务再向其他池提交任务或等待其他池，不存在跨池死锁/饥饿。</li>
 * </ul>
 *
 * <p>线程数默认 16，可用 {@code app.order.operation.worker-threads} 覆盖。
 */
@Configuration(proxyBeanMethods = false)
public class OrderOperationConfig {

    @Bean(name = "orderOperationExecutor", destroyMethod = "shutdown")
    public ExecutorService orderOperationExecutor(
            @Value("${app.order.operation.worker-threads:16}") int workerThreads) {
        int threads = Math.max(1, workerThreads);
        AtomicInteger sequence = new AtomicInteger();
        return Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable,
                    "order-operation-worker-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }
}
