package com.me.account.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 账户操作执行线程池，三条账户输入链路共用。
 *
 * <p>{@code AccountOperationServiceImpl} 在账户全局锁内持有一个消费批次，把批内命令按
 * {@code slot(userId)} 分成最多 {@link com.me.account.sharding.AccountSharding#SLOTS} 组
 * 提交到该线程池并发执行；同一 slot 的命令在组内按输入顺序串行。工作线程不接触 Kafka
 * 事务 producer，资金流水、冻结回执与 offset-seq 映射由 poll 线程统一发送。
 *
 * <p>线程数默认 8，可用 {@code app.account.operation.worker-threads} 覆盖。
 */
@Configuration(proxyBeanMethods = false)
public class AccountOperationConfig {

    @Bean(name = "accountOperationExecutor", destroyMethod = "shutdown")
    public ExecutorService accountOperationExecutor(
            @Value("${app.account.operation.worker-threads:8}") int workerThreads) {
        int threads = Math.max(1, workerThreads);
        AtomicInteger sequence = new AtomicInteger();
        return Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable,
                    "account-operation-slot-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }
}
