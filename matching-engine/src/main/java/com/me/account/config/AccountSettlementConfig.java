package com.me.account.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 成交清算消费端的账户操作线程池。
 *
 * <p>{@code AccountSettlementListener} 在每个 poll 批次内按 {@code slot(userId)} 把清算单
 * 分成最多 {@link com.me.account.sharding.AccountSharding#SLOTS} 组，提交到该线程池并发
 * 执行；同一 slot 的命令在组内按输入顺序串行。工作线程不接触 Kafka 事务 producer，
 * 资金流水由 poll 线程统一聚合发送。
 *
 * <p>线程数默认 8，可用 {@code app.account.settlement.worker-threads} 覆盖。
 */
@Configuration(proxyBeanMethods = false)
public class AccountSettlementConfig {

    @Bean(name = "accountSettlementExecutor", destroyMethod = "shutdown")
    public ExecutorService accountSettlementExecutor(
            @Value("${app.account.settlement.worker-threads:8}") int workerThreads) {
        int threads = Math.max(1, workerThreads);
        AtomicInteger sequence = new AtomicInteger();
        return Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable,
                    "account-settlement-slot-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }
}
