package com.me.account.ingress;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 一次 poll 批次的投递句柄。
 *
 * <p>poll 线程创建并发布到 ring；handler 线程处理后 complete future；poll 线程等待同一个
 * future。records 只由 handler 读取。结果类型由 {@link AccountIngressKind} 决定。
 */
final class AccountIngressTask {

    private final AccountIngressKind kind;
    private final List<? extends ConsumerRecord<String, ?>> records;
    private final CompletableFuture<AccountIngressOutcome> future = new CompletableFuture<>();

    AccountIngressTask(AccountIngressKind kind,
                       List<? extends ConsumerRecord<String, ?>> records) {
        this.kind = kind;
        this.records = records;
    }

    AccountIngressKind kind() {
        return kind;
    }

    List<? extends ConsumerRecord<String, ?>> records() {
        return records;
    }

    CompletableFuture<AccountIngressOutcome> future() {
        return future;
    }
}
