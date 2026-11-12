package com.me.account.ingress;

import com.me.account.dto.FundsCommand;
import com.me.account.mq.AccountCommandListener;
import com.me.kafka.PrefetchingBatchProcessor;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * account-command 链路的两阶段 processor。
 *
 * <p>{@link #submit(List)} 在 poll 线程把批次投进 ring；{@link #await()} 在 poll 线程等
 * handler 处理完，再在当前线程的 Kafka 事务内发送输出。
 *
 * <p>{@code pending} 只在 poll 线程访问。
 */
public final class AccountCommandRingProcessor implements PrefetchingBatchProcessor<FundsCommand> {

    private final AccountIngressRing ring;
    private final AccountCommandListener listener;

    private AccountIngressTask pending;

    public AccountCommandRingProcessor(AccountIngressRing ring,
                                       AccountCommandListener listener) {
        this.ring = ring;
        this.listener = listener;
    }

    @Override
    public void submit(List<ConsumerRecord<String, FundsCommand>> records) {
        pending = ring.submit(AccountIngressKind.COMMAND, records);
    }

    @Override
    public void await() {
        AccountIngressTask task = pending;
        pending = null;
        if (task == null) {
            throw new IllegalStateException("submit must be called before await");
        }
        AccountCommandListener.AccountCommandBatch batch =
                (AccountCommandListener.AccountCommandBatch) ring.await(task);
        listener.publish(batch);
    }
}
