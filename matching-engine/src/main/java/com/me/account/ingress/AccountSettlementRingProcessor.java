package com.me.account.ingress;

import com.me.account.dto.TradeSettlementBatch;
import com.me.account.mq.AccountSettlementListener;
import com.me.kafka.PrefetchingBatchProcessor;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * account-settlement 链路的两阶段 processor。
 *
 * <p>与 {@link AccountCommandRingProcessor} 对称；结算被拒绝时 {@code await()} 会重抛
 * {@link IllegalStateException}，由 {@code KafkaConsumerRunner} abort 并整批重投。
 */
public final class AccountSettlementRingProcessor
        implements PrefetchingBatchProcessor<TradeSettlementBatch> {

    private final AccountIngressRing ring;
    private final AccountSettlementListener listener;

    private AccountIngressTask pending;

    public AccountSettlementRingProcessor(AccountIngressRing ring,
                                          AccountSettlementListener listener) {
        this.ring = ring;
        this.listener = listener;
    }

    @Override
    public void submit(List<ConsumerRecord<String, TradeSettlementBatch>> records) {
        pending = ring.submit(AccountIngressKind.SETTLEMENT, records);
    }

    @Override
    public void await() {
        AccountIngressTask task = pending;
        pending = null;
        if (task == null) {
            throw new IllegalStateException("submit must be called before await");
        }
        AccountSettlementListener.AccountSettlementBatch batch =
                (AccountSettlementListener.AccountSettlementBatch) ring.await(task);
        listener.publish(batch);
    }
}
