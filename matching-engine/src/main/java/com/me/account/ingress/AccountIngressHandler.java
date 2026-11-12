package com.me.account.ingress;

import com.lmax.disruptor.EventHandler;
import com.me.account.dto.FundsCommand;
import com.me.account.dto.TradeSettlementBatch;
import com.me.account.mq.AccountCommandListener;
import com.me.account.mq.AccountSettlementListener;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 唯一的 account 业务处理线程：按 ring 顺序串行执行两条链路的批次。
 *
 * <p>每个事件处理完必须 complete future（成功或异常），否则 poll 线程会一直等待。
 * 业务异常只写入 future，不向 Disruptor 抛出，保证 handler 线程不会因单批失败退出。
 */
final class AccountIngressHandler implements EventHandler<AccountIngressEvent> {

    private final AccountCommandListener commandListener;
    private final AccountSettlementListener settlementListener;

    AccountIngressHandler(AccountCommandListener commandListener,
                          AccountSettlementListener settlementListener) {
        this.commandListener = commandListener;
        this.settlementListener = settlementListener;
    }

    @Override
    public void onEvent(AccountIngressEvent event, long sequence, boolean endOfBatch) {
        AccountIngressTask task = event.task();
        event.setTask(null);
        Object result = null;
        Throwable failure = null;
        try {
            result = switch (task.kind()) {
                case COMMAND -> commandListener.process(commandRecords(task));
                case SETTLEMENT -> settlementListener.process(settlementRecords(task));
            };
        } catch (Throwable t) {
            failure = t;
        } finally {
            if (failure != null) {
                task.future().completeExceptionally(failure);
            } else {
                task.future().complete(new AccountIngressOutcome(result));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static List<ConsumerRecord<String, FundsCommand>> commandRecords(
            AccountIngressTask task) {
        return (List<ConsumerRecord<String, FundsCommand>>) (List<?>) task.records();
    }

    @SuppressWarnings("unchecked")
    private static List<ConsumerRecord<String, TradeSettlementBatch>> settlementRecords(
            AccountIngressTask task) {
        return (List<ConsumerRecord<String, TradeSettlementBatch>>) (List<?>) task.records();
    }
}
