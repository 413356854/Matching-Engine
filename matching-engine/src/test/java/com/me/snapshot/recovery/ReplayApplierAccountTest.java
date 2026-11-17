package com.me.snapshot.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.me.account.dto.FundsCommand;
import com.me.account.mq.AccountOffsetSeqPublisher;
import com.me.account.mq.FundsFlowPublisher;
import com.me.account.mq.OrderFreezeResultPublisher;
import com.me.account.service.AccountBatchLock;
import com.me.account.service.AccountEntryLock;
import com.me.account.service.impl.AccountOperationServiceImpl;
import com.me.account.service.impl.AccountServiceImpl;
import com.me.kafka.FatalState;
import com.me.kafka.KafkaClientProperties;
import com.me.order.service.OrderOperationService;
import com.me.shared.event.FundsLogType;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class ReplayApplierAccountTest {

    private static final String ACCOUNT_COMMANDS = "account-commands";

    @Test
    void replaysAccountCommandBatchAndVerifiesAssignedSeq() {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            AccountServiceImpl accountService = new AccountServiceImpl();
            AccountOperationServiceImpl operations = new AccountOperationServiceImpl(
                    accountService,
                    executor,
                    new AccountBatchLock(),
                    new AccountEntryLock(),
                    new FundsFlowPublisher("funds", 8),
                    new OrderFreezeResultPublisher("freeze-results"),
                    new AccountOffsetSeqPublisher("account-offset-seq"),
                    new FatalState());

            ReplayInputFetcher fetcher = mock(ReplayInputFetcher.class);
            FundsCommand command = new FundsCommand(
                    "req-1", 1L, "USDT", FundsLogType.DEPOSIT, 100L, 0L);
            ConsumerRecord<String, FundsCommand> record =
                    new ConsumerRecord<>(ACCOUNT_COMMANDS, 0, 0L, "1", command);
            ReplayMapping mapping = new ReplayMapping(ACCOUNT_COMMANDS, 0, 0L, 1L);
            stubFetch(fetcher, record);

            ReplayApplier applier = new ReplayApplier(
                    mock(OrderOperationService.class), operations, fetcher,
                    kafkaProperties());
            ReplayApplier.ReplayStats stats = applier.replay(new ReplayMappings(
                    List.of(),
                    List.of(new ReplayBatch(ACCOUNT_COMMANDS, List.of(mapping)))));

            assertEquals(1L, stats.accountBatches());
            assertEquals(1L, stats.accountRecords());
            assertEquals(1L, operations.currentSeq());
            assertEquals(100L, accountService.view(1L, "USDT").available());
        } finally {
            executor.shutdownNow();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void stubFetch(ReplayInputFetcher fetcher,
                                  ConsumerRecord<String, FundsCommand> record) {
        when(fetcher.fetch(eq(ACCOUNT_COMMANDS), anyList()))
                .thenReturn((List) List.of(record));
    }

    private static KafkaClientProperties kafkaProperties() {
        KafkaClientProperties.Consumer consumer = new KafkaClientProperties.Consumer(
                false, "read_committed", "earliest", false,
                2000, 10, 50, 1024, 600000, 45000, Map.of());
        KafkaClientProperties.Producer producer = new KafkaClientProperties.Producer(
                "all", true, 180000, 5000, 120000, 2, 32768);
        return new KafkaClientProperties(
                true, "localhost:9092", "test", "tx-", 1000, 1000,
                32, 3000, 3000, consumer, producer,
                new KafkaClientProperties.Admin(true, false),
                Map.of(
                        "account-commands", ACCOUNT_COMMANDS,
                        "account-order-freeze-commands", "account-order-freeze-commands",
                        "account-settlements", "account-settlements"),
                Map.of(), new KafkaClientProperties.Topic(8, 1));
    }
}
