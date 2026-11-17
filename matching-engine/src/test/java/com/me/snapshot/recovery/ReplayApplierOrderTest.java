package com.me.snapshot.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.me.account.service.AccountOperationService;
import com.me.currency.vo.CurrencyView;
import com.me.currency.vo.TradingPairView;
import com.me.kafka.FatalState;
import com.me.kafka.KafkaClientProperties;
import com.me.matching.service.MatchingEngine;
import com.me.order.mq.MatchPairEventPublisher;
import com.me.order.mq.OrderFreezeCommandPublisher;
import com.me.order.mq.OrderHistoryPublisher;
import com.me.order.mq.OrderOffsetSeqPublisher;
import com.me.order.mq.TradeSettlementCommandPublisher;
import com.me.order.service.OrderBatchLock;
import com.me.order.service.OrderConfigCache;
import com.me.order.service.OrderEntryLocks;
import com.me.order.service.impl.OrderMatchingServiceImpl;
import com.me.order.service.impl.OrderOperationServiceImpl;
import com.me.order.service.impl.OrderServiceImpl;
import com.me.shared.order.OrderCommand;
import com.me.shared.order.OrderCommandBatch;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderStatus;
import com.me.shared.order.OrderType;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class ReplayApplierOrderTest {

    private static final String ORDER_COMMANDS = "order-commands";

    @Test
    void replaysOrderCommandBatchAndVerifiesAssignedSeq() {
        Fixture fixture = new Fixture();
        try {
            ReplayMapping mapping = new ReplayMapping(ORDER_COMMANDS, 0, 5L, 1L);
            stubFetch(fixture, List.of(mapping), orderCommandRecord(5L, 1L, "req-1"));

            ReplayApplier.ReplayStats stats = fixture.applier.replay(new ReplayMappings(
                    List.of(new ReplayBatch(ORDER_COMMANDS, List.of(mapping))), List.of()));

            assertEquals(1L, stats.orderBatches());
            assertEquals(1L, stats.orderRecords());
            assertEquals(1L, fixture.operations.currentSeq());
            assertNotNull(fixture.orderService.view(1L));
            assertEquals(OrderStatus.ACCEPTED, fixture.orderService.view(1L).status());
        } finally {
            fixture.close();
        }
    }

    @Test
    void acceptsGapBetweenSnapshotWatermarkAndFirstCommittedSeq() {
        Fixture fixture = new Fixture();
        try {
            ReplayMapping mapping = new ReplayMapping(ORDER_COMMANDS, 0, 9L, 5L);
            stubFetch(fixture, List.of(mapping), orderCommandRecord(9L, 2L, "req-2"));

            fixture.applier.replay(new ReplayMappings(
                    List.of(new ReplayBatch(ORDER_COMMANDS, List.of(mapping))), List.of()));

            assertEquals(5L, fixture.operations.currentSeq());
            assertEquals(OrderStatus.ACCEPTED, fixture.orderService.view(2L).status());
        } finally {
            fixture.close();
        }
    }

    @Test
    void rejectsInputPositionMismatch() {
        Fixture fixture = new Fixture();
        try {
            ReplayMapping mapping = new ReplayMapping(ORDER_COMMANDS, 0, 5L, 1L);
            // 映射要求 offset=5，抓取返回 offset=6：必须拒绝，不能把错误状态重放进去。
            stubFetch(fixture, List.of(mapping), orderCommandRecord(6L, 1L, "req-1"));

            assertThrows(com.me.snapshot.SnapshotRecoveryException.class,
                    () -> fixture.applier.replay(new ReplayMappings(
                            List.of(new ReplayBatch(ORDER_COMMANDS, List.of(mapping))),
                            List.of())));
        } finally {
            fixture.close();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void stubFetch(Fixture fixture,
                                  List<ReplayMapping> mappings,
                                  ConsumerRecord<String, OrderCommandBatch> record) {
        when(fixture.fetcher.fetch(eq(ORDER_COMMANDS), anyList()))
                .thenReturn((List) List.of(record));
    }

    private static ConsumerRecord<String, OrderCommandBatch> orderCommandRecord(
            long offset, long orderId, String requestId) {
        OrderCommand command = new OrderCommand(
                requestId, orderId, 1L, "BTC-USDT",
                OrderSide.BUY, OrderType.LIMIT, 10L, 10L);
        OrderCommandBatch batch = new OrderCommandBatch(List.of(command));
        return new ConsumerRecord<>(ORDER_COMMANDS, 0, offset, "BTC-USDT", batch);
    }

    private static final class Fixture implements AutoCloseable {

        private final ExecutorService executor = Executors.newFixedThreadPool(2);
        private final OrderServiceImpl orderService = new OrderServiceImpl();
        private final OrderOperationServiceImpl operations;
        private final ReplayInputFetcher fetcher = mock(ReplayInputFetcher.class);
        private final ReplayApplier applier;

        private Fixture() {
            MatchingEngine matchingEngine = new MatchingEngine();
            OrderConfigCache configCache = mock(OrderConfigCache.class);
            when(configCache.pair("BTC-USDT")).thenReturn(new TradingPairView(
                    1L, "BTC-USDT", "BTC", "USDT", 0, 0, 0L, 0L, 0, 0, 1, 0));
            when(configCache.currency("USDT")).thenReturn(new CurrencyView(
                    1L, "USDT", "Tether", 0, 1, 0));
            this.operations = new OrderOperationServiceImpl(
                    orderService,
                    configCache,
                    new OrderMatchingServiceImpl(matchingEngine, orderService),
                    matchingEngine,
                    executor,
                    new OrderBatchLock(),
                    new OrderEntryLocks(),
                    new OrderFreezeCommandPublisher("freeze"),
                    new MatchPairEventPublisher("match-pair"),
                    new TradeSettlementCommandPublisher("settlements"),
                    new OrderHistoryPublisher("history"),
                    new OrderOffsetSeqPublisher("offset-seq"),
                    new FatalState());
            KafkaClientProperties kafkaProperties = kafkaProperties();
            this.applier = new ReplayApplier(
                    operations, mock(AccountOperationService.class), fetcher, kafkaProperties);
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
                            "order-commands", ORDER_COMMANDS,
                            "order-freeze-results", "order-freeze-results",
                            "order-cancels", "order-cancels"),
                    Map.of(), new KafkaClientProperties.Topic(8, 1));
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }
}
