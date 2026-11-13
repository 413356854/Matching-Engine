package com.me.kafka;

import com.me.account.dto.FundsCommand;
import com.me.account.dto.OrderFreezeCommandsBatch;
import com.me.account.dto.TradeSettlementBatch;
import com.me.account.mq.AccountCommandListener;
import com.me.account.mq.AccountFreezeCommandListener;
import com.me.account.mq.AccountSettlementListener;
import com.me.admin.mq.FundsFlowConsumer;
import com.me.admin.mq.OrderHistoryConsumer;
import com.me.marketdata.mq.MarketDataListener;
import com.me.order.mq.OrderCancelListener;
import com.me.order.mq.OrderCommandListener;
import com.me.order.mq.OrderFreezeResultListener;
import com.me.shared.event.FundsFlowBatch;
import com.me.shared.event.MarketDataBatch;
import com.me.shared.event.OrderFreezeResultBatch;
import com.me.shared.order.OrderCancelBatch;
import com.me.shared.order.OrderCommandBatch;
import com.me.shared.order.OrderHistoryBatch;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 九条消费链路的 runner 装配：topic / group / 目标类型 / 处理函数。
 *
 * <p>topic 与 group 名称必须与 {@code app.kafka.topics} / {@code app.kafka.groups} 保持一致
 * （loadtest 和已有消费组 offset 都依赖它们）。
 *
 * <p><b>并发</b>：{@code app.kafka.consumer.concurrency.<stage>} 决定该 stage 起几个
 * consumer（默认 1）。多个 consumer 属于同一 group，Kafka 按分区分配，因此并发上限是
 * topic 的分区数；同一 key 始终由同一个 consumer 处理，key 内的顺序不变。
 * {@code order-command}、{@code order-cancel}、{@code account-order-freeze}、
 * {@code order-freeze-result} 都按 symbol 分区，可以按分区并行；同一 symbol 仍在单分区内
 * 按序处理。
 *
 * <p><b>poll 条数</b>：逐条 topic（{@code me.account.commands}）使用全局
 * {@code max-poll-records}；批量 topic（含 {@code me.order.commands-batch}）的一条 record
 * 内含 {@code msgList}，使用更小的 {@code batch-max-poll-records}，避免一次 poll
 * 把过多元素带进同一个事务。
 *
 * <p><b>诊断</b>：{@code app.kafka.disabled-consumers} 是逗号分隔的 stage 列表，
 * 命中的 stage 不创建 runner，用于逐段压测定位吞吐拐点。stage 名称与 bean 中的
 * {@code order-command} / {@code account-order-freeze} 等一致。
 *
 * <p>account-command / account-settlement 两条链路固定走 poll 线程直连 listener：
 * 业务在 poll 线程内完成后，输出在同一个 Kafka 事务里发送。
 */
@Configuration
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaConsumerConfiguration {

    private final Set<String> disabledStages;
    private final int fundsFlowMaxPollRecords;
    private final int fundsFlowFetchMaxWaitMs;

    /** 测试用默认值：funds-flow 拉大 poll 批次。 */
    public KafkaConsumerConfiguration(String disabledConsumers) {
        this(disabledConsumers, 100, 200);
    }

    @Autowired
    public KafkaConsumerConfiguration(
            @Value("${app.kafka.disabled-consumers:}") String disabledConsumers,
            @Value("${app.kafka.consumer.funds-flow.max-poll-records:100}")
            int fundsFlowMaxPollRecords,
            @Value("${app.kafka.consumer.funds-flow.fetch-max-wait-ms:200}")
            int fundsFlowFetchMaxWaitMs) {
        this.disabledStages = Arrays.stream(disabledConsumers.split(","))
                .map(String::trim)
                .filter(stage -> !stage.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
        this.fundsFlowMaxPollRecords = Math.max(1, fundsFlowMaxPollRecords);
        this.fundsFlowFetchMaxWaitMs = Math.max(0, fundsFlowFetchMaxWaitMs);
    }

    @Bean
    public KafkaConsumerRunners kafkaConsumerRunners(
            KafkaClientProperties properties,
            KafkaClientFactory clientFactory,
            KafkaSink sink,
            AccountCommandListener accountCommandListener,
            AccountFreezeCommandListener accountFreezeCommandListener,
            AccountSettlementListener accountSettlementListener,
            OrderCommandListener orderCommandListener,
            OrderFreezeResultListener orderFreezeResultListener,
            OrderCancelListener orderCancelListener,
            MarketDataListener marketDataListener,
            FundsFlowConsumer fundsFlowConsumer,
            OrderHistoryConsumer orderHistoryConsumer) {
        List<KafkaConsumerRunner<?>> all = new ArrayList<>();

        all.addAll(createRunners(properties, clientFactory, sink,
                "account-command", "account-commands", "account",
                FundsCommand.class, false, accountCommandListener::onCommand));

        // ORDER_FREEZE 独立链路：每个 symbol 一条批量消息，key=symbol，按分区并行；
        // 同一 symbol 仍由单 consumer 串行。这里固定走直连 listener，不接入 account
        // ingress ring（ring 是单 handler 的，会把 symbol 级并行重新串行化）。
        all.addAll(createRunners(properties, clientFactory, sink,
                "account-order-freeze", "account-order-freeze-commands", "account-order-freeze",
                OrderFreezeCommandsBatch.class, true,
                accountFreezeCommandListener::onFreezeCommandBatch));

        all.addAll(createRunners(properties, clientFactory, sink,
                "account-settlement", "account-settlements", "account-settlement",
                TradeSettlementBatch.class, true, accountSettlementListener::onSettlement));

        // 下单入口已改为批量消息：每条 record 是一个 OrderCommandBatch（≤2000 条命令），
        // 使用 batch-max-poll-records，避免一个事务处理过多元素。
        all.addAll(createRunners(properties, clientFactory, sink,
                "order-command", "order-commands", "order",
                OrderCommandBatch.class, true, orderCommandListener::onCommand));

        all.addAll(createRunners(properties, clientFactory, sink,
                "order-freeze-result", "order-freeze-results", "order-freeze-result",
                OrderFreezeResultBatch.class, true, orderFreezeResultListener::onFreezeResult));

        // 撤单链路与下单链路一致：批量消息按 symbol 分区，同一 symbol 的撤单由单 consumer
        // 按序处理；消费端再把批内撤单交给线程池并发查询 / 更新委托。
        all.addAll(createRunners(properties, clientFactory, sink,
                "order-cancel", "order-cancels", "order-cancel",
                OrderCancelBatch.class, true, orderCancelListener::onCancel));

        all.addAll(createRunners(properties, clientFactory, sink,
                "marketdata", "market-data", "marketdata",
                MarketDataBatch.class, true, marketDataListener::onMarketData));

        all.addAll(createRunners(properties, clientFactory, sink,
                "funds-flow", "funds-flow", "admin",
                FundsFlowBatch.class, true, fundsFlowConsumer::onFundsFlow));

        // 委托历史按 symbol 分区：8 个 consumer 并行归档 QuestDB，同一 symbol 由单 consumer
        // 串行处理。历史事件由委托模块在撤单 / 成交清算的同一 Kafka 事务内产出。
        all.addAll(createRunners(properties, clientFactory, sink,
                "order-history", "order-history", "order-history",
                OrderHistoryBatch.class, true, orderHistoryConsumer::onOrderHistory));

        return new KafkaConsumerRunners(all);
    }

    private <T> List<KafkaConsumerRunner<T>> createRunners(
            KafkaClientProperties properties,
            KafkaClientFactory clientFactory,
            KafkaSink sink,
            String stage,
            String topicKey,
            String groupKey,
            Class<T> type,
            boolean batchTopic,
            KafkaBatchProcessor<T> processor) {
        if (disabledStages.contains(stage)) {
            return List.of();
        }
        int concurrency = properties.consumer().concurrencyFor(stage);
        // funds-flow 写 QuestDB 的固定开销按批发生：拉大 poll 批次、延长 fetch 等待，
        // 让一次 poll 带更多 record，配合归档端的 20k 分块摊薄 QWP ack 与 commit。
        boolean fundsFlow = "funds-flow".equals(stage);
        int maxPollRecords = fundsFlow
                ? fundsFlowMaxPollRecords
                : properties.consumer().maxPollRecordsFor(batchTopic);
        int fetchMaxWaitMs = fundsFlow
                ? fundsFlowFetchMaxWaitMs
                : properties.consumer().fetchMaxWaitMs();
        List<KafkaConsumerRunner<T>> runners = new ArrayList<>(concurrency);
        for (int index = 0; index < concurrency; index++) {
            // name 必须唯一：它同时进入 client.id 和 transactional.id，同名的两个事务
            // producer 会互相 fence，导致 initTransactions / commit 失败。
            String name = concurrency == 1 ? stage : stage + "-" + index;
            runners.add(new KafkaConsumerRunner<>(name,
                    properties.topics().get(topicKey),
                    properties.groups().get(groupKey),
                    type, processor, sink, properties, clientFactory,
                    maxPollRecords, fetchMaxWaitMs));
        }
        return runners;
    }
}
