package com.me.kafka;

import com.me.account.dto.FundsCommand;
import com.me.account.dto.OrderFreezeCommandsBatch;
import com.me.account.dto.TradeSettlementBatch;
import com.me.account.mq.AccountCommandListener;
import com.me.account.mq.AccountFreezeCommandListener;
import com.me.account.mq.AccountSettlementListener;
import com.me.admin.mq.FundsFlowConsumer;
import com.me.admin.mq.OrderHistoryConsumer;
import com.me.admin.mq.UserTradeConsumer;
import com.me.marketdata.mq.MarketDataListener;
import com.me.order.mq.OrderCancelListener;
import com.me.order.mq.OrderCommandListener;
import com.me.order.mq.OrderFreezeResultListener;
import com.me.shared.event.FundsFlowBatch;
import com.me.shared.event.MatchPairBatch;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 十条消费链路的 runner 装配：topic / group / 目标类型 / 处理函数。
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
 * <p>账户的三条输入链路（account-command / account-order-freeze / account-settlement）
 * 在模块锁内完成业务并分配 seq，输出消息收集成 {@link SendPackage} 后交给
 * {@code account-committer}，由唯一 committer 线程按 FIFO 顺序统一发送并提交 offset。
 * 委托的三条输入链路（order-command / order-freeze-result / order-cancel）同理，使用
 * {@code order-committer}；批内保留各链路线程池并发。队列满时 runner 阻塞在入队，
 * 形成背压。
 */
@Configuration
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaConsumerConfiguration {

    private final Set<String> disabledStages;
    private final int fundsFlowMaxPollRecords;
    private final int fundsFlowFetchMaxWaitMs;
    private final FatalState fatalState;
    private final ModuleCommitPipeline orderCommitPipeline;
    private final ModuleCommitPipeline accountCommitPipeline;

    @Autowired
    public KafkaConsumerConfiguration(
            @Value("${app.kafka.disabled-consumers:}") String disabledConsumers,
            @Value("${app.kafka.consumer.funds-flow.max-poll-records:100}")
            int fundsFlowMaxPollRecords,
            @Value("${app.kafka.consumer.funds-flow.fetch-max-wait-ms:200}")
            int fundsFlowFetchMaxWaitMs,
            FatalState fatalState,
            @Qualifier("orderCommitPipeline") ModuleCommitPipeline orderCommitPipeline,
            @Qualifier("accountCommitPipeline") ModuleCommitPipeline accountCommitPipeline) {
        this.disabledStages = Arrays.stream(disabledConsumers.split(","))
                .map(String::trim)
                .filter(stage -> !stage.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
        this.fundsFlowMaxPollRecords = Math.max(1, fundsFlowMaxPollRecords);
        this.fundsFlowFetchMaxWaitMs = Math.max(0, fundsFlowFetchMaxWaitMs);
        this.fatalState = fatalState;
        this.orderCommitPipeline = orderCommitPipeline;
        this.accountCommitPipeline = accountCommitPipeline;
    }

    @Bean
    public KafkaConsumerRunners kafkaConsumerRunners(
            KafkaClientProperties properties,
            KafkaClientFactory clientFactory,
            AccountCommandListener accountCommandListener,
            AccountFreezeCommandListener accountFreezeCommandListener,
            AccountSettlementListener accountSettlementListener,
            OrderCommandListener orderCommandListener,
            OrderFreezeResultListener orderFreezeResultListener,
            OrderCancelListener orderCancelListener,
            MarketDataListener marketDataListener,
            FundsFlowConsumer fundsFlowConsumer,
            OrderHistoryConsumer orderHistoryConsumer,
            UserTradeConsumer userTradeConsumer) {
        List<KafkaConsumerRunner<?>> all = new ArrayList<>();

        all.addAll(createRunners(properties, clientFactory,
                "account-command", "account-commands", "account",
                FundsCommand.class, false, accountCommandListener::onCommand));

        // ORDER_FREEZE 独立链路：每个 symbol 一条批量消息，key=symbol，按分区消费；
        // 同一 symbol 仍由单 consumer 串行。这里固定走直连 listener，不接入 account
        // ingress ring；批次业务统一由 AccountOperationService 的全局锁串行执行。
        all.addAll(createRunners(properties, clientFactory,
                "account-order-freeze", "account-order-freeze-commands", "account-order-freeze",
                OrderFreezeCommandsBatch.class, true,
                accountFreezeCommandListener::onFreezeCommandBatch));

        all.addAll(createRunners(properties, clientFactory,
                "account-settlement", "account-settlements", "account-settlement",
                TradeSettlementBatch.class, true, accountSettlementListener::onSettlement));

        // 下单入口已改为批量消息：每条 record 是一个 OrderCommandBatch（≤2000 条命令），
        // 使用 batch-max-poll-records，避免一个事务处理过多元素。批次业务由
        // OrderOperationService 全局锁串行，批内仍逐条提交共享的 orderOperationExecutor。
        all.addAll(createRunners(properties, clientFactory,
                "order-command", "order-commands", "order",
                OrderCommandBatch.class, true, orderCommandListener::onCommand));

        // 冻结回执批次业务同样由 OrderOperationService 全局锁串行；批内先并发更新委托，
        // 再在引擎对象锁内聚合撮合、并发清算。
        all.addAll(createRunners(properties, clientFactory,
                "order-freeze-result", "order-freeze-results", "order-freeze-result",
                OrderFreezeResultBatch.class, true, orderFreezeResultListener::onFreezeResult));

        // 撤单链路与下单链路一致：批量消息按 symbol 分区，同一 symbol 的撤单由单 consumer
        // 按序处理；批内撤单交给线程池并发查询 / 更新委托，批次业务同样在全局锁内串行。
        all.addAll(createRunners(properties, clientFactory,
                "order-cancel", "order-cancels", "order-cancel",
                OrderCancelBatch.class, true, orderCancelListener::onCancel));

        // 撮合对 topic 有两个独立消费组：行情模块聚合 K 线，admin 归档用户成交记录。
        all.addAll(createRunners(properties, clientFactory,
                "marketdata", "match-pair", "marketdata",
                MatchPairBatch.class, true,
                (records, output) -> marketDataListener.onMatchPair(records)));

        all.addAll(createRunners(properties, clientFactory,
                "user-trade", "match-pair", "user-trade",
                MatchPairBatch.class, true, (records, output) -> userTradeConsumer.onMatchPair(records)));

        all.addAll(createRunners(properties, clientFactory,
                "funds-flow", "funds-flow", "admin",
                FundsFlowBatch.class, true, (records, output) -> fundsFlowConsumer.onFundsFlow(records)));

        // 委托历史按 symbol 分区：8 个 consumer 并行归档 QuestDB，同一 symbol 由单 consumer
        // 串行处理。历史事件由委托模块在撤单 / 成交清算的同一 Kafka 事务内产出。
        all.addAll(createRunners(properties, clientFactory,
                "order-history", "order-history", "order-history",
                OrderHistoryBatch.class, true, (records, output) -> orderHistoryConsumer.onOrderHistory(records)));

        return new KafkaConsumerRunners(all);
    }

    private <T> List<KafkaConsumerRunner<T>> createRunners(
            KafkaClientProperties properties,
            KafkaClientFactory clientFactory,
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
                    type, processor, properties, clientFactory, fatalState,
                    commitPipelineFor(stage), maxPollRecords, fetchMaxWaitMs));
        }
        return runners;
    }

    /**
     * 有状态 stage 使用的模块提交管线；审计 / 投影 stage 不修改委托或账户状态，返回 null。
     *
     * <p>同一模块的下单、冻结回执、撤单 / 充值、冻结、结算共用一条 FIFO 队列和一个
     * committer，保证跨链路提交顺序与 seq 顺序一致。
     */
    private ModuleCommitPipeline commitPipelineFor(String stage) {
        return switch (stage) {
            case "order-command", "order-freeze-result", "order-cancel" ->
                    orderCommitPipeline;
            case "account-command", "account-order-freeze", "account-settlement" ->
                    accountCommitPipeline;
            default -> null;
        };
    }
}
