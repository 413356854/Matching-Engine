package com.me.order.service.impl;

import com.me.account.dto.FundsCommand;
import com.me.account.dto.TradeSettlementCommand;
import com.me.currency.vo.CurrencyView;
import com.me.currency.vo.TradingPairView;
import com.me.kafka.BatchOutput;
import com.me.kafka.FatalState;
import com.me.matching.dto.CancelResult;
import com.me.matching.dto.MatchResult;
import com.me.matching.service.MatchingEngine;
import com.me.order.dto.FreezeCommand;
import com.me.order.dto.OrderApplyResult;
import com.me.order.dto.OrderCancelApplyResult;
import com.me.order.dto.OrderCancelContext;
import com.me.order.dto.OrderCancelMatchResult;
import com.me.order.dto.OrderCancelPrepareResult;
import com.me.order.dto.OrderFreezeApplyResult;
import com.me.order.dto.OrderMatchResult;
import com.me.order.dto.OrderOffsetSeq;
import com.me.order.dto.OrderOperationResult;
import com.me.order.mq.MatchPairEventPublisher;
import com.me.order.mq.OrderFreezeCommandPublisher;
import com.me.order.mq.OrderHistoryPublisher;
import com.me.order.mq.OrderOffsetSeqPublisher;
import com.me.order.mq.TradeSettlementCommandPublisher;
import com.me.order.service.OrderConfigCache;
import com.me.order.service.OrderMatchingService;
import com.me.order.service.OrderOperationService;
import com.me.order.service.OrderService;
import com.me.shared.event.MatchPairEvent;
import com.me.order.service.OrderBatchLock;
import com.me.order.service.OrderEntryLocks;
import com.me.shared.event.OrderFreezeResultBatch;
import com.me.shared.event.OrderFreezeResultEvent;
import com.me.shared.order.OrderCancelBatch;
import com.me.shared.order.OrderCancelCommand;
import com.me.shared.order.OrderCommand;
import com.me.shared.order.OrderCommandBatch;
import com.me.shared.order.OrderHistoryEvent;
import com.me.shared.order.OrderPlacedEvent;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 委托操作执行器实现。
 *
 * <p><b>批次串行 + 顺序执行</b>：下单命令、冻结回执、撤单三条链路的所有批次
 * 先竞争 {@link #orderLock}。锁内各链路都在消费线程上顺序处理，不再使用线程池：
 * A/B 实测逐单/逐笔 {@code submit + Future + await} 的固定开销大于并行收益；
 * 处理完成后按 ConsumerRecord 输入顺序为每条输入记录自增 {@link #orderSeq}
 * 并写入 offset-seq 映射：
 * <ul>
 *   <li>下单命令：逐条保存；</li>
 *   <li>冻结回执：顺序更新委托 → 引擎对象锁内聚合撮合 → 顺序清算 → 聚合输出；</li>
 *   <li>撤单：顺序查询 → 引擎对象锁内聚合撤销 → 顺序更新 → 聚合输出。</li>
 * </ul>
 *
 * <p><b>入口锁</b>：下单、撤单入口批次在进入 {@link #orderLock} 之前先获取
 * {@link OrderEntryLocks} 中对应的入口锁；快照持有全部入口锁后，新的外部输入不会再
 * 推进状态。冻结回执是内部链路，不获取入口锁，因此快照等待内部 topic 排空时仍可继续。
 *
 * <p><b>为什么整批加锁</b>：offset-seq 的价值在于「orderSeq 的顺序就是输入记录被委托
 * 模块处理的顺序」；每条 Kafka 原生 offset 都有独立序号后，快照恢复可以精确定位每条输入。
 * 如果只锁单条命令，不同 consumer 的批次会交错，seq 无法用于快照恢复后的确定性重放。
 * 代价是三条委托链路的跨链路并发退化为「批内并发」，吞吐上限由单批大小和各链路线程池决定。
 *
 * <p><b>输出发布也在 orderLock 内</b>：冻结命令、撮合对、结算命令、委托历史和
 * offset-seq 映射在 {@link #orderLock} 内发布（publisher 写入当前 runner 绑定的
 * {@link BatchOutput}）。这样「状态修改 → 分配 orderSeq → 发布输出」在同一个临界区内
 * 完成，listener 不再在 service 返回后再发布。Kafka 事务仍由 runner 统一提交。
 *
 * <p><b>演示版重投语义</b>：不缓存撮合输出/重放。Kafka 事务回滚后的重投可能重复执行或
 * 丢失上一次未提交的输出；订单状态与撮合引擎的 orderId 去重保证重复回执不会重复撮合。
 */
@Service
public class OrderOperationServiceImpl implements OrderOperationService {

    private static final Logger log = LoggerFactory.getLogger(OrderOperationServiceImpl.class);

    private final OrderService orderService;
    private final OrderConfigCache configCache;
    private final OrderMatchingService orderMatchingService;
    private final MatchingEngine matchingEngine;
    private final OrderFreezeCommandPublisher freezeCommandPublisher;
    private final MatchPairEventPublisher matchPairPublisher;
    private final TradeSettlementCommandPublisher settlementPublisher;
    private final OrderHistoryPublisher orderHistoryPublisher;
    private final OrderOffsetSeqPublisher offsetSeqPublisher;
    private final FatalState fatalState;

    /** 委托操作的全局序号；每条 Kafka 输入记录在 {@link #orderLock} 内自增一次。 */
    private final AtomicLong orderSeq = new AtomicLong();

    /**
     * 批次级全局锁（来自 {@link OrderBatchLock}，公平可重入）：
     * 下单命令、冻结回执、撤单三条链路的批次在这里串行，
     * 保证 orderSeq 顺序与输入记录被委托模块处理的顺序一致。
     *
     * <p>由本类的 {@code execute*} 方法在消费线程里获取；锁内完成
     * 「状态修改 → 分配 orderSeq → 发布输出 → output.enqueue()」，
     * 因此入队顺序 == seq 顺序，committer 单线程按 FIFO 提交。
     * 锁在 {@code output.enqueue()} 返回后释放，<b>不覆盖</b> committer 的
     * Kafka send / commit：runner 入队后立即 poll 下一批，Kafka 事务异步提交；
     * 提交失败由 committer 进入 fatal，不会回到本锁内重试。
     */
    private final ReentrantLock orderLock;

    /**
     * 外部入口锁：下单 / 撤单入口消费批次必须先取得，再进入 {@link #orderLock}。
     * 快照持有这两把入口锁后，新的外部输入不会再驱动状态机；内部冻结回执链路
     * 不获取入口锁，因此仍可继续排空。
     */
    private final ReentrantLock placementEntryLock;
    private final ReentrantLock cancelEntryLock;

    public OrderOperationServiceImpl(
            OrderService orderService,
            OrderConfigCache configCache,
            OrderMatchingService orderMatchingService,
            MatchingEngine matchingEngine,
            OrderBatchLock orderBatchLock,
            OrderEntryLocks orderEntryLocks,
            OrderFreezeCommandPublisher freezeCommandPublisher,
            MatchPairEventPublisher matchPairPublisher,
            TradeSettlementCommandPublisher settlementPublisher,
            OrderHistoryPublisher orderHistoryPublisher,
            OrderOffsetSeqPublisher offsetSeqPublisher,
            FatalState fatalState) {
        this.orderService = orderService;
        this.configCache = configCache;
        this.orderMatchingService = orderMatchingService;
        this.matchingEngine = matchingEngine;
        this.orderLock = orderBatchLock.reentrantLock();
        this.placementEntryLock = orderEntryLocks.placementLock();
        this.cancelEntryLock = orderEntryLocks.cancelLock();
        this.freezeCommandPublisher = freezeCommandPublisher;
        this.matchPairPublisher = matchPairPublisher;
        this.settlementPublisher = settlementPublisher;
        this.orderHistoryPublisher = orderHistoryPublisher;
        this.offsetSeqPublisher = offsetSeqPublisher;
        this.fatalState = fatalState;
    }

    // ==================================================================
    // 下单命令
    // ==================================================================

    /**
     * 下单命令批次入口（系统唯一接单方式）。
     *
     * <p>业务规则：
     * <ul>
     *   <li>一条 Kafka record 是一个 symbol 的 {@link OrderCommandBatch}，批内元素顺序
     *       就是下单顺序；下游冻结、撮合必须保持这个顺序（价格-时间优先）。</li>
     *   <li>锁顺序固定为「下单入口锁 → orderLock」，与快照的加锁顺序一致；
     *       入口锁覆盖整个批次，快照切点不会把一批订单拆成两半。</li>
     *   <li>锁内完成保存、分配 orderSeq、发布冻结命令与 offset-seq，最后入提交队列；
     *       runner 不等 Kafka 提交，输出与消费 offset 由模块 committer 原子提交。</li>
     * </ul>
     */
    @Override
    public OrderOperationResult executeCommands(
            List<ConsumerRecord<String, OrderCommandBatch>> records, BatchOutput output) {
        if (records == null || records.isEmpty()) {
            return OrderOperationResult.empty();
        }
        // 锁顺序固定为「下单入口锁 → orderLock」：快照先拿全部入口锁、再拿模块锁；
        // 这里反序会与等待入口锁的快照线程形成死锁。入口锁覆盖整个批次，
        // 保证快照切点不会把一批订单拆成“一半已保存、一半未保存”。
        placementEntryLock.lock();
        try {
            return executeCommandsLocked(records, output);
        } finally {
            placementEntryLock.unlock();
        }
    }

    /**
     * 在 orderLock 内处理一个下单批次。
     *
     * <p>完整业务流程：
     * <ol>
     *   <li>展开：一条 Kafka record 是一个 symbol 的批量消息，拆成单订单；</li>
     *   <li>保存 + 校验：并发写入内存委托表，保存成功才生成冻结命令；</li>
     *   <li>分配 seq：每条输入 record 分配一个全局 orderSeq，形成 offset-seq 映射；</li>
     *   <li>发布：冻结命令按 symbol 聚合，与 offset-seq 一起写入 BatchOutput；</li>
     *   <li>入队：输出 + 输入 offset + groupMetadata 封装成 SendPackage，
     *       由 order-committer 按 FIFO 原子提交。</li>
     * </ol>
     * 任何失败都先在锁内标记 fatal 再抛出，避免下一批在脏状态上继续推进。
     */
    private OrderOperationResult executeCommandsLocked(
            List<ConsumerRecord<String, OrderCommandBatch>> records, BatchOutput output) {
        orderLock.lock();
        try {
            if (fatalState.isFatal()) {
                // 上一批可能只完成了一部分内存修改；fatal 后状态不可信，拒绝继续消费。
                throw new IllegalStateException("[" + getClass().getSimpleName()
                        + "] engine is fatal; refusing batch");
            }
            // 1) 展开：一条 Kafka record 是一个 symbol 的 OrderCommandBatch（最多 2000 单），
            //    拆成“单订单 + 来源位置”，后续才能并发保存、又按原顺序聚合。
            List<PendingOrder> pending = flattenCommands(records);
            // 2) 保存 + 校验：每单独立校验并写入委托表；只有保存成功的订单才产生冻结命令，
            //    冻结命令的返回顺序与下单顺序一致，保证同一 symbol 的价格-时间优先。
            List<FreezeCommand> freezeCommands = saveConcurrently(pending);
            // 3) 每条输入 record 分配一个全局 orderSeq（不是每单一个），
            //    offset-seq 映射供快照恢复时按 Kafka 原生 offset 定位重放边界。
            List<OrderOffsetSeq> offsets = offsets(records);
            // 下单链路只产出冻结命令和 offset-seq；撮合对、结算命令、委托历史
            // 属于冻结回执之后的链路，这里保持空。
            OrderOperationResult result = new OrderOperationResult(offsets, freezeCommands,
                    Map.of(), List.of(), List.of());
            publish(output, result);
            // 4) 输出记录 + 输入 offset + groupMetadata 在锁内封装成包，按 FIFO 进入模块提交队列；
            //    入队顺序 == orderSeq 顺序，committer 单线程按序原子提交输出与 offset。
            //    队列满时这里阻塞（背压），不会丢弃批次，也不会破坏顺序。
            output.enqueue();
            return result;
        } catch (RuntimeException e) {
            // 在释放锁之前进入全局 fatal，下一批拿到锁会看到 fatal 并退出。
            markFatal(output, e);
            throw e;
        } finally {
            orderLock.unlock();
        }
    }

    /**
     * 顺序保存一批订单（线程池并发收益为负，已改为顺序执行）。
     *
     * <p>按 pending 原顺序逐单执行 save，保证同一 symbol 的冻结命令顺序与下单顺序一致。
     * 只有 SAVED 且生成冻结命令的订单进入输出；空命令、未知交易对、业务校验失败
     * 只记录日志，不冻结资金，也不影响同批其他订单。save 抛异常时整批进入 fatal。
     */
    private List<FreezeCommand> saveConcurrently(List<PendingOrder> pending) {
        if (pending.isEmpty()) {
            return List.of();
        }
        // A/B 实测：save 线程池版本 7.80µs/单 → 顺序 4.11µs/单，
        // 逐单 submit/Future 的开销大于并行收益，故直接顺序执行。
        List<FreezeCommand> freezeCommands = new ArrayList<>(pending.size());
        for (PendingOrder source : pending) {
            SaveOutcome outcome = save(source);
            switch (outcome.status()) {
                case NULL_COMMAND -> log.warn("null order command ignored: partition={}, "
                                + "offset={}, index={}, key={}",
                        source.record().partition(), source.record().offset(), source.index(),
                        source.record().key());
                case UNKNOWN_SYMBOL -> log.warn("order command rejected: unknown or offline "
                                + "symbol: partition={}, offset={}, index={}, key={}, command={}",
                        source.record().partition(), source.record().offset(), source.index(),
                        source.record().key(), source.command());
                case REJECTED -> log.warn("order command rejected: partition={}, offset={}, "
                                + "index={}, key={}, reason={}, command={}",
                        source.record().partition(), source.record().offset(), source.index(),
                        source.record().key(), outcome.result().reason(), source.command());
                case SAVED -> {
                    // 只有成功保存并拿到冻结命令的订单才进入输出；
                    // 其余分支只做可观测日志，不冻结资金。
                    FundsCommand freezeCommand = outcome.result().freezeCommand();
                    if (freezeCommand != null) {
                        freezeCommands.add(new FreezeCommand(
                                outcome.result().order().symbol(), freezeCommand));
                    }
                }
            }
        }
        return freezeCommands;
    }

    /**
     * 保存单笔订单并生成冻结命令（worker 线程执行）。
     *
     * <p>下单入口只携带 symbol，这里从配置缓存补齐交易对 / 计价币精度；
     * {@link OrderService#apply} 负责业务校验和落委托表：成功时委托进入 ACCEPTED
     * 并返回冻结命令，校验失败 / orderId 重复时直接拒绝，不写委托、不冻结资金。
     */
    private SaveOutcome save(PendingOrder pending) {
        OrderCommand command = pending.command();
        if (command == null) {
            return new SaveOutcome(SaveStatus.NULL_COMMAND, null);
        }
        // 从配置缓存补齐精度；后续金额换算、撮合、清算只使用最小单位整数。
        TradingPairView pair = configCache.pair(command.symbol());
        if (pair == null) {
            return new SaveOutcome(SaveStatus.UNKNOWN_SYMBOL, null);
        }
        CurrencyView quoteCurrency = configCache.currency(pair.quoteCurrency());
        int quoteRawScale = quoteCurrency == null ? 0 : quoteCurrency.rawScale();
        OrderCommand scaled = command.withScales(
                pair.priceScale(), pair.quantityScale(), quoteRawScale);

        // apply 内部完成价格/数量/冻结金额校验，并把委托写入内存委托表。
        OrderApplyResult result = orderService.apply(scaled);
        return result.success()
                ? new SaveOutcome(SaveStatus.SAVED, result)
                : new SaveOutcome(SaveStatus.REJECTED, result);
    }

    /**
     * 把 poll 到的 Kafka 批量消息展开成单订单列表。
     *
     * <p>一条 record 的 value 是一个 symbol 的 {@link OrderCommandBatch}（最多 2000 单），
     * 同一 record 内的元素顺序就是下单顺序；展开时保留 record + index，便于后面
     * 按原顺序聚合冻结命令，以及在日志中定位具体订单。
     */
    private static List<PendingOrder> flattenCommands(
            List<ConsumerRecord<String, OrderCommandBatch>> records) {
        List<PendingOrder> pending = new ArrayList<>();
        for (ConsumerRecord<String, OrderCommandBatch> record : records) {
            OrderCommandBatch batch = record.value();
            if (batch == null) {
                continue;
            }
            List<OrderCommand> commands = batch.getMsgList();
            for (int index = 0; index < commands.size(); index++) {
                pending.add(new PendingOrder(record, index, commands.get(index)));
            }
        }
        return pending;
    }

    // ==================================================================
    // 冻结回执（撮合 + 清算）
    // ==================================================================

    /**
     * 冻结回执批次：账户冻结结果驱动撮合与清算。
     *
     * <p>业务流程：
     * <ol>
     *   <li>回执落地：成功冻结的委托从 ACCEPTED 推进到 PAID，失败则 REJECTED；</li>
     *   <li>撮合：本批 PAID 委托按输入顺序送入撮合引擎，产生成交对；</li>
     *   <li>清算：为每笔成交生成买卖双方的结算命令，并推进委托成交量 / 状态；</li>
     *   <li>聚合输出：撮合对（行情 / 用户成交）、结算命令、委托历史；</li>
     *   <li>分配 seq、发布并入队，与输入 offset 由 committer 原子提交。</li>
     * </ol>
     * 引擎对象锁从撮合开始一直持有到清算完成，撤单的引擎阶段会等待，避免与清算交错。
     */
    @Override
    public OrderOperationResult executeFreezeResults(
            List<ConsumerRecord<String, OrderFreezeResultBatch>> records, BatchOutput output) {
        if (records == null || records.isEmpty()) {
            return OrderOperationResult.empty();
        }
        orderLock.lock();
        try {
            if (fatalState.isFatal()) {
                throw new IllegalStateException("[" + getClass().getSimpleName()
                        + "] engine is fatal; refusing batch");
            }
            List<PendingFreeze> pending = flattenFreezeResults(records);
            // 回执先落地委托状态：成功冻结 → PAID，失败 → REJECTED；
            // 只有真正 PAID 的委托才会进入撮合。
            List<PlacedOrder> placed = applyFreezeResults(pending);

            List<OrderMatchResult> matches = List.of();
            if (!placed.isEmpty()) {
                // 撮合与清算对同一批委托必须是一个原子过程：先引擎撮合，再并发清算；
                // 期间持有引擎对象锁，撤单的引擎阶段会等待，避免撤单与清算交错。
                synchronized (matchingEngine) {
                    List<OrderPlacedEvent> orders = new ArrayList<>(placed.size());
                    for (PlacedOrder order : placed) {
                        orders.add(order.event());
                    }
                    List<MatchResult> engineResults = orderMatchingService.matchBatch(orders);
                    matches = clearConcurrently(placed, engineResults);
                }
            }

            // 按 symbol 聚合撮合对（行情 / 用户成交）；结算命令与委托历史保持批内顺序。
            Outputs outputs = assembleOutputs(placed, matches);
            List<OrderOffsetSeq> offsets = offsets(records);
            OrderOperationResult result = new OrderOperationResult(offsets, List.of(),
                    outputs.matchPairsBySymbol(), outputs.settlementCommands(),
                    outputs.historyEvents());
            publish(output, result);
            // 锁内直接路由到模块提交队列：BatchOutput.enqueue -> ModuleCommitPipeline.enqueue -> queue.put。
            output.enqueue();
            return result;
        } catch (RuntimeException e) {
            // 在释放锁之前进入全局 fatal，下一批拿到锁会看到 fatal 并退出。
            markFatal(output, e);
            throw e;
        } finally {
            orderLock.unlock();
        }
    }

    private static List<PendingFreeze> flattenFreezeResults(
            List<ConsumerRecord<String, OrderFreezeResultBatch>> records) {
        List<PendingFreeze> pending = new ArrayList<>();
        for (ConsumerRecord<String, OrderFreezeResultBatch> record : records) {
            OrderFreezeResultBatch batch = record.value();
            if (batch == null) {
                continue;
            }
            List<OrderFreezeResultEvent> events = batch.getMsgList();
            for (int index = 0; index < events.size(); index++) {
                pending.add(new PendingFreeze(record, index, events.get(index)));
            }
        }
        return pending;
    }

    /**
     * 把冻结回执逐条落到内存委托：成功冻结的委托 ACCEPTED → PAID，失败的 → REJECTED。
     * 返回真正进入撮合的 PAID 委托；重复回执由撮合引擎按 orderId 去重，
     * 不会重复产生撮合对 / 清算输出。
     *
     * <p><b>顺序执行</b>：A/B 实测逐事件线程池版本 5.62µs/event → 顺序 1.34µs/event，
     * submit/Future/await 的固定开销大于并行收益；逐事件计时保留（{@code applyTaskSum}），
     * 用于确认顺序执行后 taskSum 与墙钟基本一致。
     */
    private List<PlacedOrder> applyFreezeResults(List<PendingFreeze> pending) {
        // A/B 实测：apply 线程池版本 5.62µs/event → 顺序 1.34µs/event，
        // 线程池并发收益为负，故直接顺序执行。
        List<PlacedOrder> placed = new ArrayList<>(pending.size());
        for (PendingFreeze source : pending) {
            OrderFreezeApplyResult result = orderService.onFreezeResult(source.event());
            if (!result.handled()) {
                log.warn("order freeze result ignored: partition={}, offset={}, index={}, key={}, "
                                + "reason={}, event={}",
                        source.record().partition(), source.record().offset(), source.index(),
                        source.record().key(), result.reason(), source.event());
                continue;
            }
            if (result.placedEvent() == null) {
                log.warn("order rejected after freeze failure: partition={}, offset={}, index={}, "
                                + "orderId={}, reason={}",
                        source.record().partition(), source.record().offset(), source.index(),
                        source.event() == null ? null : source.event().orderId(),
                        result.reason());
                continue;
            }
            placed.add(new PlacedOrder(source, result.placedEvent()));
        }
        return placed;
    }

    /**
     * 成交清算（顺序执行；线程池并发收益为负，已移除 workerPool）。
     * 按引擎返回的撮合结果逐笔生成订单状态变化与结算命令；
     * duplicate 结果只记日志、不重复产生清算输出（演示版不重放）。
     */
    private List<OrderMatchResult> clearConcurrently(List<PlacedOrder> placed,
                                                     List<MatchResult> engineResults) {
        if (engineResults.size() != placed.size()) {
            throw new IllegalStateException("engine result count " + engineResults.size()
                    + " does not match placed order count " + placed.size());
        }
        // A/B 实测：clear 线程池版本 9.86µs/event → 顺序 7.62µs/event，
        // 逐笔 submit/Future 的开销大于并行收益，故直接顺序执行。
        List<OrderMatchResult> matches = new ArrayList<>(placed.size());
        for (int index = 0; index < placed.size(); index++) {
            PlacedOrder order = placed.get(index);
            MatchResult result = engineResults.get(index);
            if (result.duplicate()) {
                log.warn("matching duplicate ignored without replay: orderId={}, symbol={}, "
                                + "partition={}, offset={}, index={}",
                        order.event().orderId(), order.event().symbol(),
                        order.source().record().partition(),
                        order.source().record().offset(), order.source().index());
            }
            matches.add(orderMatchingService.clear(order.event(), result));
        }
        return matches;
    }

    /**
     * 把撮合结果拆成下游三类输出：撮合对（行情 / 用户成交）、结算命令、委托历史。
     * 只有 publishable 的结果才输出，避免重复 / 失败结果污染下游。
     */
    private Outputs assembleOutputs(List<PlacedOrder> placed, List<OrderMatchResult> matches) {
        Map<String, List<MatchPairEvent>> matchPairsBySymbol = new LinkedHashMap<>();
        List<TradeSettlementCommand> settlementCommands = new ArrayList<>();
        List<OrderHistoryEvent> historyEvents = new ArrayList<>();
        for (int index = 0; index < matches.size(); index++) {
            OrderMatchResult match = matches.get(index);
            PlacedOrder source = placed.get(index);
            if (!match.publishable()) {
                log.warn("in-process matching produced no publishable outputs: partition={}, "
                                + "offset={}, index={}, orderId={}, outcome={}",
                        source.source().record().partition(), source.source().record().offset(),
                        source.source().index(), match.orderId(), match.outcome());
                continue;
            }
            String symbol = symbolKey(source.source().record());
            if (!match.matchPairEvents().isEmpty()) {
                matchPairsBySymbol.computeIfAbsent(symbol, ignored -> new ArrayList<>())
                        .addAll(match.matchPairEvents());
            }
            if (!match.settlementCommands().isEmpty()) {
                settlementCommands.addAll(match.settlementCommands());
            }
            if (!match.historyEvents().isEmpty()) {
                historyEvents.addAll(match.historyEvents());
            }
        }
        return new Outputs(matchPairsBySymbol, settlementCommands, historyEvents);
    }

    /** 回执消息 key 即 symbol；兼容旧的固定 key。 */
    private static String symbolKey(ConsumerRecord<String, OrderFreezeResultBatch> record) {
        String key = record.key();
        return key == null || key.isBlank() ? BatchOutput.BATCH_KEY : key;
    }

    // ==================================================================
    // 撤单
    // ==================================================================

    /**
     * 撤单批次入口。
     *
     * <p>撤单只处理可撤状态（PAID / 进行中的 PARTIALLY_FILLED）的委托：先从撮合引擎撤掉挂单，
     * 再更新委托状态并生成解冻结算 / 委托历史。撤单入口锁与下单入口锁分开，
     * 但两者都必须先于 orderLock，锁顺序与快照保持一致。
     */
    @Override
    public OrderOperationResult executeCancels(
            List<ConsumerRecord<String, OrderCancelBatch>> records, BatchOutput output) {
        if (records == null || records.isEmpty()) {
            return OrderOperationResult.empty();
        }
        cancelEntryLock.lock();
        try {
            return executeCancelsLocked(records, output);
        } finally {
            cancelEntryLock.unlock();
        }
    }

    /**
     * 在 orderLock 内处理一个撤单批次：
     * 展开 → 并发校验可撤状态 → 引擎撤单 → 并发更新委托 → 聚合解冻 / 历史 → 发布入队。
     */
    private OrderOperationResult executeCancelsLocked(
            List<ConsumerRecord<String, OrderCancelBatch>> records, BatchOutput output) {
        orderLock.lock();
        try {
            if (fatalState.isFatal()) {
                throw new IllegalStateException("[" + getClass().getSimpleName()
                        + "] engine is fatal; refusing batch");
            }
            // 1) 展开成单条撤单；批内顺序用于结果聚合与日志定位。
            List<PendingCancel> pending = flattenCancels(records);
            // 2) 并发校验委托是否存在、属于该用户 / 交易对、当前状态是否可撤。
            List<PreparedCancel> prepared = prepareConcurrently(pending);
            List<OrderCancelContext> contexts = new ArrayList<>(prepared.size());
            for (PreparedCancel item : prepared) {
                OrderCancelPrepareResult result = item.prepareResult();
                if (result.cancellable()) {
                    contexts.add(result.context());
                    continue;
                }
                PendingCancel source = item.source();
            }

            List<TradeSettlementCommand> settlements = new ArrayList<>();
            List<OrderHistoryEvent> historyEvents = new ArrayList<>();
            if (!contexts.isEmpty()) {
                // 3) 引擎撤单 → 更新委托 → 生成解冻结算 / 委托历史，整个过程在 orderLock 内。
                cancelAndApply(contexts, settlements, historyEvents);
            }
            // 4) 撤单链路产出解冻结算与委托历史；冻结命令、撮合对为空。
            List<OrderOffsetSeq> offsets = offsets(records);
            OrderOperationResult result = new OrderOperationResult(offsets, List.of(), Map.of(),
                    settlements, historyEvents);
            publish(output, result);
            // 锁内直接路由到模块提交队列：BatchOutput.enqueue -> ModuleCommitPipeline.enqueue -> queue.put。
            output.enqueue();
            return result;
        } catch (RuntimeException e) {
            // 在释放锁之前进入全局 fatal，下一批拿到锁会看到 fatal 并退出。
            markFatal(output, e);
            throw e;
        } finally {
            orderLock.unlock();
        }
    }

    private void markFatal(BatchOutput output, RuntimeException error) {
        fatalState.markFatal(
                output == null ? getClass().getSimpleName() : output.source(), error);
    }

    /**
     * 在 orderLock 内发布本批输出：冻结命令、撮合对、结算命令、委托历史、offset-seq。
     *
     * <p>publisher 直接把记录写入调用方传入的 {@link BatchOutput}，最终由 runner
     * 封装成 SendPackage、模块 committer 在同一个 Kafka 事务里提交。
     */
    private void publish(BatchOutput output, OrderOperationResult result) {
        if (!result.freezeCommands().isEmpty()) {
            freezeCommandPublisher.publishBatch(output, result.freezeCommands());
        }
        if (!result.matchPairsBySymbol().isEmpty()) {
            matchPairPublisher.publishBatch(output, result.matchPairsBySymbol());
        }
        if (!result.settlementCommands().isEmpty()) {
            settlementPublisher.publishBatch(output, result.settlementCommands());
        }
        if (!result.historyEvents().isEmpty()) {
            orderHistoryPublisher.publishBatch(output, result.historyEvents());
            // 历史事件已进入本批输出（随事务提交后对外可见），终态委托可以离开内存。
            orderService.onHistoryPublished(result.historyEvents());
        }
        offsetSeqPublisher.publish(output, result);
    }

    /**
     * 撤单核心：引擎撤单 → 委托状态更新 → 解冻结算 / 委托历史。
     *
     * <p>先调用撮合引擎把挂单从订单簿移除；只有引擎接受（accepted）的撤单才继续
     * 更新委托。更新成功的委托产生解冻结算命令（买家退剩余计价币、卖家退剩余基础币）
     * 和终态委托历史（无成交 CANCELED，有成交则 PARTIALLY_FILLED 终态）。
     * 引擎移除失败的委托（已成交 / 已不在簿）不会重复解冻。
     */
    private void cancelAndApply(List<OrderCancelContext> contexts,
                                List<TradeSettlementCommand> settlements,
                                List<OrderHistoryEvent> historyEvents) {
        // 聚合撮合撤消单，调用撮合引擎（引擎对象锁），收集撤消挂单。
        List<OrderCancelMatchResult> matchResults = orderMatchingService.cancel(contexts);
        List<OrderCancelContext> cancelingContexts = new ArrayList<>(contexts.size());
        for (OrderCancelMatchResult result : matchResults) {
            CancelResult engineResult = result.engineResult();
            if (engineResult.removed()) {
                log.debug("order cancel removed from book: orderId={}, symbol={}, quantity={}",
                        engineResult.orderId(), engineResult.symbol(),
                        engineResult.removedQuantity());
            } else {
            }
            if (result.accepted()) {
                // 引擎接受才推进委托；拒绝的只记录原因，避免对已成交/已撤销委托重复解冻。
                cancelingContexts.add(result.context());
            } else {
            }
        }
        if (cancelingContexts.isEmpty()) {
            return;
        }

        List<OrderCancelApplyResult> applied = applyConcurrently(cancelingContexts);
        for (int index = 0; index < applied.size(); index++) {
            OrderCancelApplyResult result = applied.get(index);
            OrderCancelContext context = cancelingContexts.get(index);
            if (!result.handled()) {
                log.warn("order cancel update ignored: orderId={}, symbol={}, reason={}",
                        context.orderId(), context.symbol(), result.reason());
                continue;
            }
            if (result.refundAmount() > 0) {
            }
            settlements.addAll(result.settlementCommands());
            if (result.historyEvent() != null) {
                historyEvents.add(result.historyEvent());
            }
        }
    }

    private static List<PendingCancel> flattenCancels(
            List<ConsumerRecord<String, OrderCancelBatch>> records) {
        List<PendingCancel> pending = new ArrayList<>();
        for (ConsumerRecord<String, OrderCancelBatch> record : records) {
            OrderCancelBatch batch = record.value();
            if (batch == null) {
                continue;
            }
            List<OrderCancelCommand> commands = batch.getMsgList();
            for (int index = 0; index < commands.size(); index++) {
                pending.add(new PendingCancel(record, index, commands.get(index)));
            }
        }
        return pending;
    }

    private List<PreparedCancel> prepareConcurrently(List<PendingCancel> pending) {
        // 撤单链路无 1M 下单流量，未单独做 A/B；与其它链路一致顺序执行，
        // 避免逐条 submit 的固定开销。
        List<PreparedCancel> prepared = new ArrayList<>(pending.size());
        for (PendingCancel item : pending) {
            prepared.add(new PreparedCancel(item, prepare(item)));
        }
        return prepared;
    }

    private OrderCancelPrepareResult prepare(PendingCancel item) {
        OrderCancelCommand command = item.command();
        if (command == null) {
            return OrderCancelPrepareResult.rejected("CANCEL_COMMAND_IS_NULL");
        }
        return orderService.prepareCancel(command.orderId(), command.userId(), command.symbol());
    }

    private List<OrderCancelApplyResult> applyConcurrently(List<OrderCancelContext> contexts) {
        // 撤单链路无 1M 下单流量，未单独做 A/B；与其它链路一致顺序执行，
        // 避免逐条 submit 的固定开销。
        List<OrderCancelApplyResult> applied = new ArrayList<>(contexts.size());
        for (OrderCancelContext context : contexts) {
            applied.add(orderService.applyCancel(context));
        }
        return applied;
    }

    // ==================================================================
    // 公共辅助
    // ==================================================================

    @Override
    public long currentSeq() {
        return orderSeq.get();
    }

    @Override
    public void restoreSeq(long seq) {
        if (seq < 0) {
            throw new IllegalArgumentException("orderSeq must not be negative: " + seq);
        }
        long current = orderSeq.get();
        if (seq < current) {
            throw new IllegalStateException("orderSeq cannot move backwards: current="
                    + current + ", requested=" + seq);
        }
        orderSeq.set(seq);
    }

    /**
     * 本批每条 Kafka 输入记录与自己的全局 seq 的映射；按 ConsumerRecord 输入顺序，
     * 每条记录自增一次。任意 Kafka 原生 offset 都能反查到唯一的 orderSeq。
     */
    private List<OrderOffsetSeq> offsets(List<? extends ConsumerRecord<?, ?>> records) {
        // 注意：seq 是“每条 Kafka 输入 record 一个”，不是每单一个；
        // 同一 record 内的多笔订单共享这个 seq，快照恢复以 record 为最小重放单位。
        List<OrderOffsetSeq> offsets = new ArrayList<>(records.size());
        for (ConsumerRecord<?, ?> record : records) {
            offsets.add(new OrderOffsetSeq(
                    record.topic(), record.partition(), record.offset(),
                    orderSeq.incrementAndGet()));
        }
        return offsets;
    }

    /** 一条待保存订单及其在批量消息中的位置，用于按原顺序聚合与日志定位。 */
    private record PendingOrder(
            ConsumerRecord<String, OrderCommandBatch> record,
            int index,
            OrderCommand command) {
    }

    private enum SaveStatus {
        SAVED,
        NULL_COMMAND,
        UNKNOWN_SYMBOL,
        REJECTED
    }

    private record SaveOutcome(SaveStatus status, OrderApplyResult result) {
    }

    /** 一条待处理冻结回执及其在批量消息中的位置。 */
    private record PendingFreeze(
            ConsumerRecord<String, OrderFreezeResultBatch> record,
            int index,
            OrderFreezeResultEvent event) {
    }

    /** 已支付、可以进入撮合的委托及其来源位置。 */
    private record PlacedOrder(PendingFreeze source, OrderPlacedEvent event) {
    }

    /** 冻结回执链路聚合后的输出。 */
    private record Outputs(
            Map<String, List<MatchPairEvent>> matchPairsBySymbol,
            List<TradeSettlementCommand> settlementCommands,
            List<OrderHistoryEvent> historyEvents) {
    }

    /** 一条待处理撤单及其在批量消息中的位置，仅用于日志定位。 */
    private record PendingCancel(
            ConsumerRecord<String, OrderCancelBatch> record,
            int index,
            OrderCancelCommand command) {
    }

    private record PreparedCancel(PendingCancel source, OrderCancelPrepareResult prepareResult) {
    }
}
