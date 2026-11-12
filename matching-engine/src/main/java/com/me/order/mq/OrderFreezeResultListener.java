package com.me.order.mq;

import com.me.account.dto.TradeSettlementCommand;
import com.me.kafka.KafkaSink;
import com.me.order.dto.OrderFreezeApplyResult;
import com.me.order.dto.OrderMatchResult;
import com.me.order.service.OrderMatchingService;
import com.me.order.service.OrderService;
import com.me.shared.event.MarketDataEvent;
import com.me.shared.event.OrderFreezeResultBatch;
import com.me.shared.event.OrderFreezeResultEvent;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 消费账户模块的冻结回执批量消息，把委托推进为已支付或已拒绝；已支付时**直接在进程内调用
 * 撮合引擎**，完成成交清算并把行情、双边结算命令发布到 Kafka。
 *
 * <p>冻结成功：委托 {@code ACCEPTED → PAID}，随后同步调用 {@link OrderMatchingService}；
 * 冻结失败：委托 {@code ACCEPTED → REJECTED}，不撮合。
 *
 * <p><b>批量消费</b>：一个 poll 批次一个 Kafka 事务；每条 {@link OrderFreezeResultBatch}
 * 消息内部再按列表顺序处理。冻结回执 offset、{@code me.marketdata-batch} 行情与
 * {@code me.account.settlements-batch} 结算命令一起提交/回滚。任一条失败都会回滚整批并重投。
 *
 * <p><b>演示版重投语义</b>：不缓存撮合输出。同一订单的重复（或回滚后重投）回执仍会交给
 * {@link OrderMatchingService}，撮合引擎按 orderId 返回 duplicate，因此不会重复产生行情/
 * 结算；若上一次事务未提交，这些输出会丢失。后续会补备份消息与内存状态恢复。
 *
 * <p><b>并发</b>：冻结回执批量消息按 symbol 分组、key=symbol，同一 symbol 的消息落在同一分区
 * 且保持生产顺序。{@code app.kafka.consumer.concurrency.order-freeze-result} 可以起多个
 * consumer：Kafka 按分区分配，保证同一 symbol 只由一个 consumer 处理。
 */
@Component
public class OrderFreezeResultListener {

    private static final Logger log = LoggerFactory.getLogger(OrderFreezeResultListener.class);

    private final OrderService orderService;
    private final OrderMatchingService orderMatchingService;
    private final MarketDataEventPublisher marketDataPublisher;
    private final TradeSettlementCommandPublisher settlementPublisher;

    public OrderFreezeResultListener(OrderService orderService,
                                     OrderMatchingService orderMatchingService,
                                     MarketDataEventPublisher marketDataPublisher,
                                     TradeSettlementCommandPublisher settlementPublisher) {
        this.orderService = orderService;
        this.orderMatchingService = orderMatchingService;
        this.marketDataPublisher = marketDataPublisher;
        this.settlementPublisher = settlementPublisher;
    }

    public void onFreezeResult(List<ConsumerRecord<String, OrderFreezeResultBatch>> records) {
        // 回执消息 key=symbol；行情/结算按 symbol 分组后各自发到 symbol 分区。
        Map<String, List<MarketDataEvent>> marketDataBySymbol = new LinkedHashMap<>();
        List<TradeSettlementCommand> settlementCommands = new ArrayList<>();
        for (ConsumerRecord<String, OrderFreezeResultBatch> record : records) {
            OrderFreezeResultBatch batch = record.value();
            if (batch == null) {
                continue;
            }
            List<OrderFreezeResultEvent> events = batch.getMsgList();
            for (int index = 0; index < events.size(); index++) {
                handle(record, index, events.get(index),
                        marketDataBySymbol, settlementCommands);
            }
        }
        // 整批先发行情、再发结算，全部处于同一个 Kafka 事务。
        if (!marketDataBySymbol.isEmpty()) {
            marketDataPublisher.publishBatch(marketDataBySymbol);
        }
        if (!settlementCommands.isEmpty()) {
            settlementPublisher.publishBatch(settlementCommands);
        }
    }

    private void handle(ConsumerRecord<String, OrderFreezeResultBatch> record,
                        int index,
                        OrderFreezeResultEvent event,
                        Map<String, List<MarketDataEvent>> marketDataBySymbol,
                        List<TradeSettlementCommand> settlementCommands) {
        OrderFreezeApplyResult result = orderService.onFreezeResult(event);
        if (!result.handled()) {
            log.warn("order freeze result ignored: partition={}, offset={}, index={}, key={}, "
                            + "reason={}, event={}",
                    record.partition(), record.offset(), index, record.key(),
                    result.reason(), event);
            return;
        }
        if (result.placedEvent() == null) {
            log.warn("order rejected after freeze failure: partition={}, offset={}, index={}, "
                            + "orderId={}, reason={}",
                    record.partition(), record.offset(), index,
                    event == null ? null : event.orderId(), result.reason());
            return;
        }

        OrderMatchResult match = orderMatchingService.match(
                result.placedEvent(), record.partition(), record.offset(), index);
        if (!match.publishable()) {
            log.warn("in-process matching produced no publishable outputs: partition={}, "
                            + "offset={}, index={}, orderId={}, outcome={}",
                    record.partition(), record.offset(), index, match.orderId(),
                    match.outcome());
            return;
        }

        // 先按 symbol 收集、批次结束统一发送：行情先入队，再整批结算；与冻结回执 offset 同事务提交。
        String symbol = symbolKey(record);
        if (!match.marketDataEvents().isEmpty()) {
            marketDataBySymbol.computeIfAbsent(symbol, ignored -> new ArrayList<>())
                    .addAll(match.marketDataEvents());
        }
        if (!match.settlementCommands().isEmpty()) {
            settlementCommands.addAll(match.settlementCommands());
        }
        match.ignoredTrades().forEach(ignored ->
                log.warn("trade clearing ignored: orderId={}, tradeId={}, reason={}",
                        match.orderId(), ignored.tradeId(), ignored.reason()));
    }

    /** 回执消息 key 即 symbol；兼容旧的固定 key。 */
    private static String symbolKey(ConsumerRecord<String, OrderFreezeResultBatch> record) {
        String key = record.key();
        return key == null || key.isBlank() ? KafkaSink.BATCH_KEY : key;
    }
}
