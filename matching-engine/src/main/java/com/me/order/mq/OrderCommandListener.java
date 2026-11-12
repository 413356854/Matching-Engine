package com.me.order.mq;

import com.me.account.dto.FundsCommand;
import com.me.currency.vo.CurrencyView;
import com.me.currency.vo.TradingPairView;
import com.me.order.dto.OrderApplyResult;
import com.me.order.service.OrderConfigCache;
import com.me.order.service.OrderService;
import com.me.shared.order.OrderCommand;
import com.me.shared.order.OrderCommandBatch;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 消费委托命令，保存委托并向账户模块发送冻结命令。
 *
 * <p><b>唯一接单入口</b>：币币模块的 {@code POST /api/v1/currency/orders} 只向
 * {@code me.order.commands-batch} 投递命令，不直接调用委托模块；本监听器消费该 topic，
 * 是系统唯一的下单方式与入口。
 *
 * <p>topic key 使用交易对 symbol，同一交易对的命令落在同一分区、按序处理。
 * 同一 orderId + requestId 重投由 {@link OrderService} 返回已有委托的当前状态（不做
 * requestId 缓存）；监听容器重试不会生成重复 orderId 的委托。交易对 / 币种精度来自
 * {@link OrderConfigCache}（启动时加载的内存 Map），消费路径不访问 MySQL。
 *
 * <p><b>批量消息</b>：{@code me.order.commands-batch} 的每条 Kafka 消息是一个
 * {@link OrderCommandBatch}（下单入口 Disruptor 按「2000 条或 20ms」聚合、再按 symbol
 * 分组发送），批内按列表顺序逐条保存委托并生成冻结命令。一个 poll 批次一个 Kafka 事务；
 * 冻结命令与「委托命令消费 offset」一起提交或回滚。发送失败会回滚整批，命令重投时重新发送；
 * 冻结命令 requestId 固定，账户模块按 requestId 幂等。
 *
 * <p>冻结命令按 symbol 分组后发往 {@code me.account.order-freeze-commands-batch}，key=symbol；
 * 充值等其它资金命令仍在 {@code me.account.commands}（key=userId）。这样账户冻结阶段可以
 * 按 symbol 分区并行，同一 symbol 的冻结仍由单 consumer 按序处理。
 */
@Component
public class OrderCommandListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCommandListener.class);

    private final OrderService orderService;
    private final OrderConfigCache configCache;
    private final OrderFreezeCommandPublisher freezeCommandPublisher;

    public OrderCommandListener(OrderService orderService,
                                OrderConfigCache configCache,
                                OrderFreezeCommandPublisher freezeCommandPublisher) {
        this.orderService = orderService;
        this.configCache = configCache;
        this.freezeCommandPublisher = freezeCommandPublisher;
    }

    public void onCommand(List<ConsumerRecord<String, OrderCommandBatch>> records) {
        List<OrderFreezeCommandPublisher.FreezeCommand> freezeCommands = new ArrayList<>();
        for (ConsumerRecord<String, OrderCommandBatch> record : records) {
            OrderCommandBatch batch = record.value();
            if (batch == null) {
                continue;
            }
            List<OrderCommand> commands = batch.getMsgList();
            for (int index = 0; index < commands.size(); index++) {
                handle(record, index, commands.get(index), freezeCommands);
            }
        }
        if (!freezeCommands.isEmpty()) {
            freezeCommandPublisher.publishBatch(freezeCommands);
        }
    }

    private void handle(ConsumerRecord<String, OrderCommandBatch> record,
                        int index,
                        OrderCommand command,
                        List<OrderFreezeCommandPublisher.FreezeCommand> freezeCommands) {
        if (command == null) {
            log.warn("null order command ignored: partition={}, offset={}, index={}, key={}",
                    record.partition(), record.offset(), index, record.key());
            return;
        }

        TradingPairView pair = configCache.pair(command.symbol());
        if (pair == null) {
            log.warn("order command rejected: unknown or offline symbol: partition={}, "
                            + "offset={}, index={}, key={}, command={}",
                    record.partition(), record.offset(), index, record.key(), command);
            return;
        }

        CurrencyView quoteCurrency = configCache.currency(pair.quoteCurrency());
        int quoteScale = quoteCurrency == null ? 0 : quoteCurrency.scale();

        OrderCommand scaled = command.withScales(
                pair.priceScale(), pair.quantityScale(), quoteScale);

        OrderApplyResult result = orderService.apply(scaled);
        if (!result.success()) {
            log.warn("order command rejected: partition={}, offset={}, index={}, key={}, "
                            + "reason={}, command={}",
                    record.partition(), record.offset(), index, record.key(),
                    result.reason(), command);
            return;
        }

        FundsCommand freezeCommand = result.freezeCommand();
        if (freezeCommand != null) {
            freezeCommands.add(new OrderFreezeCommandPublisher.FreezeCommand(
                    result.order().symbol(), freezeCommand));
        }
    }
}
