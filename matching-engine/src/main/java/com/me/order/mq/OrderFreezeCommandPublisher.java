package com.me.order.mq;

import com.me.account.dto.FundsCommand;
import com.me.account.dto.OrderFreezeCommandsBatch;
import com.me.kafka.KafkaSink;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 把下单冻结命令按 symbol 分组发送到 {@code me.account.order-freeze-commands-batch}：
 * 每个 symbol 一条 {@link OrderFreezeCommandsBatch} 批量消息，key=symbol。
 *
 * <p>冻结链路保持 symbol 分区：同一 symbol 的命令由单 consumer 顺序消费，冻结回执
 * 也按 symbol 汇总，撮合阶段的价格-时间优先不被破坏。账户模块在消费端再把批内命令
 * 按 userId slot 拆给 8 个线程并发执行；冻结、清算、充值仍共用 {@code slot(userId)}
 * 的账户锁。
 *
 * <p>只由委托模块的 Kafka 消费链路在冻结回执批次事务内发送；REST 下单不再直接触发冻结。
 */
@Component
public class OrderFreezeCommandPublisher {

    private final KafkaSink sink;
    private final String topic;

    public OrderFreezeCommandPublisher(
            KafkaSink sink,
            @Value("${app.kafka.topics.account-order-freeze-commands}") String topic) {
        this.sink = sink;
        this.topic = topic;
    }

    /** 一条待发送的冻结命令及其 symbol；symbol 只用于 Kafka key 与批次分组。 */
    public record FreezeCommand(String symbol, FundsCommand command) {
    }

    /** 在当前消费批次事务中发送一条冻结命令（封装成单元素批量消息）。 */
    public void publish(String symbol, FundsCommand command) {
        sink.sendBatchMessage(topic, symbol, List.of(command), OrderFreezeCommandsBatch::new);
    }

    /**
     * 按 symbol 分组，每组一条批量消息入队，由当前 Kafka 事务统一提交。
     *
     * <p>symbol 分区保证同一 symbol 的冻结命令由单 consumer 顺序处理，冻结回执也按
     * symbol 汇总，撮合阶段的价格-时间优先不被破坏。账户模块在消费端再把批内命令
     * 按 userId slot 拆给 8 个线程并发执行。
     */
    public void publishBatch(List<FreezeCommand> commands) {
        if (commands == null || commands.isEmpty()) {
            return;
        }
        Map<String, List<FundsCommand>> bySymbol = new LinkedHashMap<>();
        for (FreezeCommand command : commands) {
            bySymbol.computeIfAbsent(command.symbol(), ignored -> new ArrayList<>())
                    .add(command.command());
        }
        bySymbol.forEach((symbol, batch) ->
                sink.sendBatchMessage(topic, symbol, batch, OrderFreezeCommandsBatch::new));
    }
}
