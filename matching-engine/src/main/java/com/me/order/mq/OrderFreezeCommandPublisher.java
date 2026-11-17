package com.me.order.mq;

import com.me.account.dto.FundsCommand;
import com.me.account.dto.OrderFreezeCommandsBatch;
import com.me.kafka.BatchOutput;
import com.me.order.dto.FreezeCommand;
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
 * <p>只由 {@code OrderCommandListener} 在委托命令消费批次的事务内发送；REST 下单不再直接
 * 触发冻结。
 */
@Component
public class OrderFreezeCommandPublisher {

    private final String topic;

    public OrderFreezeCommandPublisher(
            @Value("${app.kafka.topics.account-order-freeze-commands}") String topic) {
        this.topic = topic;
    }

    /** 把一条冻结命令封装成单元素批量消息，放入本批次输出。 */
    public void publish(BatchOutput output, String symbol, FundsCommand command) {
        output.sendBatchMessage(topic, symbol, List.of(command), OrderFreezeCommandsBatch::new);
    }

    /**
     * 按 symbol 分组，每组一条批量消息入队，由当前 Kafka 事务统一提交。
     *
     * <p>symbol 分区保证同一 symbol 的冻结命令由单 consumer 顺序处理，冻结回执也按
     * symbol 汇总，撮合阶段的价格-时间优先不被破坏。账户模块在消费端再把批内命令
     * 按 userId slot 拆给 8 个线程并发执行。
     */
    public void publishBatch(BatchOutput output, List<FreezeCommand> commands) {
        if (commands == null || commands.isEmpty()) {
            return;
        }
        Map<String, List<FundsCommand>> bySymbol = new LinkedHashMap<>();
        for (FreezeCommand command : commands) {
            bySymbol.computeIfAbsent(command.symbol(), ignored -> new ArrayList<>())
                    .add(command.command());
        }
        bySymbol.forEach((symbol, batch) ->
                output.sendBatchMessage(topic, symbol, batch, OrderFreezeCommandsBatch::new));
    }
}
