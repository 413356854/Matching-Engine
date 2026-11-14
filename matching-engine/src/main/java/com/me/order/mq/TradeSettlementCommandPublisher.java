package com.me.order.mq;

import com.me.account.dto.TradeSettlementBatch;
import com.me.account.dto.TradeSettlementCommand;
import com.me.account.sharding.AccountSharding;
import com.me.kafka.KafkaSink;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 把成交清算命令按 userId slot 分组发送到 {@code me.account.settlements-batch}：
 * 每个 slot 一条 {@link TradeSettlementBatch} 批量消息，分区与 key 都是 slot（0..7）。
 *
 * <p>一笔成交拆成买方、卖方两条单用户命令，因此每条命令只落在一个 slot；账户模块
 * 只锁该用户对应的账户锁，不依赖跨用户原子性（金额已在下单冻结阶段扣除）。
 *
 * <p>发送发生在冻结回执消费 runner 的事务内：冻结回执 offset、撮合对记录与结算命令一起提交。
 * 演示版不缓存/重放结算命令。
 */
@Component
public class TradeSettlementCommandPublisher {

    private final KafkaSink sink;
    private final String topic;

    public TradeSettlementCommandPublisher(
            KafkaSink sink,
            @Value("${app.kafka.topics.account-settlements}") String topic) {
        this.sink = sink;
        this.topic = topic;
    }

    /** 按 userId slot 分组发送结算批量消息，由当前 Kafka 事务统一提交。 */
    public void publishBatch(List<TradeSettlementCommand> commands) {
        if (commands == null || commands.isEmpty()) {
            return;
        }
        Map<Integer, List<TradeSettlementCommand>> bySlot = new LinkedHashMap<>();
        for (TradeSettlementCommand command : commands) {
            int slot = AccountSharding.slot(command.legs().get(0).userId());
            bySlot.computeIfAbsent(slot, ignored -> new ArrayList<>()).add(command);
        }
        bySlot.forEach((slot, batch) ->
                sink.sendBatchMessage(topic, slot, Integer.toString(slot), batch,
                        TradeSettlementBatch::new));
    }
}
