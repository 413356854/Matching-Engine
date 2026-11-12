package com.me.account.mq;

import com.me.account.dto.FundsCommand;
import com.me.account.sharding.AccountSharding;
import com.me.kafka.KafkaRestPublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 把资金命令（如管理员充值）发送到 {@code me.account.commands}。
 *
 * <p>分区与账户锁使用同一个 {@code slot(userId)}：同一用户的充值、冻结、清算都落到
 * 同一分区/同一把账户锁，不会并发修改同一账户。
 *
 * <p>REST 线程没有消费批次事务，使用 {@link KafkaRestPublisher} 的幂等、非事务 producer
 * 同步发送并等待 ack；失败时抛回 controller，返回 503。
 */
@Component
public class AccountCommandPublisher {

    private final KafkaRestPublisher restPublisher;
    private final String topic;

    public AccountCommandPublisher(KafkaRestPublisher restPublisher,
                                   @Value("${app.kafka.topics.account-commands}") String topic) {
        this.restPublisher = restPublisher;
        this.topic = topic;
    }

    /** 幂等、非事务发送并等待 ack；失败时抛异常，由调用方返回可重试错误。 */
    public void publishAndWait(FundsCommand command) {
        int slot = AccountSharding.slot(command.userId());
        restPublisher.send(topic, slot, Integer.toString(slot), command);
    }
}
