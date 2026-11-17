package com.me.kafka;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * 一个 poll 批次的待提交输出包。
 *
 * <p>业务处理在模块锁内完成，输出消息先收集到本对象，再放入模块的 FIFO 队列；
 * 由唯一的 committer 线程按入队顺序开启 Kafka 事务，发送消息、提交输入 offset。
 *
 * <p>runner / service 不等待提交结果：队列长度是唯一的在途窗口，committer 失败时
 * 进入全局 fatal，由外部监控重启整个进程。
 *
 * <p>包内保存的是尚未序列化的 Java 对象，构造后不再修改；committer 是唯一的发送方。
 */
public final class SendPackage {

    private final String source;
    private final List<ProducerRecord<String, Object>> records;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;
    private final ConsumerGroupMetadata groupMetadata;

    public SendPackage(String source,
                       List<ProducerRecord<String, Object>> records,
                       Map<TopicPartition, OffsetAndMetadata> offsets,
                       ConsumerGroupMetadata groupMetadata) {
        this.source = source;
        this.records = List.copyOf(records);
        this.offsets = Map.copyOf(offsets);
        this.groupMetadata = groupMetadata;
    }

    /** 产生该包的 runner 名称，仅用于日志。 */
    public String source() {
        return source;
    }

    public List<ProducerRecord<String, Object>> records() {
        return records;
    }

    public Map<TopicPartition, OffsetAndMetadata> offsets() {
        return offsets;
    }

    public ConsumerGroupMetadata groupMetadata() {
        return groupMetadata;
    }
}
