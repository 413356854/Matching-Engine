package com.me.kafka;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 两阶段批处理器：先把整批交给后台处理线程，再在 poll 线程等待并完成收尾。
 *
 * <p>两阶段之间 {@link KafkaConsumerRunner} 会非阻塞地 {@code poll} 一次，预取下一批，
 * 从而把「commit 之后等下一次 poll 返回」的时间藏到当前批的业务处理后面。
 *
 * <p><b>线程约束</b>：{@link #submit(List)} 与 {@link #await()} 都必须在 poll 线程调用；
 * {@code await()} 返回前必须完成所有需要 Kafka 事务上下文的动作（例如通过
 * {@link KafkaSink} 发送输出），失败时直接抛出。
 */
public interface PrefetchingBatchProcessor<T> extends KafkaBatchProcessor<T> {

    /** 把本批交给处理线程后立即返回。 */
    void submit(List<ConsumerRecord<String, T>> records);

    /** 等待 submit 的批次处理完成，并在当前线程执行事务内收尾；失败抛出。 */
    void await();

    /** 不支持预取的调用方按同步语义使用。 */
    @Override
    default void process(List<ConsumerRecord<String, T>> records) {
        submit(records);
        await();
    }
}
