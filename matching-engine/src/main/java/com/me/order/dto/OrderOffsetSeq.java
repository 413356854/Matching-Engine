package com.me.order.dto;

/**
 * 一条委托输入消费记录与全局委托序列号的对应关系。
 *
 * <p>委托模块在全局 {@code orderLock} 内按 ConsumerRecord 输入顺序，为每条 Kafka 记录
 * （{@code topic / partition / offset}）分配独立的递增 {@code orderSeq}，再把记录与
 * 自己的序号配对，汇总成一条 {@link OrderOffsetSeqBatch} 发往
 * {@code me.order.offset-seq-batch}。
 *
 * <p>备份快照恢复后回溯 Kafka 消息时，用这些记录确定「哪些输入消息已经执行过」
 * （{@code orderSeq <= 快照水位}）以及重放时的全局执行顺序。
 */
public record OrderOffsetSeq(
        String topic,
        int partition,
        long offset,
        long orderSeq) {
}
