package com.me.account.dto;

/**
 * 一条账户输入消费记录与全局账户序列号的对应关系。
 *
 * <p>账户模块在账户全局锁内按 ConsumerRecord 输入顺序，为每条 Kafka 记录
 * （{@code topic / partition / offset}）分配独立的递增 {@code accountSeq}，再把记录与
 * 自己的序号配对，汇总成一条 {@link AccountOffsetSeqBatch} 发往
 * {@code me.account.offset-seq-batch}。
 *
 * <p>备份快照恢复后回溯 Kafka 消息时，用这些记录确定「哪些输入消息已经执行过」
 * （{@code accountSeq <= 快照水位}）以及重放时的全局执行顺序。
 */
public record AccountOffsetSeq(
        String topic,
        int partition,
        long offset,
        long accountSeq) {
}
