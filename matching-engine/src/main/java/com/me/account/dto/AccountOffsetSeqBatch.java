package com.me.account.dto;

import java.util.List;

/**
 * 账户 offset-seq 批量消息：账户模块处理完一个消费批次后，把该批每条输入记录
 * 与自己的 {@code accountSeq} 的对应关系封装成一条 Kafka 消息，发往
 * {@code me.account.offset-seq-batch}。
 *
 * <p>与资金流水、冻结回执、消费 offset 在同一个 Kafka 事务内提交，因此
 * 「输入 offset → accountSeq」的映射不会出现写一半的情况。备份快照恢复后按
 * {@code accountSeq} 顺序回溯/重放这些输入消息。
 *
 * <p>三条账户链路使用各自的事务 producer，topic 内的到达顺序不一定等于
 * {@code accountSeq} 顺序；回溯时应按 {@code accountSeq} 排序（并忽略重复提交的
 * 同一输入记录）后再重放。
 */
public class AccountOffsetSeqBatch {

    private List<AccountOffsetSeq> msgList = List.of();

    public AccountOffsetSeqBatch() {
    }

    public AccountOffsetSeqBatch(List<AccountOffsetSeq> msgList) {
        setMsgList(msgList);
    }

    public List<AccountOffsetSeq> getMsgList() {
        return msgList == null ? List.of() : msgList;
    }

    public void setMsgList(List<AccountOffsetSeq> msgList) {
        this.msgList = msgList == null ? List.of() : List.copyOf(msgList);
    }
}
