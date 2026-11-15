package com.me.order.dto;

import java.util.List;

/**
 * 委托 offset-seq 批量消息：委托模块处理完一个消费批次后，把该批每条输入记录
 * 与批次 {@code orderSeq} 的对应关系封装成一条 Kafka 消息，发往
 * {@code me.order.offset-seq-batch}。
 *
 * <p>与撮合输出（撮合对 / 结算命令 / 委托历史）及输入消费 offset 在同一个 Kafka
 * 事务内提交，因此「输入 offset → orderSeq」的映射不会出现写一半的情况。
 *
 * <p>三条委托输入链路使用各自的事务 producer，topic 内的到达顺序不一定等于
 * {@code orderSeq} 顺序；回溯时应按 {@code orderSeq} 排序（并忽略重复提交的
 * 同一输入记录）后再重放。
 */
public class OrderOffsetSeqBatch {

    private List<OrderOffsetSeq> msgList = List.of();

    public OrderOffsetSeqBatch() {
    }

    public OrderOffsetSeqBatch(List<OrderOffsetSeq> msgList) {
        setMsgList(msgList);
    }

    public List<OrderOffsetSeq> getMsgList() {
        return msgList == null ? List.of() : List.copyOf(msgList);
    }

    public void setMsgList(List<OrderOffsetSeq> msgList) {
        this.msgList = msgList == null ? List.of() : List.copyOf(msgList);
    }
}
