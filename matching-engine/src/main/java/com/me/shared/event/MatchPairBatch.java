package com.me.shared.event;

import java.util.List;

/**
 * 撮合对批量消息：委托模块撮合完一个消费批次后，把该批产生的全部
 * {@link MatchPairEvent} 封装成一条 Kafka 消息，发往 {@code me.match-pair-batch}。
 *
 * <p>批内顺序即撮合产生成交的顺序，行情模块按列表顺序聚合 K 线；admin 模块把每个撮合对
 * 拆成 taker / maker 两条用户成交记录归档。
 */
public class MatchPairBatch {

    private List<MatchPairEvent> msgList = List.of();

    public MatchPairBatch() {
    }

    public MatchPairBatch(List<MatchPairEvent> msgList) {
        setMsgList(msgList);
    }

    public List<MatchPairEvent> getMsgList() {
        return msgList == null ? List.of() : msgList;
    }

    public void setMsgList(List<MatchPairEvent> msgList) {
        this.msgList = msgList == null ? List.of() : List.copyOf(msgList);
    }
}
