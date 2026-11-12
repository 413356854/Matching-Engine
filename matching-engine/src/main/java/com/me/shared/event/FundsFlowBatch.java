package com.me.shared.event;

import java.util.List;

/**
 * 资金流水批量消息：账户模块处理完一个消费批次后，把该批产生的全部
 * {@link FundsFlowEvent} 封装成一条 Kafka 消息，发往 {@code me.funds-batch}。
 *
 * <p>批内顺序即账户操作顺序；admin 模块按列表顺序归档，并按 eventId 去重。
 */
public class FundsFlowBatch {

    private List<FundsFlowEvent> msgList = List.of();

    public FundsFlowBatch() {
    }

    public FundsFlowBatch(List<FundsFlowEvent> msgList) {
        setMsgList(msgList);
    }

    public List<FundsFlowEvent> getMsgList() {
        return msgList == null ? List.of() : msgList;
    }

    public void setMsgList(List<FundsFlowEvent> msgList) {
        this.msgList = msgList == null ? List.of() : List.copyOf(msgList);
    }
}
