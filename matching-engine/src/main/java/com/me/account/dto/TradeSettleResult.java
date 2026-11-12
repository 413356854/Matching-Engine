package com.me.account.dto;

import com.me.shared.event.FundsFlowEvent;
import java.util.List;

/**
 * 双边成交结算结果。
 *
 * <p>成功时每条资金腿生成一条资金流水（{@link FundsFlowEvent}）；
 * 失败时余额不变，{@code reason} 说明拒绝原因。
 */
public record TradeSettleResult(boolean success, String reason, List<FundsFlowEvent> flowEvents) {

    public TradeSettleResult {
        flowEvents = flowEvents == null ? List.of() : List.copyOf(flowEvents);
    }

    public static TradeSettleResult success(List<FundsFlowEvent> flowEvents) {
        return new TradeSettleResult(true, null, flowEvents);
    }

    public static TradeSettleResult rejected(String reason) {
        return new TradeSettleResult(false, reason, List.of());
    }
}
