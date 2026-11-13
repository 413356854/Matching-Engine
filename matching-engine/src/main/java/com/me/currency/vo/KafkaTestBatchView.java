package com.me.currency.vo;

/**
 * 批量下单测试回执。
 *
 * <p>{@code accepted} / {@code rejected} 是本批逐单校验的结果；成功受理的命令只表示已进入
 * 下单 Disruptor，不表示委托已保存或已冻结。{@code firstOrderId} / {@code lastOrderId}
 * 是本批由服务端生成的首尾委托号，便于压测时定位。
 */
public record KafkaTestBatchView(
        boolean success,
        String reason,
        int accepted,
        int rejected,
        Long firstOrderId,
        Long lastOrderId,
        String firstRejectReason) {

    public static KafkaTestBatchView accepted(int accepted, int rejected,
                                              Long firstOrderId, Long lastOrderId,
                                              String firstRejectReason) {
        return new KafkaTestBatchView(true, null, accepted, rejected,
                firstOrderId, lastOrderId, firstRejectReason);
    }

    public static KafkaTestBatchView rejected(String reason) {
        return new KafkaTestBatchView(false, reason, 0, 0, null, null, null);
    }

    public static KafkaTestBatchView publishFailed(String reason, int accepted, int rejected) {
        return new KafkaTestBatchView(false, reason, accepted, rejected, null, null, null);
    }
}
