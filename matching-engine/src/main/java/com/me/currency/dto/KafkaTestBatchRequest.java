package com.me.currency.dto;

import java.util.List;

/**
 * 批量下单测试请求（{@code POST /api/v1/currency/orders/kafka-test-batch}）。
 *
 * <p>只用于压测 / 外部批量接入：请求体是下单请求数组，<b>不携带 orderId</b>；
 * 服务端逐单校验并由币币模块生成委托号后写入同一个下单 Disruptor。
 */
public record KafkaTestBatchRequest(List<PlaceOrderRequest> orders) {

    public KafkaTestBatchRequest {
        orders = orders == null ? List.of() : List.copyOf(orders);
    }
}
