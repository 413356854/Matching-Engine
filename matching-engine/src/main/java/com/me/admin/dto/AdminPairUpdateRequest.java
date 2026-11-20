package com.me.admin.dto;

/**
 * 管理端修改交易对请求。
 *
 * <p>交易对代码、基础币种和计价币种不可修改：已有委托与历史记录都以 symbol 关联，
 * 改代码等同于新建交易对。可调整精度、最小下单量 / 金额、费率、上线状态和排序。
 *
 * <p>缺失的数值按 0 处理；{@code status} 1=上线、0=下线。
 */
public record AdminPairUpdateRequest(
        Integer priceScale,
        Integer quantityScale,
        Long minOrderQuantity,
        Long minOrderAmount,
        Integer makerFeeBps,
        Integer takerFeeBps,
        Integer status,
        Integer sortOrder) {
}
