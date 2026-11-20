package com.me.admin.dto;

/**
 * 管理端新增交易对请求。
 *
 * <p>交易对代码由 {@code baseCurrency + "-" + quoteCurrency} 派生，不接受客户端指定；
 * 精度、限额与费率规则见 {@link AdminPairUpdateRequest}。
 */
public record AdminPairCreateRequest(
        String baseCurrency,
        String quoteCurrency,
        Integer priceScale,
        Integer quantityScale,
        Long minOrderQuantity,
        Long minOrderAmount,
        Integer makerFeeBps,
        Integer takerFeeBps,
        Integer status,
        Integer sortOrder) {
}
