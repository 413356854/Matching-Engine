package com.me.currency.vo;

/**
 * 币种视图：币种代码、名称与精度。
 *
 * <p>{@code scale} 是小数位数：最小单位 = 10<sup>-scale</sup>，前端据此把账本里的
 * 整数余额换算成展示值。
 */
public record CurrencyView(
        long id,
        String code,
        String name,
        int scale,
        int status,
        int sortOrder) {
}
