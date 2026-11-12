package com.me.currency.vo;

/**
 * 交易对视图：对外暴露的交易对配置快照。
 *
 * <p>价格和数量都是交易对最小单位的整数；{@code priceScale} / {@code quantityScale}
 * 表示对外展示时的小数位数，不改变金额本身。
 *
 * <p>费率单位是基点（bps）：1 bps = 0.01%，10000 bps = 100%。
 * 允许负值表示返佣（rebate）。
 */
public record TradingPairView(
        long id,
        String symbol,
        String baseCurrency,
        String quoteCurrency,
        int priceScale,
        int quantityScale,
        long minOrderQuantity,
        long minOrderAmount,
        int makerFeeBps,
        int takerFeeBps,
        int status,
        int sortOrder) {
}
