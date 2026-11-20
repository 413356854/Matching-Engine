package com.me.currency.vo;

/**
 * 币种视图：币种代码、名称与精度。
 *
 * <p>{@code rawScale} 是账本/撮合/结算使用的原始精度：金额、数量最小单位 =
 * 10<sup>-rawScale</sup>；{@code scale} 是对用户的展示小数上限，必须
 * {@code scale <= rawScale}。例如 USDT {@code rawScale=6, scale=2}：
 * 内部按 6 位记账，界面最多展示 2 位。
 */
public record CurrencyView(
        long id,
        String code,
        String name,
        int scale,
        int rawScale,
        int status,
        int sortOrder) {

    public CurrencyView {
        if (scale < 0 || scale > 18) {
            throw new IllegalArgumentException("display scale out of range: " + scale);
        }
        if (rawScale < 0 || rawScale > 18) {
            throw new IllegalArgumentException("raw scale out of range: " + rawScale);
        }
        if (scale > rawScale) {
            throw new IllegalArgumentException(
                    "display scale " + scale + " exceeds raw scale " + rawScale);
        }
    }
}
