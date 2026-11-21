package com.me.shared.marketdata;

/**
 * 只读最新成交价查询端口：供下单入口做市价单参考价校验。
 *
 * <p>实现方只返回内存中的最近一笔成交价；没有成交或缓存尚未就绪时返回 {@code null}，
 * 调用方只能把返回值当作参考价，不能据此阻塞下单。
 */
public interface LatestPriceQueryPort {

    /**
     * @param symbol 交易对（大写，{@code BASE-QUOTE}）
     * @return 最近一笔成交价（最小单位）；没有成交时返回 {@code null}
     */
    Long latestPrice(String symbol);
}
