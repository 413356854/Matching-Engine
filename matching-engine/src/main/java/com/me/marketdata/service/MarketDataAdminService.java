package com.me.marketdata.service;

import com.me.marketdata.entity.Kline;
import com.me.marketdata.entity.KlineInterval;
import java.util.List;

/**
 * 行情管理端服务：查询归档的 K 线、一键清空 K 线。
 */
public interface MarketDataAdminService {

    /**
     * 查询最近 N 条 K 线，按 bucket 起点倒序（最新在前）。
     *
     * @param symbol   交易对；{@code null} 表示不筛选
     * @param interval K 线周期；{@code null} 表示不筛选
     * @param limit    条数，1..1000，超出范围按边界夹取
     */
    List<Kline> recentKlines(String symbol, KlineInterval interval, int limit);

    /**
     * 一键删除全部 K 线，返回删除前的记录数；成交明细不受影响。
     */
    long clearKlines();
}
