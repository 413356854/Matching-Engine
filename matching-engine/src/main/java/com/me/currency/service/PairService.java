package com.me.currency.service;

import com.me.currency.vo.TradingPairView;
import java.util.List;

/**
 * 交易对业务接口：查询交易对配置。跨模块只调用该接口。
 */
public interface PairService {

    /**
     * 查询全部上线交易对，按 {@code sortOrder}、{@code id} 升序。
     */
    List<TradingPairView> listOnlinePairs();

    /**
     * 查询全部交易对（含下线状态），按 {@code sortOrder}、{@code id} 升序。
     *
     * <p>供启动缓存使用：缓存需要保留下线交易对，才能区分 {@code SYMBOL_NOT_FOUND}
     * 与 {@code SYMBOL_NOT_TRADING}。
     */
    List<TradingPairView> listPairs();

    /**
     * 按交易对代码查询配置；{@code symbol} 不区分大小写。不存在时返回 {@code null}。
     *
     * <p>返回结果可能带下线状态（{@code status=0}），由调用方决定是否允许交易。
     */
    TradingPairView findBySymbol(String symbol);
}
