package com.me.marketdata.service.impl;

import com.me.marketdata.entity.Kline;
import com.me.marketdata.entity.KlineInterval;
import com.me.marketdata.repository.MarketDataRepository;
import com.me.marketdata.service.MarketDataAdminService;
import com.me.marketdata.service.MarketDataService;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * 行情管理端实现：直接委托 QuestDB 访问层。
 */
@Service
public class MarketDataAdminServiceImpl implements MarketDataAdminService {

    private final MarketDataRepository repository;
    private final KlineAggregator aggregator;
    private final MarketDataService marketDataService;

    public MarketDataAdminServiceImpl(MarketDataRepository repository,
                                      KlineAggregator aggregator,
                                      MarketDataService marketDataService) {
        this.repository = repository;
        this.aggregator = aggregator;
        this.marketDataService = marketDataService;
    }

    @Override
    public List<Kline> recentKlines(String symbol, KlineInterval interval, int limit) {
        return repository.findRecentKlines(symbol, interval, limit);
    }

    @Override
    public long clearKlines() {
        long deletedCount = repository.truncateKlines();
        // 锚点已随 K 线表一起清空，内存里的当前 K 线和写入节流状态也必须丢弃，
        // 否则下一批会从旧状态续写、或把重建后的 bucket 误判为已写过。
        aggregator.clear();
        marketDataService.clearWriteState();
        return deletedCount;
    }
}
