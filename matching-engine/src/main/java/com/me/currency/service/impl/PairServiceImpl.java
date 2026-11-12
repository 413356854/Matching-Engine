package com.me.currency.service.impl;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.me.currency.entity.TradingPair;
import com.me.currency.mapper.TradingPairMapper;
import com.me.currency.service.PairService;
import com.me.currency.vo.TradingPairView;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.springframework.stereotype.Service;

/**
 * 交易对查询实现：直接读 {@code pair_symbol} 表。
 *
 * <p>上线交易对按 {@code sort_order}、{@code id} 升序返回；按代码查询时统一转大写，
 * 与 {@code OrderCommand} 的 symbol 归一化口径一致。
 */
@Service
public class PairServiceImpl implements PairService {

    private static final int STATUS_ONLINE = 1;

    private final TradingPairMapper tradingPairMapper;

    public PairServiceImpl(TradingPairMapper tradingPairMapper) {
        this.tradingPairMapper = tradingPairMapper;
    }

    @Override
    public List<TradingPairView> listOnlinePairs() {
        List<TradingPair> pairs = tradingPairMapper.selectList(
                Wrappers.<TradingPair>lambdaQuery()
                        .eq(TradingPair::getStatus, STATUS_ONLINE)
                        .orderByAsc(TradingPair::getSortOrder)
                        .orderByAsc(TradingPair::getId));
        return pairs.stream().map(PairServiceImpl::toView).toList();
    }

    @Override
    public List<TradingPairView> listPairs() {
        List<TradingPair> pairs = tradingPairMapper.selectList(
                Wrappers.<TradingPair>lambdaQuery()
                        .orderByAsc(TradingPair::getSortOrder)
                        .orderByAsc(TradingPair::getId));
        return pairs.stream().map(PairServiceImpl::toView).toList();
    }

    @Override
    public TradingPairView findBySymbol(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return null;
        }
        TradingPair pair = tradingPairMapper.selectOne(
                Wrappers.<TradingPair>lambdaQuery()
                        .eq(TradingPair::getSymbol, symbol.strip().toUpperCase(Locale.ROOT)));
        return pair == null ? null : toView(pair);
    }

    private static TradingPairView toView(TradingPair pair) {
        return new TradingPairView(
                Objects.requireNonNullElse(pair.getId(), 0L),
                pair.getSymbol(),
                pair.getBaseCurrency(),
                pair.getQuoteCurrency(),
                Objects.requireNonNullElse(pair.getPriceScale(), 0),
                Objects.requireNonNullElse(pair.getQuantityScale(), 0),
                Objects.requireNonNullElse(pair.getMinOrderQuantity(), 0L),
                Objects.requireNonNullElse(pair.getMinOrderAmount(), 0L),
                Objects.requireNonNullElse(pair.getMakerFeeBps(), 0),
                Objects.requireNonNullElse(pair.getTakerFeeBps(), 0),
                Objects.requireNonNullElse(pair.getStatus(), 0),
                Objects.requireNonNullElse(pair.getSortOrder(), 0));
    }
}
