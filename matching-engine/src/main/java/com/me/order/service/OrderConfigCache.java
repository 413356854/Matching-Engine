package com.me.order.service;

import com.me.currency.service.CurrencyService;
import com.me.currency.service.PairService;
import com.me.currency.vo.CurrencyView;
import com.me.currency.vo.TradingPairView;
import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * 委托模块的交易对 / 币种启动缓存。
 *
 * <p>启动时查询一次 {@link PairService#listOnlinePairs()} 和
 * {@link CurrencyService#listEnabledCurrencies()}，按 symbol / code 建成不可变 Map；
 * 之后下单命令消费路径只查内存，不再逐批访问 MySQL。缓存只在进程启动时刷新，
 * 修改交易对或币种配置后需要重启应用。
 *
 * <p>缓存的是上线交易对；外部生产者向 {@code me.order.commands-batch} 写入下线或未知 symbol
 * 时会被拒绝。
 */
@Component
public class OrderConfigCache {

    private final PairService pairService;
    private final CurrencyService currencyService;

    private volatile Map<String, TradingPairView> pairsBySymbol = Map.of();
    private volatile Map<String, CurrencyView> currenciesByCode = Map.of();

    public OrderConfigCache(PairService pairService, CurrencyService currencyService) {
        this.pairService = pairService;
        this.currencyService = currencyService;
    }

    @PostConstruct
    public void load() {
        Map<String, TradingPairView> pairs = new HashMap<>();
        for (TradingPairView pair : pairService.listOnlinePairs()) {
            if (pair != null && pair.symbol() != null) {
                pairs.put(normalize(pair.symbol()), pair);
            }
        }
        Map<String, CurrencyView> currencies = new HashMap<>();
        for (CurrencyView currency : currencyService.listEnabledCurrencies()) {
            if (currency != null && currency.code() != null) {
                currencies.put(normalize(currency.code()), currency);
            }
        }
        pairsBySymbol = Map.copyOf(pairs);
        currenciesByCode = Map.copyOf(currencies);
    }

    /** 按 symbol 查询上线交易对；不存在返回 {@code null}。 */
    public TradingPairView pair(String symbol) {
        return symbol == null ? null : pairsBySymbol.get(normalize(symbol));
    }

    /** 按币种代码查询启用币种；不存在返回 {@code null}。 */
    public CurrencyView currency(String code) {
        return code == null ? null : currenciesByCode.get(normalize(code));
    }

    private static String normalize(String value) {
        return value.strip().toUpperCase(Locale.ROOT);
    }
}
