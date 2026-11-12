package com.me.currency.service;

import com.me.currency.vo.CurrencyView;
import com.me.currency.vo.TradingPairView;
import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * 币币模块的交易对 / 币种启动缓存。
 *
 * <p>启动时各查询一次列表，按 symbol / code 建成不可变 Map；下单校验和交易对、币种查询
 * 接口都只读内存，不再逐请求访问 MySQL。缓存保留下线交易对，以便区分
 * {@code SYMBOL_NOT_FOUND} 与 {@code SYMBOL_NOT_TRADING}；修改配置后需要重启应用。
 */
@Component
public class CurrencyConfigCache {

    private static final int STATUS_ONLINE = 1;

    private final PairService pairService;
    private final CurrencyService currencyService;

    private volatile Map<String, TradingPairView> pairsBySymbol = Map.of();
    private volatile Map<String, CurrencyView> currenciesByCode = Map.of();
    private volatile List<TradingPairView> onlinePairs = List.of();
    private volatile List<CurrencyView> enabledCurrencies = List.of();

    public CurrencyConfigCache(PairService pairService, CurrencyService currencyService) {
        this.pairService = pairService;
        this.currencyService = currencyService;
    }

    @PostConstruct
    public void load() {
        List<TradingPairView> pairs = pairService.listPairs();
        Map<String, TradingPairView> pairMap = new HashMap<>();
        for (TradingPairView pair : pairs) {
            if (pair != null && pair.symbol() != null) {
                pairMap.put(normalize(pair.symbol()), pair);
            }
        }
        List<CurrencyView> currencies = currencyService.listEnabledCurrencies();
        Map<String, CurrencyView> currencyMap = new HashMap<>();
        for (CurrencyView currency : currencies) {
            if (currency != null && currency.code() != null) {
                currencyMap.put(normalize(currency.code()), currency);
            }
        }
        pairsBySymbol = Map.copyOf(pairMap);
        currenciesByCode = Map.copyOf(currencyMap);
        onlinePairs = pairs.stream()
                .filter(pair -> pair != null && pair.status() == STATUS_ONLINE)
                .toList();
        enabledCurrencies = List.copyOf(currencies);
    }

    /** 按 symbol 查询交易对（含下线状态）；不存在返回 {@code null}。 */
    public TradingPairView pair(String symbol) {
        return symbol == null ? null : pairsBySymbol.get(normalize(symbol));
    }

    /** 按币种代码查询启用币种；不存在返回 {@code null}。 */
    public CurrencyView currency(String code) {
        return code == null ? null : currenciesByCode.get(normalize(code));
    }

    /** 上线交易对，保持启动时查询的 sortOrder / id 顺序。 */
    public List<TradingPairView> onlinePairs() {
        return onlinePairs;
    }

    /** 启用币种，保持启动时查询的 sortOrder / id 顺序。 */
    public List<CurrencyView> enabledCurrencies() {
        return enabledCurrencies;
    }

    private static String normalize(String value) {
        return value.strip().toUpperCase(Locale.ROOT);
    }
}
