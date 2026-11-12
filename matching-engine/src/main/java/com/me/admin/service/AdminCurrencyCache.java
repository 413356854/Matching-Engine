package com.me.admin.service;

import com.me.currency.service.CurrencyService;
import com.me.currency.vo.CurrencyView;
import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * 管理模块的启用币种启动缓存。
 *
 * <p>启动时查询一次 {@link CurrencyService#listEnabledCurrencies()}，按 code 建成不可变 Map；
 * 充值接口只查内存。修改币种配置后需要重启应用。
 */
@Component
public class AdminCurrencyCache {

    private final CurrencyService currencyService;

    private volatile Map<String, CurrencyView> currenciesByCode = Map.of();

    public AdminCurrencyCache(CurrencyService currencyService) {
        this.currencyService = currencyService;
    }

    @PostConstruct
    public void load() {
        Map<String, CurrencyView> currencies = new HashMap<>();
        for (CurrencyView currency : currencyService.listEnabledCurrencies()) {
            if (currency != null && currency.code() != null) {
                currencies.put(normalize(currency.code()), currency);
            }
        }
        currenciesByCode = Map.copyOf(currencies);
    }

    /** 按币种代码查询启用币种；不存在返回 {@code null}。 */
    public CurrencyView find(String code) {
        return code == null ? null : currenciesByCode.get(normalize(code));
    }

    private static String normalize(String value) {
        return value.strip().toUpperCase(Locale.ROOT);
    }
}
