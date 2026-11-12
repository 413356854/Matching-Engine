package com.me.currency.controller;

import com.me.currency.service.CurrencyConfigCache;
import com.me.currency.vo.CurrencyView;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 币种查询接口。
 *
 * <p>{@code GET /api/v1/currencies} 返回全部启用币种及其精度，按展示顺序升序。
 * 前端用 {@code scale} 把账户余额（最小单位整数）换算成展示值。
 */
@RestController
@RequestMapping("/api/v1/currencies")
public class CurrencyController {

    private final CurrencyConfigCache configCache;

    public CurrencyController(CurrencyConfigCache configCache) {
        this.configCache = configCache;
    }

    @GetMapping
    public List<CurrencyView> list() {
        return configCache.enabledCurrencies();
    }
}
