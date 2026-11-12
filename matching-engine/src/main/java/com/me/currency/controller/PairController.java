package com.me.currency.controller;

import com.me.currency.service.CurrencyConfigCache;
import com.me.currency.vo.TradingPairView;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 交易对查询接口。
 *
 * <p>{@code GET /api/v1/pairs} 返回全部上线交易对，按展示顺序升序。
 */
@RestController
@RequestMapping("/api/v1/pairs")
public class PairController {

    private final CurrencyConfigCache configCache;

    public PairController(CurrencyConfigCache configCache) {
        this.configCache = configCache;
    }

    @GetMapping
    public List<TradingPairView> list() {
        return configCache.onlinePairs();
    }
}
