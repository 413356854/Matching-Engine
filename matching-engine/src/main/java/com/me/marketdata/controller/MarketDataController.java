package com.me.marketdata.controller;

import com.me.marketdata.entity.KlineInterval;
import com.me.marketdata.service.MarketDataService;
import com.me.marketdata.vo.KlineQueryView;
import com.me.marketdata.vo.KlineView;
import com.me.marketdata.vo.TradeQueryView;
import com.me.marketdata.vo.TradeView;
import java.util.List;
import java.util.Locale;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 行情查询接口。
 *
 * <p>{@code GET /api/v1/marketdata/klines?symbol=BTC-USDT&interval=1m&limit=100}
 * 返回最近 N 条 K 线，按 bucket 起点升序（便于直接画图）。
 *
 * <p>{@code GET /api/v1/marketdata/trades?symbol=BTC-USDT&limit=50}
 * 返回最近 N 笔成交，按成交时间倒序（最新在前），供前端「最新成交」列表使用。
 *
 * <p>K 线：{@code symbol} 必填并统一转大写；{@code interval} 缺省 {@code 1m}，支持
 * {@code 1m / 5m / 15m / 30m / 1h / 4h / 1d}；{@code limit} 缺省 100、上限 1000。
 * 没有成交的周期不会生成 K 线，返回空列表。
 *
 * <p>成交：{@code symbol} 必填并统一转大写；{@code limit} 缺省 50、上限 500。
 */
@RestController
@RequestMapping("/api/v1/marketdata")
public class MarketDataController {

    private static final String DEFAULT_INTERVAL = "1m";

    private final MarketDataService marketDataService;

    public MarketDataController(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    @GetMapping("/klines")
    public ResponseEntity<KlineQueryView> klines(
            @RequestParam(name = "symbol", required = false) String symbol,
            @RequestParam(name = "interval", required = false) String interval,
            @RequestParam(name = "limit", defaultValue = "100") int limit) {
        String normalizedSymbol = symbol == null ? "" : symbol.strip().toUpperCase(Locale.ROOT);
        String normalizedInterval = interval == null || interval.isBlank()
                ? DEFAULT_INTERVAL
                : interval.strip().toLowerCase(Locale.ROOT);

        if (normalizedSymbol.isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(KlineQueryView.rejected(normalizedSymbol, normalizedInterval,
                            "SYMBOL_REQUIRED"));
        }

        KlineInterval klineInterval;
        try {
            klineInterval = KlineInterval.of(normalizedInterval);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .body(KlineQueryView.rejected(normalizedSymbol, normalizedInterval,
                            "INTERVAL_INVALID"));
        }

        if (limit <= 0) {
            return ResponseEntity.badRequest()
                    .body(KlineQueryView.rejected(normalizedSymbol, normalizedInterval,
                            "LIMIT_INVALID"));
        }

        List<KlineView> klines = marketDataService.klines(normalizedSymbol, klineInterval, limit)
                .stream()
                .map(KlineView::from)
                .toList();
        return ResponseEntity.ok(
                KlineQueryView.success(normalizedSymbol, klineInterval.label(), klines));
    }

    @GetMapping("/trades")
    public ResponseEntity<TradeQueryView> trades(
            @RequestParam(name = "symbol", required = false) String symbol,
            @RequestParam(name = "limit", defaultValue = "50") int limit) {
        String normalizedSymbol = symbol == null ? "" : symbol.strip().toUpperCase(Locale.ROOT);
        if (normalizedSymbol.isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(TradeQueryView.rejected(normalizedSymbol, "SYMBOL_REQUIRED"));
        }
        if (limit <= 0) {
            return ResponseEntity.badRequest()
                    .body(TradeQueryView.rejected(normalizedSymbol, "LIMIT_INVALID"));
        }

        List<TradeView> trades = marketDataService.recentTrades(normalizedSymbol, limit)
                .stream()
                .map(TradeView::from)
                .toList();
        return ResponseEntity.ok(TradeQueryView.success(normalizedSymbol, trades));
    }
}
