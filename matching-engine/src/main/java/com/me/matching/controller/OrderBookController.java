package com.me.matching.controller;

import com.me.matching.service.MatchingEngine;
import com.me.matching.vo.OrderBookDepthView;
import java.util.Locale;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 订单簿查询接口。
 *
 * <p>{@code GET /api/v1/marketdata/orderbook?symbol=BTC-USDT&depth=20} 返回撮合引擎内存
 * 订单簿的只读快照：买盘价格降序、卖盘价格升序，{@code bestBid} / {@code bestAsk} 是买一 /
 * 卖一。市价单没有价格，只通过 {@code marketBidCount} / {@code marketAskCount} 报告数量。
 *
 * <p>{@code symbol} 必填并统一转大写；{@code depth} 缺省 20、上限 100。
 * 交易对没有订单簿时返回成功和空列表。这是只读展示接口，不改变订单簿。
 */
@RestController
@RequestMapping("/api/v1/marketdata")
public class OrderBookController {

    private static final int MAX_DEPTH = 100;
    private static final int MAX_SYMBOL_LENGTH = 32;

    private final MatchingEngine matchingEngine;

    public OrderBookController(MatchingEngine matchingEngine) {
        this.matchingEngine = matchingEngine;
    }

    @GetMapping("/orderbook")
    public ResponseEntity<OrderBookDepthView> orderBook(
            @RequestParam(name = "symbol", required = false) String symbol,
            @RequestParam(name = "depth", defaultValue = "20") int depth) {
        String normalizedSymbol = symbol == null ? "" : symbol.strip().toUpperCase(Locale.ROOT);
        if (normalizedSymbol.isEmpty()) {
            return badRequest(normalizedSymbol, "SYMBOL_REQUIRED");
        }
        if (normalizedSymbol.length() > MAX_SYMBOL_LENGTH
                || !normalizedSymbol.matches("[A-Z0-9-]+")) {
            return badRequest(normalizedSymbol, "SYMBOL_INVALID");
        }
        if (depth <= 0) {
            return badRequest(normalizedSymbol, "DEPTH_INVALID");
        }

        int safeDepth = Math.min(depth, MAX_DEPTH);
        return ResponseEntity.ok(OrderBookDepthView.success(
                matchingEngine.depth(normalizedSymbol, safeDepth), safeDepth));
    }

    private static ResponseEntity<OrderBookDepthView> badRequest(String symbol, String reason) {
        return ResponseEntity.badRequest().body(OrderBookDepthView.rejected(symbol, reason));
    }
}
