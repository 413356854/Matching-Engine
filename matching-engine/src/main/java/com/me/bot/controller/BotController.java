package com.me.bot.controller;

import com.me.bot.config.BotProperties;
import com.me.bot.service.BinanceMarketClient;
import com.me.bot.service.BinanceMarketClient.Quote;
import com.me.bot.service.BotTradingService;
import com.me.bot.service.BotTradingService.EnableResult;
import com.me.bot.vo.BotQuoteView;
import com.me.bot.vo.BotStatusView;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 机器人模拟模块接口。
 *
 * <p>{@code GET /api/v1/bot/status} 查询模块状态、币安行情快照与下单统计；
 * {@code POST /api/v1/bot/enable} 先调用币安测试接口，通过后才开启自动交易；
 * {@code POST /api/v1/bot/disable} 停止下单并断开行情连接。
 *
 * <p>开启失败时仍返回完整状态，前端据 {@code reason} 提示。币安不可达返回
 * {@code 503 BINANCE_UNREACHABLE}，提示需要配置可访问币安的网络（代理）。
 */
@RestController
@RequestMapping("/api/v1/bot")
public class BotController {

    private final BotProperties properties;
    private final BinanceMarketClient marketClient;
    private final BotTradingService tradingService;

    public BotController(BotProperties properties,
                         BinanceMarketClient marketClient,
                         BotTradingService tradingService) {
        this.properties = properties;
        this.marketClient = marketClient;
        this.tradingService = tradingService;
    }

    @GetMapping("/status")
    public BotStatusView status() {
        return view(true, null, null);
    }

    @PostMapping("/enable")
    public ResponseEntity<BotStatusView> enable() {
        EnableResult result = tradingService.enable();
        BotStatusView view = view(result.success(), result.reason(), result.message());
        if (result.success()) {
            return ResponseEntity.ok(view);
        }
        HttpStatus status = switch (result.reason()) {
            case "BINANCE_UNREACHABLE" -> HttpStatus.SERVICE_UNAVAILABLE;
            case "SYMBOL_NOT_TRADING" -> HttpStatus.CONFLICT;
            default -> HttpStatus.BAD_REQUEST;
        };
        return ResponseEntity.status(status).body(view);
    }

    @PostMapping("/disable")
    public BotStatusView disable() {
        tradingService.disable();
        return view(true, null, "自动交易已关闭");
    }

    private BotStatusView view(boolean success, String reason, String message) {
        List<BotQuoteView> quotes = new ArrayList<>();
        Map<String, Quote> latest = marketClient.quotes();
        for (String symbol : properties.symbols()) {
            Quote quote = latest.get(symbol);
            if (quote != null) {
                quotes.add(new BotQuoteView(
                        symbol,
                        quote.price().toPlainString(),
                        quote.receivedAt().toString()));
            }
        }
        Instant lastOrderAt = tradingService.lastOrderAt();
        return new BotStatusView(
                success,
                reason,
                message,
                properties.enabled(),
                tradingService.trading(),
                properties.userId(),
                properties.symbols(),
                properties.orderIntervalMs(),
                marketClient.connected(),
                marketClient.proxyDescription(),
                List.copyOf(quotes),
                tradingService.ordersPlaced(),
                tradingService.ordersRejected(),
                lastOrderAt == null ? null : lastOrderAt.toString(),
                tradingService.lastError(),
                marketClient.lastError());
    }
}
