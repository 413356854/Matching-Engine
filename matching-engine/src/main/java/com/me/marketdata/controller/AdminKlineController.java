package com.me.marketdata.controller;

import com.me.marketdata.entity.KlineInterval;
import com.me.marketdata.service.MarketDataAdminService;
import com.me.marketdata.vo.AdminKlineClearView;
import com.me.marketdata.vo.AdminKlineQueryView;
import com.me.marketdata.vo.KlineView;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 管理端 K 线接口。
 *
 * <p>{@code GET /api/v1/admin/klines} 查询 QuestDB 中归档的 K 线，按 bucket 起点倒序。
 * 筛选参数均可选：{@code symbol}、{@code interval}；{@code limit} 缺省 100、上限 1000。
 *
 * <p>{@code DELETE /api/v1/admin/klines} 一键清空全部 K 线（{@code TRUNCATE TABLE md_kline}），
 * 返回清空前的记录数；成交明细 {@code md_trade} 不受影响。
 *
 * <p>这是演示用的管理接口，没有鉴权，部署到公网前必须加权限控制。
 */
@RestController
@RequestMapping("/api/v1/admin/klines")
public class AdminKlineController {

    private static final Logger log = LoggerFactory.getLogger(AdminKlineController.class);

    private static final int MAX_SYMBOL_LENGTH = 32;

    private final MarketDataAdminService adminService;

    public AdminKlineController(MarketDataAdminService adminService) {
        this.adminService = adminService;
    }

    @GetMapping
    public ResponseEntity<AdminKlineQueryView> list(
            @RequestParam(name = "symbol", required = false) String symbol,
            @RequestParam(name = "interval", required = false) String interval,
            @RequestParam(name = "limit", defaultValue = "100") int limit) {
        String normalizedSymbol = normalizeUpper(symbol);
        String normalizedInterval = normalizeLower(interval);

        if (normalizedSymbol != null
                && (normalizedSymbol.length() > MAX_SYMBOL_LENGTH
                || !normalizedSymbol.matches("[A-Z0-9-]+"))) {
            return badRequest(normalizedSymbol, normalizedInterval, "SYMBOL_INVALID");
        }

        KlineInterval parsedInterval = null;
        if (normalizedInterval != null) {
            try {
                parsedInterval = KlineInterval.of(normalizedInterval);
            } catch (IllegalArgumentException e) {
                return badRequest(normalizedSymbol, normalizedInterval, "INTERVAL_INVALID");
            }
        }

        if (limit <= 0) {
            return badRequest(normalizedSymbol, normalizedInterval, "LIMIT_INVALID");
        }

        try {
            List<KlineView> klines = adminService
                    .recentKlines(normalizedSymbol, parsedInterval, limit)
                    .stream()
                    .map(KlineView::from)
                    .toList();
            return ResponseEntity.ok(AdminKlineQueryView.success(
                    normalizedSymbol, normalizedInterval, klines));
        } catch (DataAccessException e) {
            log.error("admin kline query failed: symbol={}, interval={}",
                    normalizedSymbol, normalizedInterval, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(AdminKlineQueryView.rejected(
                            normalizedSymbol, normalizedInterval, "QUERY_FAILED"));
        }
    }

    @DeleteMapping
    public ResponseEntity<AdminKlineClearView> clear() {
        try {
            long deletedCount = adminService.clearKlines();
            return ResponseEntity.ok(AdminKlineClearView.success(deletedCount));
        } catch (DataAccessException e) {
            log.error("admin kline clear failed", e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(AdminKlineClearView.rejected("CLEAR_FAILED"));
        }
    }

    private static String normalizeUpper(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.strip().toUpperCase(Locale.ROOT);
        return normalized.isEmpty() ? null : normalized;
    }

    private static String normalizeLower(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.strip().toLowerCase(Locale.ROOT);
        return normalized.isEmpty() ? null : normalized;
    }

    private static ResponseEntity<AdminKlineQueryView> badRequest(
            String symbol, String interval, String reason) {
        return ResponseEntity.badRequest()
                .body(AdminKlineQueryView.rejected(symbol, interval, reason));
    }
}
