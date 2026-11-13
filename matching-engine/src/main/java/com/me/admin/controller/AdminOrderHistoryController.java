package com.me.admin.controller;

import com.me.admin.service.OrderHistoryQueryService;
import com.me.admin.vo.AdminOrderHistoryQueryView;
import com.me.admin.vo.AdminOrderHistoryView;
import com.me.shared.order.OrderStatus;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 管理端委托历史接口。
 *
 * <p>{@code GET /api/v1/admin/order-history} 查询 QuestDB 中归档的终态委托
 * （撤单 / 撮合完成），按最终时间倒序。筛选参数均可选：{@code userId}、{@code symbol}、
 * {@code status}（CANCELED / FILLED）；{@code limit} 缺省 100、上限 1000。
 *
 * <p>这是演示用的管理接口，没有鉴权，部署到公网前必须加权限控制。
 */
@RestController
@RequestMapping("/api/v1/admin/order-history")
public class AdminOrderHistoryController {

    private static final Logger log = LoggerFactory.getLogger(AdminOrderHistoryController.class);

    private static final int MAX_SYMBOL_LENGTH = 32;

    private final OrderHistoryQueryService queryService;

    public AdminOrderHistoryController(OrderHistoryQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping
    public ResponseEntity<AdminOrderHistoryQueryView> list(
            @RequestParam(name = "userId", required = false) Long userId,
            @RequestParam(name = "symbol", required = false) String symbol,
            @RequestParam(name = "status", required = false) String status,
            @RequestParam(name = "limit", defaultValue = "100") int limit) {
        String normalizedSymbol = normalizeUpper(symbol);
        String normalizedStatus = normalizeUpper(status);

        if (userId != null && userId <= 0) {
            return badRequest(userId, normalizedSymbol, normalizedStatus, "USER_ID_INVALID");
        }
        if (normalizedSymbol != null
                && (normalizedSymbol.length() > MAX_SYMBOL_LENGTH
                || !normalizedSymbol.matches("[A-Z0-9-]+"))) {
            return badRequest(userId, normalizedSymbol, normalizedStatus, "SYMBOL_INVALID");
        }

        OrderStatus parsedStatus = null;
        if (normalizedStatus != null) {
            try {
                parsedStatus = OrderStatus.valueOf(normalizedStatus);
            } catch (IllegalArgumentException e) {
                return badRequest(userId, normalizedSymbol, normalizedStatus,
                        "STATUS_INVALID");
            }
            if (parsedStatus != OrderStatus.CANCELED && parsedStatus != OrderStatus.FILLED) {
                return badRequest(userId, normalizedSymbol, normalizedStatus,
                        "STATUS_INVALID");
            }
        }

        if (limit <= 0) {
            return badRequest(userId, normalizedSymbol, normalizedStatus, "LIMIT_INVALID");
        }

        try {
            List<AdminOrderHistoryView> orders = queryService
                    .list(userId, normalizedSymbol, parsedStatus, limit)
                    .stream()
                    .map(AdminOrderHistoryView::from)
                    .toList();
            return ResponseEntity.ok(AdminOrderHistoryQueryView.success(
                    userId, normalizedSymbol, normalizedStatus, orders));
        } catch (DataAccessException e) {
            log.error("admin order history query failed: userId={}, symbol={}, status={}",
                    userId, normalizedSymbol, normalizedStatus, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(AdminOrderHistoryQueryView.rejected(
                            userId, normalizedSymbol, normalizedStatus, "QUERY_FAILED"));
        }
    }

    private static String normalizeUpper(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.strip().toUpperCase(Locale.ROOT);
        return normalized.isEmpty() ? null : normalized;
    }

    private static ResponseEntity<AdminOrderHistoryQueryView> badRequest(
            Long userId, String symbol, String status, String reason) {
        return ResponseEntity.badRequest()
                .body(AdminOrderHistoryQueryView.rejected(userId, symbol, status, reason));
    }
}
