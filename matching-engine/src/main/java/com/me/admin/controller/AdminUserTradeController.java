package com.me.admin.controller;

import com.me.admin.service.UserTradeQueryService;
import com.me.admin.vo.AdminUserTradeQueryView;
import com.me.admin.vo.AdminUserTradeView;
import com.me.shared.trade.UserTradeRole;
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
 * 管理端用户成交记录接口。
 *
 * <p>{@code GET /api/v1/admin/user-trades} 查询 QuestDB 中 admin 归档的用户成交记录，
 * 按成交时间倒序。筛选参数均可选：{@code userId}、{@code symbol}、{@code role}
 * （TAKER / MAKER）；{@code limit} 缺省 100、上限 1000。
 *
 * <p>每个撮合对拆成 taker / maker 两条记录：{@code role=TAKER} 是主动方，
 * {@code role=MAKER} 是被动方。这是演示用的管理接口，没有鉴权，部署到公网前必须加权限控制。
 */
@RestController
@RequestMapping("/api/v1/admin/user-trades")
public class AdminUserTradeController {

    private static final Logger log = LoggerFactory.getLogger(AdminUserTradeController.class);

    private static final int MAX_SYMBOL_LENGTH = 32;

    private final UserTradeQueryService queryService;

    public AdminUserTradeController(UserTradeQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping
    public ResponseEntity<AdminUserTradeQueryView> list(
            @RequestParam(name = "userId", required = false) Long userId,
            @RequestParam(name = "symbol", required = false) String symbol,
            @RequestParam(name = "role", required = false) String role,
            @RequestParam(name = "limit", defaultValue = "100") int limit) {
        String normalizedSymbol = normalizeUpper(symbol);
        String normalizedRole = normalizeUpper(role);

        if (userId != null && userId <= 0) {
            return badRequest(userId, normalizedSymbol, normalizedRole, "USER_ID_INVALID");
        }
        if (normalizedSymbol != null
                && (normalizedSymbol.length() > MAX_SYMBOL_LENGTH
                || !normalizedSymbol.matches("[A-Z0-9-]+"))) {
            return badRequest(userId, normalizedSymbol, normalizedRole, "SYMBOL_INVALID");
        }

        UserTradeRole parsedRole = null;
        if (normalizedRole != null) {
            try {
                parsedRole = UserTradeRole.valueOf(normalizedRole);
            } catch (IllegalArgumentException e) {
                return badRequest(userId, normalizedSymbol, normalizedRole, "ROLE_INVALID");
            }
        }

        if (limit <= 0) {
            return badRequest(userId, normalizedSymbol, normalizedRole, "LIMIT_INVALID");
        }

        try {
            List<AdminUserTradeView> trades = queryService
                    .list(userId, normalizedSymbol, parsedRole, limit)
                    .stream()
                    .map(AdminUserTradeView::from)
                    .toList();
            return ResponseEntity.ok(AdminUserTradeQueryView.success(
                    userId, normalizedSymbol, normalizedRole, trades));
        } catch (DataAccessException e) {
            log.error("admin user trade query failed: userId={}, symbol={}, role={}",
                    userId, normalizedSymbol, normalizedRole, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(AdminUserTradeQueryView.rejected(
                            userId, normalizedSymbol, normalizedRole, "QUERY_FAILED"));
        }
    }

    private static String normalizeUpper(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.strip().toUpperCase(Locale.ROOT);
        return normalized.isEmpty() ? null : normalized;
    }

    private static ResponseEntity<AdminUserTradeQueryView> badRequest(
            Long userId, String symbol, String role, String reason) {
        return ResponseEntity.badRequest()
                .body(AdminUserTradeQueryView.rejected(userId, symbol, role, reason));
    }
}
