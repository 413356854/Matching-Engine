package com.me.currency.controller;

import com.me.currency.vo.UserFundsFlowQueryView;
import com.me.currency.vo.UserFundsFlowView;
import com.me.shared.flow.FundsFlowHistoryQueryPort;
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
 * 币币交易页面的用户历史资金流水接口。
 *
 * <p>{@code GET /api/v1/currency/funds-flows?userId=1001&currency=USDT&limit=50}
 * 通过 shared 的 {@link FundsFlowHistoryQueryPort} 查询 QuestDB {@code funds_flow} 投影，
 * 按发生时间倒序返回（最新记录在前）。币种可选，{@code limit} 缺省 50、服务端上限 1000。
 *
 * <p>币币模块不直接访问 {@code funds_flow} 表：查询契约在 shared，QuestDB 访问层与写入
 * 仍由 admin 模块负责。
 */
@RestController
@RequestMapping("/api/v1/currency/funds-flows")
public class UserFundsFlowController {

    private static final Logger log = LoggerFactory.getLogger(UserFundsFlowController.class);

    private static final int MAX_CURRENCY_LENGTH = 16;

    private final FundsFlowHistoryQueryPort queryPort;

    public UserFundsFlowController(FundsFlowHistoryQueryPort queryPort) {
        this.queryPort = queryPort;
    }

    @GetMapping
    public ResponseEntity<UserFundsFlowQueryView> list(
            @RequestParam(name = "userId", required = false) Long userId,
            @RequestParam(name = "currency", required = false) String currency,
            @RequestParam(name = "limit", defaultValue = "50") int limit) {
        String normalizedCurrency = normalizeUpper(currency);

        if (userId == null) {
            return badRequest(null, normalizedCurrency, "USER_ID_REQUIRED");
        }
        if (userId <= 0) {
            return badRequest(userId, normalizedCurrency, "USER_ID_INVALID");
        }
        if (normalizedCurrency != null
                && (normalizedCurrency.length() > MAX_CURRENCY_LENGTH
                || !normalizedCurrency.matches("[A-Z0-9]+"))) {
            return badRequest(userId, normalizedCurrency, "CURRENCY_INVALID");
        }
        if (limit <= 0) {
            return badRequest(userId, normalizedCurrency, "LIMIT_INVALID");
        }

        try {
            List<UserFundsFlowView> flows = queryPort.list(userId, normalizedCurrency, limit)
                    .stream()
                    .map(UserFundsFlowView::from)
                    .toList();
            return ResponseEntity.ok(
                    UserFundsFlowQueryView.success(userId, normalizedCurrency, flows));
        } catch (DataAccessException e) {
            log.error("user funds flow query failed: userId={}, currency={}, limit={}",
                    userId, normalizedCurrency, limit, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(UserFundsFlowQueryView.rejected(
                            userId, normalizedCurrency, "QUERY_FAILED"));
        }
    }

    private static String normalizeUpper(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.strip().toUpperCase(Locale.ROOT);
        return normalized.isEmpty() ? null : normalized;
    }

    private static ResponseEntity<UserFundsFlowQueryView> badRequest(
            Long userId, String currency, String reason) {
        return ResponseEntity.badRequest()
                .body(UserFundsFlowQueryView.rejected(userId, currency, reason));
    }
}
