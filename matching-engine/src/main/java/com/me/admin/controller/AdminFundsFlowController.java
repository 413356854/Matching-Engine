package com.me.admin.controller;

import com.me.admin.service.FundsFlowQueryService;
import com.me.admin.vo.FundsFlowClearView;
import com.me.admin.vo.FundsFlowQueryView;
import com.me.admin.vo.FundsFlowView;
import com.me.shared.event.FundsLogType;
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
 * 管理端资金流水接口。
 *
 * <p>{@code GET /api/v1/admin/funds-flows} 查询 QuestDB 中归档的流水列表，按发生时间倒序。
 * 筛选参数均可选：{@code userId}、{@code currency}、{@code logType}；
 * {@code limit} 缺省 100、上限 1000。
 *
 * <p>{@code DELETE /api/v1/admin/funds-flows} 一键清空全部流水。QuestDB 不支持行级
 * {@code DELETE}，实现为 {@code TRUNCATE TABLE funds_flow}，返回清空前的记录数。
 *
 * <p>这是演示用的管理接口，没有鉴权，部署到公网前必须加权限控制。
 */
@RestController
@RequestMapping("/api/v1/admin/funds-flows")
public class AdminFundsFlowController {

    private static final Logger log = LoggerFactory.getLogger(AdminFundsFlowController.class);

    private static final int MAX_CURRENCY_LENGTH = 16;

    private final FundsFlowQueryService queryService;

    public AdminFundsFlowController(FundsFlowQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping
    public ResponseEntity<FundsFlowQueryView> list(
            @RequestParam(name = "userId", required = false) Long userId,
            @RequestParam(name = "currency", required = false) String currency,
            @RequestParam(name = "logType", required = false) String logType,
            @RequestParam(name = "limit", defaultValue = "100") int limit) {
        String normalizedCurrency = normalizeUpper(currency);
        String normalizedLogType = normalizeUpper(logType);

        if (userId != null && userId <= 0) {
            return badRequest(userId, normalizedCurrency, normalizedLogType, "USER_ID_INVALID");
        }
        if (normalizedCurrency != null
                && (normalizedCurrency.length() > MAX_CURRENCY_LENGTH
                || !normalizedCurrency.matches("[A-Z0-9]+"))) {
            return badRequest(userId, normalizedCurrency, normalizedLogType, "CURRENCY_INVALID");
        }

        FundsLogType parsedLogType = null;
        if (normalizedLogType != null) {
            try {
                parsedLogType = FundsLogType.valueOf(normalizedLogType);
            } catch (IllegalArgumentException e) {
                return badRequest(userId, normalizedCurrency, normalizedLogType,
                        "LOG_TYPE_INVALID");
            }
        }

        if (limit <= 0) {
            return badRequest(userId, normalizedCurrency, normalizedLogType, "LIMIT_INVALID");
        }

        try {
            List<FundsFlowView> flows = queryService
                    .list(userId, normalizedCurrency, parsedLogType, limit)
                    .stream()
                    .map(FundsFlowView::from)
                    .toList();
            return ResponseEntity.ok(FundsFlowQueryView.success(
                    userId, normalizedCurrency, normalizedLogType, flows));
        } catch (DataAccessException e) {
            log.error("admin funds flow query failed: userId={}, currency={}, logType={}",
                    userId, normalizedCurrency, normalizedLogType, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(FundsFlowQueryView.rejected(
                            userId, normalizedCurrency, normalizedLogType, "QUERY_FAILED"));
        }
    }

    @DeleteMapping
    public ResponseEntity<FundsFlowClearView> clear() {
        try {
            long deletedCount = queryService.clearAll();
            return ResponseEntity.ok(FundsFlowClearView.success(deletedCount));
        } catch (DataAccessException e) {
            log.error("admin funds flow clear failed", e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(FundsFlowClearView.rejected("CLEAR_FAILED"));
        }
    }

    private static String normalizeUpper(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.strip().toUpperCase(Locale.ROOT);
        return normalized.isEmpty() ? null : normalized;
    }

    private static ResponseEntity<FundsFlowQueryView> badRequest(
            Long userId, String currency, String logType, String reason) {
        return ResponseEntity.badRequest()
                .body(FundsFlowQueryView.rejected(userId, currency, logType, reason));
    }
}
