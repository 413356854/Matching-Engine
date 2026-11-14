package com.me.currency.controller;

import com.me.currency.vo.UserTradeQueryView;
import com.me.currency.vo.UserTradeView;
import com.me.shared.trade.UserTradeQueryPort;
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
 * 币币交易页面的用户历史成交接口。
 *
 * <p>{@code GET /api/v1/currency/user-trades?userId=1001&symbol=BTC-USDT&limit=50}
 * 通过 shared 的 {@link UserTradeQueryPort} 查询 QuestDB {@code user_trade} 投影，
 * 按成交时间倒序返回（最新记录在前）。交易对可选，{@code limit} 缺省 50、服务端上限 1000。
 *
 * <p>每个撮合对拆成 taker / maker 两条记录，接口按 {@code userId} 只返回当前用户自己的
 * 一侧；币币模块不直接访问 {@code user_trade} 表，QuestDB 访问层仍由 admin 模块负责。
 */
@RestController
@RequestMapping("/api/v1/currency/user-trades")
public class UserTradeController {

    private static final Logger log = LoggerFactory.getLogger(UserTradeController.class);

    private static final int MAX_SYMBOL_LENGTH = 32;

    private final UserTradeQueryPort queryPort;

    public UserTradeController(UserTradeQueryPort queryPort) {
        this.queryPort = queryPort;
    }

    @GetMapping
    public ResponseEntity<UserTradeQueryView> list(
            @RequestParam(name = "userId", required = false) Long userId,
            @RequestParam(name = "symbol", required = false) String symbol,
            @RequestParam(name = "limit", defaultValue = "50") int limit) {
        String normalizedSymbol = normalizeUpper(symbol);

        if (userId == null) {
            return badRequest(null, normalizedSymbol, "USER_ID_REQUIRED");
        }
        if (userId <= 0) {
            return badRequest(userId, normalizedSymbol, "USER_ID_INVALID");
        }
        if (normalizedSymbol != null
                && (normalizedSymbol.length() > MAX_SYMBOL_LENGTH
                || !normalizedSymbol.matches("[A-Z0-9-]+"))) {
            return badRequest(userId, normalizedSymbol, "SYMBOL_INVALID");
        }
        if (limit <= 0) {
            return badRequest(userId, normalizedSymbol, "LIMIT_INVALID");
        }

        try {
            List<UserTradeView> trades = queryPort.listByUser(userId, normalizedSymbol, limit)
                    .stream()
                    .map(UserTradeView::from)
                    .toList();
            return ResponseEntity.ok(
                    UserTradeQueryView.success(userId, normalizedSymbol, trades));
        } catch (DataAccessException e) {
            log.error("user trade query failed: userId={}, symbol={}, limit={}",
                    userId, normalizedSymbol, limit, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(UserTradeQueryView.rejected(
                            userId, normalizedSymbol, "QUERY_FAILED"));
        }
    }

    private static String normalizeUpper(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.strip().toUpperCase(Locale.ROOT);
        return normalized.isEmpty() ? null : normalized;
    }

    private static ResponseEntity<UserTradeQueryView> badRequest(
            Long userId, String symbol, String reason) {
        return ResponseEntity.badRequest()
                .body(UserTradeQueryView.rejected(userId, symbol, reason));
    }
}
