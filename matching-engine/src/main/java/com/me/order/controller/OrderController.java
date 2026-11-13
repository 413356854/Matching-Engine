package com.me.order.controller;

import com.me.order.service.OrderService;
import com.me.order.vo.OrderHistoryQueryView;
import com.me.order.vo.OrderHistoryView;
import com.me.order.vo.OrderQueryView;
import com.me.order.vo.OrderView;
import com.me.shared.order.OrderHistoryQueryPort;
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
 * 委托查询接口：用户当前未完成委托与历史委托。
 *
 * <p>下单接口已迁到币币模块的 {@code POST /api/v1/currency/orders}；该接口只向 Kafka
 * {@code me.order.commands-batch} 投递命令，委托模块的 {@code OrderCommandListener} 是唯一接单入口。
 *
 * <p>{@code GET /api/v1/orders?userId=...&symbol=...&limit=...} 查询用户当前未完成委托
 * （{@code ACCEPTED} / {@code PAID} / {@code PARTIALLY_FILLED}），按创建时间倒序。
 *
 * <p>{@code GET /api/v1/orders/history?userId=...&symbol=...&limit=...} 通过 shared 的
 * {@link OrderHistoryQueryPort} 查询 QuestDB {@code ord_history} 中已撤单 / 已撮合完成的委托，
 * 按最终时间倒序。委托模块不直接访问该表，QuestDB 访问层由 admin 模块负责。
 */
@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private static final int MAX_SYMBOL_LENGTH = 32;

    private final OrderService orderService;
    private final OrderHistoryQueryPort historyQueryPort;

    public OrderController(OrderService orderService,
                           OrderHistoryQueryPort historyQueryPort) {
        this.orderService = orderService;
        this.historyQueryPort = historyQueryPort;
    }

    /**
     * 查询用户当前未完成委托（等待冻结 / 撮合中 / 部分成交），按创建时间倒序。
     *
     * <p>{@code symbol} 可选，不传时返回该用户全部交易对的当前委托；{@code limit} 缺省 100、
     * 服务端上限 200。
     */
    @GetMapping
    public ResponseEntity<OrderQueryView> openOrders(
            @RequestParam(name = "userId", required = false) Long userId,
            @RequestParam(name = "symbol", required = false) String symbol,
            @RequestParam(name = "limit", defaultValue = "100") int limit) {
        String normalizedSymbol = symbol == null ? null : symbol.strip().toUpperCase(Locale.ROOT);
        if (normalizedSymbol != null && normalizedSymbol.isEmpty()) {
            normalizedSymbol = null;
        }

        if (userId == null) {
            return orderBadRequest(0, normalizedSymbol, "USER_ID_REQUIRED");
        }
        if (userId <= 0) {
            return orderBadRequest(userId, normalizedSymbol, "USER_ID_INVALID");
        }
        if (normalizedSymbol != null
                && (normalizedSymbol.length() > MAX_SYMBOL_LENGTH
                || !normalizedSymbol.matches("[A-Z0-9-]+"))) {
            return orderBadRequest(userId, normalizedSymbol, "SYMBOL_INVALID");
        }
        if (limit <= 0) {
            return orderBadRequest(userId, normalizedSymbol, "LIMIT_INVALID");
        }

        List<OrderView> orders = orderService.openOrders(userId, normalizedSymbol, limit);
        return ResponseEntity.ok(OrderQueryView.success(userId, normalizedSymbol, orders));
    }

    /**
     * 查询用户历史委托（撤单 / 撮合完成），按最终时间倒序。
     *
     * <p>{@code symbol} 可选，不传时返回该用户全部交易对的历史委托；{@code limit} 缺省 50、
     * 服务端上限 1000。数据来自 admin 模块归档的 QuestDB {@code ord_history}。
     */
    @GetMapping("/history")
    public ResponseEntity<OrderHistoryQueryView> history(
            @RequestParam(name = "userId", required = false) Long userId,
            @RequestParam(name = "symbol", required = false) String symbol,
            @RequestParam(name = "limit", defaultValue = "50") int limit) {
        String normalizedSymbol = symbol == null ? null : symbol.strip().toUpperCase(Locale.ROOT);
        if (normalizedSymbol != null && normalizedSymbol.isEmpty()) {
            normalizedSymbol = null;
        }

        if (userId == null) {
            return historyBadRequest(0, normalizedSymbol, "USER_ID_REQUIRED");
        }
        if (userId <= 0) {
            return historyBadRequest(userId, normalizedSymbol, "USER_ID_INVALID");
        }
        if (normalizedSymbol != null
                && (normalizedSymbol.length() > MAX_SYMBOL_LENGTH
                || !normalizedSymbol.matches("[A-Z0-9-]+"))) {
            return historyBadRequest(userId, normalizedSymbol, "SYMBOL_INVALID");
        }
        if (limit <= 0) {
            return historyBadRequest(userId, normalizedSymbol, "LIMIT_INVALID");
        }

        try {
            List<OrderHistoryView> orders = historyQueryPort
                    .listByUser(userId, normalizedSymbol, limit)
                    .stream()
                    .map(OrderHistoryView::from)
                    .toList();
            return ResponseEntity.ok(
                    OrderHistoryQueryView.success(userId, normalizedSymbol, orders));
        } catch (DataAccessException e) {
            log.error("order history query failed: userId={}, symbol={}, limit={}",
                    userId, normalizedSymbol, limit, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(OrderHistoryQueryView.rejected(
                            userId, normalizedSymbol, "QUERY_FAILED"));
        }
    }

    private static ResponseEntity<OrderHistoryQueryView> historyBadRequest(
            long userId, String symbol, String reason) {
        return ResponseEntity.badRequest()
                .body(OrderHistoryQueryView.rejected(userId, symbol, reason));
    }

    private static ResponseEntity<OrderQueryView> orderBadRequest(
            long userId, String symbol, String reason) {
        return ResponseEntity.badRequest().body(OrderQueryView.rejected(userId, symbol, reason));
    }
}
