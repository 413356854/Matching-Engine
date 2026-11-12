package com.me.order.controller;

import com.me.order.service.OrderService;
import com.me.order.vo.OrderQueryView;
import com.me.order.vo.OrderView;
import java.util.List;
import java.util.Locale;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 委托查询接口：用户当前未完成委托。
 *
 * <p>下单接口已迁到币币模块的 {@code POST /api/v1/currency/orders}；该接口只向 Kafka
 * {@code me.order.commands-batch} 投递命令，委托模块的 {@code OrderCommandListener} 是唯一接单入口。
 *
 * <p>{@code GET /api/v1/orders?userId=...&symbol=...&limit=...} 查询用户当前未完成委托
 * （{@code ACCEPTED} / {@code PAID} / {@code PARTIALLY_FILLED}），按创建时间倒序。
 */
@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private static final int MAX_SYMBOL_LENGTH = 32;

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
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

    private static ResponseEntity<OrderQueryView> orderBadRequest(
            long userId, String symbol, String reason) {
        return ResponseEntity.badRequest().body(OrderQueryView.rejected(userId, symbol, reason));
    }
}
