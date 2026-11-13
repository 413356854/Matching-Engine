package com.me.currency.controller;

import com.me.currency.dto.CancelOrderRequest;
import com.me.currency.mq.OrderCancelCommandPublisher;
import com.me.currency.service.CurrencyConfigCache;
import com.me.currency.vo.CancelOrderSubmissionView;
import com.me.currency.vo.TradingPairView;
import com.me.shared.order.OrderCancelCommand;
import java.util.Locale;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 币币模块撤单接口：同步校验后只向 {@code me.order.cancels-batch} 投递撤单命令。
 *
 * <p><b>禁止直接调用委托模块</b>：本接口不查委托、不修改委托状态、不解冻资金，也不 import
 * 任何 {@code com.me.order} 类型；委托模块的 {@code OrderCancelListener} 消费命令，是系统
 * 唯一的撤单入口。
 *
 * <p>{@code POST /api/v1/currency/orders/cancel} 成功返回 {@code 202 Accepted} 和命令回执
 * （requestId / orderId / symbol）。命令先非阻塞写入<b>独立的撤单 Disruptor</b>，再由聚合
 * 线程按「单 symbol 1000 条或 20ms」把 map 中所有 symbol 的撤单分别封装成
 * {@code OrderCancelBatch} 发往 Kafka；202 只表示命令已进入 ring，不等待 broker ack。
 * 委托是否可撤、解冻结果等由委托模块异步推进，不会通过本接口返回。
 */
@RestController
@RequestMapping("/api/v1/currency/orders")
public class OrderCancelController {

    private static final Logger log = LoggerFactory.getLogger(OrderCancelController.class);

    private static final int STATUS_ONLINE = 1;

    private final CurrencyConfigCache configCache;
    private final OrderCancelCommandPublisher cancelCommandPublisher;
    private final boolean cancelPublishEnabled;

    public OrderCancelController(
            CurrencyConfigCache configCache,
            OrderCancelCommandPublisher cancelCommandPublisher,
            @Value("${app.currency.cancel-publish-enabled:true}") boolean cancelPublishEnabled) {
        this.configCache = configCache;
        this.cancelCommandPublisher = cancelCommandPublisher;
        this.cancelPublishEnabled = cancelPublishEnabled;
    }

    @PostMapping("/cancel")
    public ResponseEntity<CancelOrderSubmissionView> cancel(
            @RequestBody(required = false) CancelOrderRequest request) {
        String requestId = request == null ? null : normalizeRequestId(request.requestId());
        String invalidReason = validate(request);
        if (invalidReason != null) {
            return badRequest(requestId, invalidReason);
        }

        String symbol = request.symbol().strip().toUpperCase(Locale.ROOT);
        TradingPairView pair = configCache.pair(symbol);
        if (pair == null) {
            return badRequest(requestId, "SYMBOL_NOT_FOUND");
        }
        if (pair.status() != STATUS_ONLINE) {
            return badRequest(requestId, "SYMBOL_NOT_TRADING");
        }

        long orderId = request.orderId();
        long userId = request.userId() == null ? 0L : request.userId();
        OrderCancelCommand command = new OrderCancelCommand(requestId, orderId, userId, symbol);

        if (cancelPublishEnabled) {
            try {
                cancelCommandPublisher.publish(command);
            } catch (RuntimeException e) {
                log.error("order cancel command publish failed: requestId={}, orderId={}, symbol={}",
                        requestId, orderId, symbol, e);
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .body(CancelOrderSubmissionView.rejected(
                                requestId, orderId, symbol, "CANCEL_COMMAND_PUBLISH_FAILED"));
            }
        }

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(CancelOrderSubmissionView.accepted(requestId, orderId, symbol));
    }

    private static ResponseEntity<CancelOrderSubmissionView> badRequest(
            String requestId, String reason) {
        return ResponseEntity.badRequest()
                .body(CancelOrderSubmissionView.rejected(requestId, reason));
    }

    /** 基础参数校验；通过返回 {@code null}。 */
    private static String validate(CancelOrderRequest request) {
        if (request == null) {
            return "REQUEST_REQUIRED";
        }
        if (request.orderId() == null) {
            return "ORDER_ID_REQUIRED";
        }
        if (request.orderId() <= 0) {
            return "ORDER_ID_INVALID";
        }
        if (request.symbol() == null || request.symbol().isBlank()) {
            return "SYMBOL_REQUIRED";
        }
        if (request.userId() != null && request.userId() <= 0) {
            return "USER_ID_INVALID";
        }
        return null;
    }

    private static String normalizeRequestId(String requestId) {
        return requestId == null || requestId.isBlank()
                ? UUID.randomUUID().toString()
                : requestId.strip();
    }
}
