package com.me.currency.controller;

import com.me.currency.dto.PlaceOrderRequest;
import com.me.currency.mq.OrderCommandPublisher;
import com.me.currency.service.CurrencyConfigCache;
import com.me.currency.service.OrderIdGenerator;
import com.me.currency.vo.CurrencyView;
import com.me.currency.vo.PlaceOrderSubmissionView;
import com.me.currency.vo.TradingPairView;
import com.me.shared.money.Amounts;
import com.me.shared.order.OrderCommand;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderType;
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
 * 币币模块下单接口：同步校验后只向 {@code me.order.commands-batch} 投递委托命令。
 *
 * <p><b>禁止直接调用委托模块</b>：本接口不保存委托、不冻结资金，也不 import 任何
 * {@code com.me.order} 类型；委托模块的 {@code OrderCommandListener} 消费命令，是系统唯一的
 * 下单入口。
 *
 * <p>{@code POST /api/v1/currency/orders} 成功返回 {@code 202 Accepted} 和命令回执
 * （requestId / orderId / symbol）。命令先非阻塞写入进程内 Disruptor，再由聚合线程按
 * 「2000 条或 20ms」组装成 {@code OrderCommandBatch} 发往 Kafka；202 只表示命令已进入
 * ring，不等待 broker ack。冻结失败、重复委托、撮合结果等由委托模块异步推进，
 * 不会通过本接口返回。
 */
@RestController
@RequestMapping("/api/v1/currency/orders")
public class OrderPlacementController {

    private static final Logger log = LoggerFactory.getLogger(OrderPlacementController.class);

    private static final int STATUS_ONLINE = 1;

    private final CurrencyConfigCache configCache;
    private final OrderIdGenerator orderIdGenerator;
    private final OrderCommandPublisher orderCommandPublisher;
    private final boolean orderPublishEnabled;

    public OrderPlacementController(
            CurrencyConfigCache configCache,
            OrderIdGenerator orderIdGenerator,
            OrderCommandPublisher orderCommandPublisher,
            @Value("${app.currency.order-publish-enabled:true}") boolean orderPublishEnabled) {
        this.configCache = configCache;
        this.orderIdGenerator = orderIdGenerator;
        this.orderCommandPublisher = orderCommandPublisher;
        this.orderPublishEnabled = orderPublishEnabled;
    }

    @PostMapping
    public ResponseEntity<PlaceOrderSubmissionView> place(
            @RequestBody(required = false) PlaceOrderRequest request) {
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

        long price = request.type() == OrderType.MARKET ? 0L : request.price();
        // 市价买入的预算：前端按卖一价（没有则最新价）× 1.05 × 数量算出，后端只做冻结与撮合。
        long marketBuyAmount = isMarketBuy(request) ? request.amount() : 0L;
        CurrencyView quoteCurrency = configCache.currency(pair.quoteCurrency());
        int quoteScale = quoteCurrency == null ? 0 : quoteCurrency.scale();
        String pairInvalidReason = validateAgainstPair(
                pair, price, request.quantity(), marketBuyAmount, quoteScale);
        if (pairInvalidReason != null) {
            return badRequest(requestId, pairInvalidReason);
        }

        long orderId = request.orderId() == null ? orderIdGenerator.nextId() : request.orderId();
        OrderCommand command = new OrderCommand(
                requestId, orderId, request.userId(), symbol,
                request.side(), request.type(), price, request.quantity(),
                pair.priceScale(), pair.quantityScale(), quoteScale, marketBuyAmount);

        if (orderPublishEnabled) {
            try {
                orderCommandPublisher.publish(command);
            } catch (RuntimeException e) {
                log.error("order command publish failed: requestId={}, orderId={}, symbol={}",
                        requestId, orderId, symbol, e);
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .body(PlaceOrderSubmissionView.rejected(
                                requestId, orderId, symbol, "ORDER_COMMAND_PUBLISH_FAILED"));
            }
        }

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(PlaceOrderSubmissionView.accepted(requestId, orderId, symbol));
    }

    private static ResponseEntity<PlaceOrderSubmissionView> badRequest(
            String requestId, String reason) {
        return ResponseEntity.badRequest()
                .body(PlaceOrderSubmissionView.rejected(requestId, reason));
    }

    /** 基础参数校验；通过返回 {@code null}。 */
    private static String validate(PlaceOrderRequest request) {
        if (request == null) {
            return "REQUEST_REQUIRED";
        }
        if (request.userId() == null) {
            return "USER_ID_REQUIRED";
        }
        if (request.userId() <= 0) {
            return "USER_ID_INVALID";
        }
        if (request.symbol() == null || request.symbol().isBlank()) {
            return "SYMBOL_REQUIRED";
        }
        if (request.side() == null) {
            return "SIDE_REQUIRED";
        }
        if (request.type() == null) {
            return "TYPE_REQUIRED";
        }
        if (request.quantity() == null) {
            return "QUANTITY_REQUIRED";
        }
        if (request.quantity() <= 0) {
            return "QUANTITY_INVALID";
        }
        if (request.orderId() != null && request.orderId() <= 0) {
            return "ORDER_ID_INVALID";
        }
        if (request.type() == OrderType.LIMIT) {
            if (request.price() == null || request.price() <= 0) {
                return "PRICE_REQUIRED_FOR_LIMIT";
            }
        } else if (request.price() != null && request.price() != 0) {
            return "PRICE_NOT_ALLOWED_FOR_MARKET";
        }
        if (isMarketBuy(request)) {
            // 市价买入必须携带计价币预算，否则无法冻结资金。
            if (request.amount() == null || request.amount() <= 0) {
                return "AMOUNT_REQUIRED_FOR_MARKET_BUY";
            }
        } else if (request.amount() != null && request.amount() != 0) {
            return "AMOUNT_NOT_ALLOWED";
        }
        return null;
    }

    /** 按交易对配置校验最小下单量和最小下单金额；通过返回 {@code null}。 */
    private static String validateAgainstPair(TradingPairView pair, long price, long quantity,
                                              long marketBuyAmount, int quoteScale) {
        if (pair.minOrderQuantity() > 0 && quantity < pair.minOrderQuantity()) {
            return "BELOW_MIN_ORDER_QUANTITY";
        }
        if (marketBuyAmount > 0) {
            // 市价买入直接以预算金额做最小金额校验；预算已由前端换算成计价币最小单位。
            if (pair.minOrderAmount() > 0 && marketBuyAmount < pair.minOrderAmount()) {
                return "BELOW_MIN_ORDER_AMOUNT";
            }
            return null;
        }
        if (price > 0) {
            // 下限价单先把 price × quantity 换算成计价币金额：既用于最小金额校验，
            // 也在同步阶段拦截溢出，避免异步消费时整批失败重投。
            long amount;
            try {
                amount = Amounts.quoteAmount(price, quantity,
                        pair.priceScale(), pair.quantityScale(), quoteScale);
            } catch (ArithmeticException | IllegalArgumentException e) {
                return "AMOUNT_OVERFLOW";
            }
            if (pair.minOrderAmount() > 0 && amount < pair.minOrderAmount()) {
                return "BELOW_MIN_ORDER_AMOUNT";
            }
        }
        return null;
    }

    private static boolean isMarketBuy(PlaceOrderRequest request) {
        return request.side() == OrderSide.BUY && request.type() == OrderType.MARKET;
    }

    private static String normalizeRequestId(String requestId) {
        return requestId == null || requestId.isBlank()
                ? UUID.randomUUID().toString()
                : requestId.strip();
    }
}
