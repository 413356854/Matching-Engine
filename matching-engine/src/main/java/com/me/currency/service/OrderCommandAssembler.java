package com.me.currency.service;

import com.me.currency.dto.PlaceOrderRequest;
import com.me.currency.vo.CurrencyView;
import com.me.currency.vo.TradingPairView;
import com.me.shared.marketdata.LatestPriceQueryPort;
import com.me.shared.money.Amounts;
import com.me.shared.order.OrderCommand;
import com.me.shared.order.OrderRequestLimits;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderType;
import java.util.Locale;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * 下单请求的共享组装器：单笔接口与批量测试接口共用。
 *
 * <p>负责同步校验、补齐交易对 / 币种 raw_scale、生成委托号，并组装成
 * {@link OrderCommand}。<b>委托号只在这里生成</b>，请求不携带 orderId。
 *
 * <p>只做同步校验，不保存委托、不冻结资金、不发送 Kafka；命令由调用方交给
 * {@code OrderCommandPublisher}（进程内 Disruptor）。
 */
@Component
public class OrderCommandAssembler {

    private static final int STATUS_ONLINE = 1;

    private final CurrencyConfigCache configCache;
    private final OrderIdGenerator orderIdGenerator;
    private final LatestPriceQueryPort latestPriceQuery;

    public OrderCommandAssembler(CurrencyConfigCache configCache,
                                 OrderIdGenerator orderIdGenerator,
                                 LatestPriceQueryPort latestPriceQuery) {
        this.configCache = configCache;
        this.orderIdGenerator = orderIdGenerator;
        this.latestPriceQuery = latestPriceQuery;
    }

    /**
     * 校验并组装一条下单命令。
     *
     * <p>{@code accepted()=false} 时 {@link Result#reason()} 是拒绝原因；此时
     * {@link Result#command()} / {@link Result#orderId()} 为 {@code null}。
     */
    public Result assemble(PlaceOrderRequest request) {
        String requestId = request == null ? null : normalizeRequestId(request.requestId());
        String invalidReason = validate(request);
        if (invalidReason != null) {
            return new Result(requestId, null, null, null, invalidReason);
        }

        String symbol = request.symbol().strip().toUpperCase(Locale.ROOT);
        TradingPairView pair = configCache.pair(symbol);
        if (pair == null) {
            return new Result(requestId, null, symbol, null, "SYMBOL_NOT_FOUND");
        }
        if (pair.status() != STATUS_ONLINE) {
            return new Result(requestId, null, symbol, null, "SYMBOL_NOT_TRADING");
        }

        long price = request.type() == OrderType.MARKET ? 0L : request.price();
        // 市价买入的预算：前端按卖一价（没有则最新价）× 1.05 × 数量算出，后端只做冻结与撮合。
        long marketBuyAmount = isMarketBuy(request) ? request.amount() : 0L;
        CurrencyView quoteCurrency = configCache.currency(pair.quoteCurrency());
        // amount 的原始整数来自计价币 raw_scale；price_scale 只是 amount 的
        // 计算/展示精度，不参与账户单位换算。
        int quoteRawScale = quoteCurrency == null ? 0 : quoteCurrency.rawScale();
        String pairInvalidReason = validateAgainstPair(
                pair, price, request.quantity(), marketBuyAmount, quoteRawScale);
        if (pairInvalidReason != null) {
            return new Result(requestId, null, symbol, null, pairInvalidReason);
        }

        // 委托号只由币币模块生成；接口不接收客户端 orderId，避免复用同一委托号。
        long orderId = orderIdGenerator.nextId();
        OrderCommand command = new OrderCommand(
                requestId, orderId, request.userId(), symbol,
                request.side(), request.type(), price, request.quantity(),
                pair.priceScale(), pair.quantityScale(), quoteRawScale, marketBuyAmount);
        return new Result(requestId, orderId, symbol, command, null);
    }

    /** 基础参数校验；通过返回 {@code null}。 */
    private static String validate(PlaceOrderRequest request) {
        if (request == null) {
            return "REQUEST_REQUIRED";
        }
        if (request.requestId() != null
                && request.requestId().strip().length()
                > OrderRequestLimits.MAX_REQUEST_ID_LENGTH) {
            return "REQUEST_ID_INVALID";
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

    /** 按交易对配置校验最小下单量、数量步长和最小下单金额；通过返回 {@code null}。 */
    private String validateAgainstPair(TradingPairView pair, long price, long quantity,
                                       long marketBuyAmount, int quoteRawScale) {
        if (pair.minOrderQuantity() > 0 && quantity < pair.minOrderQuantity()) {
            return "BELOW_MIN_ORDER_QUANTITY";
        }
        // 价格精度高于计价币 raw_scale 时，amount 无法换算回计价币原始整数；
        // 该交易对配置本身不可用（创建入口已拦截，这里防御存量/手工写入的配置）。
        if (pair.priceScale() > quoteRawScale) {
            return "PRICE_SCALE_EXCEEDS_RAW_SCALE";
        }
        // 数量必须对齐最小步长，保证与任意 maker 价格成交时价 × 量都能整除成
        // 计价币最小单位；否则撮合引擎会在成交时因金额不可表示而进入 fatal。
        // 精度越界的交易对配置也按该原因拒绝，避免把配置错误变成 500。
        try {
            if (!Amounts.isQuantityAligned(quantity, pair.priceScale(), pair.quantityScale(),
                    quoteRawScale)) {
                return "QUANTITY_STEP_INVALID";
            }
        } catch (IllegalArgumentException e) {
            return "QUANTITY_STEP_INVALID";
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
                        pair.priceScale(), pair.quantityScale(), quoteRawScale);
            } catch (ArithmeticException | IllegalArgumentException e) {
                return "AMOUNT_OVERFLOW";
            }
            if (pair.minOrderAmount() > 0 && amount < pair.minOrderAmount()) {
                return "BELOW_MIN_ORDER_AMOUNT";
            }
            return null;
        }
        // 市价卖单没有价格：用最近成交价估算名义金额；没有参考价时跳过金额校验
        // （该交易对没有成交，订单会按 IOC 撤销，不会产生成交）。
        if (pair.minOrderAmount() > 0) {
            Long referencePrice = latestPriceQuery.latestPrice(pair.symbol());
            if (referencePrice != null && referencePrice > 0) {
                long amount;
                try {
                    amount = Amounts.quoteAmount(referencePrice, quantity,
                            pair.priceScale(), pair.quantityScale(), quoteRawScale);
                } catch (ArithmeticException | IllegalArgumentException e) {
                    return "AMOUNT_OVERFLOW";
                }
                if (amount < pair.minOrderAmount()) {
                    return "BELOW_MIN_ORDER_AMOUNT";
                }
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

    /**
     * 组装结果。
     *
     * @param requestId 归一化后的请求标识（校验失败时也会返回）
     * @param orderId   服务端生成的委托号；校验失败时为 {@code null}
     * @param symbol    归一化后的交易对；基础字段校验失败时可能为 {@code null}
     * @param command   可投递的命令；校验失败时为 {@code null}
     * @param reason    拒绝原因；成功时为 {@code null}
     */
    public record Result(
            String requestId,
            Long orderId,
            String symbol,
            OrderCommand command,
            String reason) {

        public boolean accepted() {
            return command != null;
        }
    }
}
