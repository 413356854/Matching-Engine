package com.me.bot.service;

import com.me.bot.config.BotProperties;
import com.me.currency.dto.PlaceOrderRequest;
import com.me.currency.mq.OrderCommandPublisher;
import com.me.currency.service.CurrencyConfigCache;
import com.me.currency.service.OrderCommandAssembler;
import com.me.currency.vo.CurrencyView;
import com.me.currency.vo.TradingPairView;
import com.me.shared.order.OrderSide;
import com.me.shared.order.OrderType;
import jakarta.annotation.PreDestroy;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 机器人自动交易：开启 / 关闭状态与按固定间隔下单。
 *
 * <p>开启时先调用币安测试接口；不可达直接拒绝。通过测试后建立 WebSocket 订阅并启动
 * 单线程调度器，每个间隔为每个配置交易对提交一笔限价单：
 * 价格 = 币安最新价（按交易对 {@code priceScale} 换算成最小单位），
 * 数量 = {@code order-notional / 最新价}（按 {@code quantityScale} 向下取整），
 * 买卖方向逐交易对交替。没有足够新的行情时跳过该交易对，不猜测价格。
 *
 * <p>下单复用 {@link OrderCommandAssembler} 的同步校验与委托号生成，
 * 然后交给 {@link OrderCommandPublisher} 进入币币模块的下单 Disruptor，
 * 与 REST 下单走完全相同的接单路径。
 */
@Service
public class BotTradingService {

    private static final Logger log = LoggerFactory.getLogger(BotTradingService.class);

    private static final int STATUS_ONLINE = 1;

    private final BotProperties properties;
    private final BinanceMarketClient marketClient;
    private final CurrencyConfigCache configCache;
    private final OrderCommandAssembler commandAssembler;
    private final OrderCommandPublisher commandPublisher;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "bot-trading");
                thread.setDaemon(true);
                return thread;
            });
    private final AtomicBoolean trading = new AtomicBoolean();
    private final Map<String, AtomicBoolean> buyNext = new ConcurrentHashMap<>();
    private final LongAdder ordersPlaced = new LongAdder();
    private final LongAdder ordersRejected = new LongAdder();

    private volatile ScheduledFuture<?> tickTask;
    private volatile Instant lastOrderAt;
    private volatile String lastError;

    public BotTradingService(BotProperties properties,
                             BinanceMarketClient marketClient,
                             CurrencyConfigCache configCache,
                             OrderCommandAssembler commandAssembler,
                             OrderCommandPublisher commandPublisher) {
        this.properties = properties;
        this.marketClient = marketClient;
        this.configCache = configCache;
        this.commandAssembler = commandAssembler;
        this.commandPublisher = commandPublisher;
    }

    /**
     * 开启自动交易。开启前必须通过币安 REST 测试接口。
     *
     * @return 开启结果；已开启时幂等返回成功
     */
    public synchronized EnableResult enable() {
        if (!properties.enabled()) {
            return new EnableResult(false, "BOT_DISABLED",
                    "机器人模块未启用（app.bot.enabled=false）", 0);
        }
        if (trading.get()) {
            return new EnableResult(true, null, "自动交易已在运行", 0);
        }

        String invalidSymbol = firstUnavailableSymbol();
        if (invalidSymbol != null) {
            return new EnableResult(false, "SYMBOL_NOT_TRADING",
                    "机器人交易对不可用: " + invalidSymbol, 0);
        }

        BinanceMarketClient.TestResult test = marketClient.testConnection();
        if (!test.reachable()) {
            lastError = "币安测试接口不可达: " + test.error();
            log.warn("bot enable rejected: {} (url={}, proxy={})",
                    lastError, properties.binance().pingUrl(),
                    marketClient.proxyDescription());
            return new EnableResult(false, "BINANCE_UNREACHABLE",
                    "币安测试接口不可达，开启自动交易需要配置可访问币安的网络（代理）",
                    test.latencyMs());
        }

        marketClient.connect();
        trading.set(true);
        startTicker();
        lastError = null;
        return new EnableResult(true, null, "自动交易已开启", test.latencyMs());
    }

    /** 关闭自动交易并断开币安行情连接；幂等。 */
    public synchronized void disable() {
        boolean wasTrading = trading.getAndSet(false);
        ScheduledFuture<?> task = tickTask;
        tickTask = null;
        if (task != null) {
            task.cancel(false);
        }
        marketClient.disconnect();
        if (wasTrading) {
        }
    }

    public boolean trading() {
        return trading.get();
    }

    public long ordersPlaced() {
        return ordersPlaced.sum();
    }

    public long ordersRejected() {
        return ordersRejected.sum();
    }

    public Instant lastOrderAt() {
        return lastOrderAt;
    }

    public String lastError() {
        return lastError;
    }

    @PreDestroy
    public void shutdown() {
        disable();
        scheduler.shutdownNow();
    }

    private void startTicker() {
        ScheduledFuture<?> current = tickTask;
        if (current != null && !current.isCancelled()) {
            return;
        }
        tickTask = scheduler.scheduleAtFixedRate(
                this::safeTick, 0, properties.orderIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void safeTick() {
        try {
            tick();
        } catch (RuntimeException e) {
            lastError = "机器人下单异常: " + e;
            log.warn("bot tick failed", e);
        }
    }

    private void tick() {
        if (!trading.get()) {
            return;
        }
        for (String symbol : properties.symbols()) {
            if (!trading.get()) {
                return;
            }
            try {
                placeOne(symbol);
            } catch (RuntimeException e) {
                ordersRejected.increment();
                lastError = symbol + " 下单异常: " + e;
                log.warn("bot order failed: symbol={}", symbol, e);
            }
        }
    }

    private void placeOne(String symbol) {
        BinanceMarketClient.Quote quote = marketClient.quote(symbol);
        if (quote == null) {
            return;
        }
        TradingPairView pair = configCache.pair(symbol);
        if (pair == null || pair.status() != STATUS_ONLINE) {
            lastError = symbol + " 交易对不存在或已下线";
            return;
        }

        long price = toSmallest(quote.price(), pair.priceScale());
        long quantity = quantityFor(pair, quote.price(), pair.priceScale());
        if (price <= 0 || quantity <= 0) {
            lastError = symbol + " 下单价格或数量无效";
            return;
        }

        PlaceOrderRequest request = new PlaceOrderRequest(
                "bot-" + UUID.randomUUID(),
                properties.userId(),
                symbol,
                nextSide(symbol),
                OrderType.LIMIT,
                price,
                quantity,
                null);
        OrderCommandAssembler.Result result = commandAssembler.assemble(request);
        if (!result.accepted()) {
            ordersRejected.increment();
            lastError = symbol + " 下单被拒绝: " + result.reason();
            log.warn("bot order rejected: symbol={}, reason={}", symbol, result.reason());
            return;
        }

        try {
            commandPublisher.publish(result.command());
            ordersPlaced.increment();
            lastOrderAt = Instant.now();
            lastError = null;
        } catch (RuntimeException e) {
            ordersRejected.increment();
            lastError = symbol + " 下单命令投递失败";
            log.warn("bot order publish failed: symbol={}", symbol, e);
        }
    }

    /** 展示价 × 10^scale，四舍五入到最小单位。 */
    private static long toSmallest(BigDecimal displayPrice, int scale) {
        return displayPrice.movePointRight(scale)
                .setScale(0, RoundingMode.HALF_UP)
                .longValueExact();
    }

    /**
     * 数量按「名义金额 / 最新价」计算，并按交易对数量精度向下取整。
     *
     * <p>账本按<b>成交价</b>（maker 挂单价）和成交量换算计价币金额，且要求精确整除。
     * 机器人的订单既可能做 maker 也可能做 taker，成交价是任一笔币安报价换算出的
     * 任意整数价格，因此数量必须满足：对任意价格，{@code priceSmallest × quantitySmallest}
     * 都能被 {@code 10^(priceScale + quantityScale - quoteRawScale)} 整除。
     * 做法是把数量向下对齐到该 10 的幂（而不是某个具体价格的 gcd 步长），
     * 保证全链路成交金额都是整数，避免撮合内核抛 {@code quote amount is not an integer}。
     */
    private long quantityFor(TradingPairView pair, BigDecimal price, int priceScale) {
        BigDecimal quantity = properties.orderNotional()
                .divide(price, pair.quantityScale(), RoundingMode.DOWN);
        long smallest = quantity.movePointRight(pair.quantityScale())
                .setScale(0, RoundingMode.DOWN)
                .longValueExact();
        if (smallest <= 0) {
            return 0;
        }

        CurrencyView quoteCurrency = configCache.currency(pair.quoteCurrency());
        int quoteRawScale = quoteCurrency == null ? 0 : quoteCurrency.rawScale();
        int netScale = priceScale + pair.quantityScale() - quoteRawScale;
        if (netScale <= 0) {
            return smallest;
        }

        long step;
        try {
            step = pow10(netScale);
        } catch (ArithmeticException e) {
            return smallest;
        }
        return (smallest / step) * step;
    }

    private static long pow10(int exponent) {
        long value = 1L;
        for (int i = 0; i < exponent; i++) {
            value = Math.multiplyExact(value, 10L);
        }
        return value;
    }

    private OrderSide nextSide(String symbol) {
        AtomicBoolean nextBuy = buyNext.computeIfAbsent(
                symbol, ignored -> new AtomicBoolean(true));
        if (nextBuy.compareAndSet(true, false)) {
            return OrderSide.BUY;
        }
        nextBuy.set(true);
        return OrderSide.SELL;
    }

    private String firstUnavailableSymbol() {
        List<String> symbols = properties.symbols();
        for (String symbol : symbols) {
            String normalized = symbol.strip().toUpperCase(Locale.ROOT);
            TradingPairView pair = configCache.pair(normalized);
            if (pair == null || pair.status() != STATUS_ONLINE) {
                return normalized;
            }
        }
        return null;
    }

    /**
     * 开启结果。
     *
     * @param success   是否成功
     * @param reason    失败原因码；成功时为 {@code null}
     * @param message   给前端的说明
     * @param binanceLatencyMs 币安测试接口耗时
     */
    public record EnableResult(boolean success, String reason, String message,
                               long binanceLatencyMs) {
    }
}
