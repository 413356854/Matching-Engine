package com.me.bot.config;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 机器人模拟模块配置（{@code app.bot.*}）。
 *
 * <p>金额类配置（{@code order-notional}、{@code initial-deposits}）使用<b>展示单位</b>：
 * 100 表示 100 USDT，1 表示 1 BTC；模块按交易对 / 币种精度换算成账本最小单位后下单或充值。
 *
 * @param enabled       是否启用机器人模块；关闭后不充值、也不允许开启自动交易
 * @param userId        机器人使用的默认用户 ID
 * @param symbols       订阅并下单的交易对，默认 BTC-USDT、ETH-USDT
 * @param orderIntervalMs 每笔订单的间隔（毫秒），默认 500；每个交易对每个间隔下一单，买卖交替
 * @param orderNotional 单笔名义金额（计价币展示单位），默认 100；数量 = 名义金额 / 币安最新价
 * @param initialDeposits 启动时补齐到的目标余额（币种 → 展示单位）；已有余额（可用 + 冻结）
 *                        达到目标时不重复充值，因此重启不会无限放大余额
 * @param binance       币安 HTTP / WebSocket 连接配置
 */
@ConfigurationProperties(prefix = "app.bot")
public record BotProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("9001") long userId,
        List<String> symbols,
        @DefaultValue("500") long orderIntervalMs,
        @DefaultValue("100") BigDecimal orderNotional,
        Map<String, BigDecimal> initialDeposits,
        Binance binance) {

    public BotProperties {
        if (userId <= 0) {
            throw new IllegalArgumentException("app.bot.user-id must be > 0: " + userId);
        }
        if (orderIntervalMs < 50) {
            throw new IllegalArgumentException(
                    "app.bot.order-interval-ms must be >= 50: " + orderIntervalMs);
        }
        if (orderNotional == null || orderNotional.signum() <= 0) {
            throw new IllegalArgumentException(
                    "app.bot.order-notional must be > 0: " + orderNotional);
        }

        List<String> normalizedSymbols = symbols == null
                ? List.<String>of()
                : symbols.stream()
                        .filter(symbol -> symbol != null && !symbol.isBlank())
                        .map(symbol -> symbol.strip().toUpperCase(Locale.ROOT))
                        .distinct()
                        .toList();
        symbols = normalizedSymbols.isEmpty()
                ? List.of("BTC-USDT", "ETH-USDT")
                : normalizedSymbols;

        Map<String, BigDecimal> normalizedDeposits = new LinkedHashMap<>();
        if (initialDeposits != null) {
            initialDeposits.forEach((currency, amount) -> {
                if (currency == null || currency.isBlank() || amount == null
                        || amount.signum() <= 0) {
                    return;
                }
                normalizedDeposits.put(
                        currency.strip().toUpperCase(Locale.ROOT), amount);
            });
        }
        initialDeposits = normalizedDeposits.isEmpty()
                ? Map.of(
                        "USDT", new BigDecimal("1000000"),
                        "BTC", new BigDecimal("100"),
                        "ETH", new BigDecimal("1000"))
                : Map.copyOf(normalizedDeposits);

        binance = binance == null
                ? new Binance(null, null, null, 0, 0, 0, 0)
                : binance;
    }

    /**
     * 币安连接配置（{@code app.bot.binance.*}）。
     *
     * @param streamUrl       组合流地址前缀，默认币安 {@code wss} 组合流
     * @param pingUrl         开启自动交易前调用的 REST 测试接口
     * @param proxy           可选 HTTP 代理，例如 {@code http://127.0.0.1:7890}；
     *                        留空时使用 JVM 系统代理（Windows 读取 IE / 系统代理设置）
     * @param connectTimeoutMs HTTP / WebSocket 连接超时
     * @param pingTimeoutMs   测试接口超时
     * @param reconnectDelayMs WebSocket 断开后的重连间隔
     * @param quoteStaleMs    行情超过该时长未更新时停止下单，视为行情中断
     */
    public record Binance(
            String streamUrl,
            String pingUrl,
            String proxy,
            @DefaultValue("8000") int connectTimeoutMs,
            @DefaultValue("5000") int pingTimeoutMs,
            @DefaultValue("3000") long reconnectDelayMs,
            @DefaultValue("60000") long quoteStaleMs) {

        public Binance {
            streamUrl = blankToDefault(
                    streamUrl, "wss://stream.binance.com:9443/stream?streams=");
            pingUrl = blankToDefault(pingUrl, "https://api.binance.com/api/v3/ping");
            proxy = proxy == null ? "" : proxy.strip();
            connectTimeoutMs = connectTimeoutMs > 0 ? connectTimeoutMs : 8000;
            pingTimeoutMs = pingTimeoutMs > 0 ? pingTimeoutMs : 5000;
            reconnectDelayMs = reconnectDelayMs > 0 ? reconnectDelayMs : 3000;
            quoteStaleMs = quoteStaleMs > 0 ? quoteStaleMs : 60000;
        }

        private static String blankToDefault(String value, String fallback) {
            return value == null || value.isBlank() ? fallback : value.strip();
        }
    }
}
