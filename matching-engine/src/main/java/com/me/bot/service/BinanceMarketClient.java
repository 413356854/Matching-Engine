package com.me.bot.service;

import com.me.bot.config.BotProperties;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * 币安现货行情客户端：REST 连通性测试 + WebSocket {@code miniTicker} 最新价订阅。
 *
 * <p>使用 JDK 自带的 {@link HttpClient}（不引入第三方 WebSocket 依赖）。HTTP 测试接口与
 * WebSocket 共用同一个 {@link ProxySelector}：配置了 {@code app.bot.binance.proxy} 时走
 * 指定 HTTP 代理；留空时使用 JVM 系统代理（Windows 下即系统 / IE 代理）。
 *
 * <p>组合流地址由 {@code stream-url} 前缀 + 每个交易对的
 * {@code btcusdt@miniTicker} 组成。收到消息后只把最新价写入内存，不做落库；
 * 断线后按 {@code reconnect-delay-ms} 自动重连，直到调用 {@link #disconnect()}。
 */
@Component
public class BinanceMarketClient {

    private static final Logger log = LoggerFactory.getLogger(BinanceMarketClient.class);

    private static final JsonMapper MAPPER = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private static final String STREAM_SUFFIX = "@miniTicker";

    private final BotProperties.Binance config;
    private final HttpClient httpClient;
    private final ProxySelector proxySelector;
    private final String proxyDescription;
    private final String streamUrl;
    private final Map<String, String> streamToSymbol;
    private final String symbolsText;

    private final Map<String, Quote> quotes = new ConcurrentHashMap<>();
    private final ScheduledExecutorService reconnectScheduler =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "binance-reconnect");
                thread.setDaemon(true);
                return thread;
            });

    private final AtomicBoolean desiredConnected = new AtomicBoolean();
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean();
    private volatile WebSocket webSocket;
    private volatile boolean connected;
    private volatile String lastError;

    public BinanceMarketClient(BotProperties properties) {
        this.config = properties.binance();
        ProxyChoice proxy = resolveProxy(this.config);
        this.proxySelector = proxy.selector();
        this.proxyDescription = proxy.description();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(this.config.connectTimeoutMs()))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .proxy(proxySelector)
                .build();

        Map<String, String> mapping = new LinkedHashMap<>();
        for (String symbol : properties.symbols()) {
            mapping.put(streamName(symbol), symbol);
        }
        this.streamToSymbol = Map.copyOf(mapping);
        this.symbolsText = mapping.keySet().stream().collect(Collectors.joining("/"));
        this.streamUrl = this.config.streamUrl() + symbolsText;
    }

    /**
     * 调用币安 REST 测试接口，验证当前网络 / 代理是否可以访问币安。
     *
     * @return 测试结果；失败时 {@link TestResult#error()} 给出异常摘要
     */
    public TestResult testConnection() {
        long startedNanos = System.nanoTime();
        HttpRequest request = HttpRequest.newBuilder(URI.create(config.pingUrl()))
                .timeout(Duration.ofMillis(config.pingTimeoutMs()))
                .header("Accept", "application/json")
                .header("User-Agent", "matching-engine-bot/1.0")
                .GET()
                .build();
        try {
            HttpResponse<Void> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            long latencyMs = elapsedMillis(startedNanos);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return new TestResult(true, latencyMs, null);
            }
            return new TestResult(false, latencyMs, "HTTP " + response.statusCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new TestResult(false, elapsedMillis(startedNanos), "请求被中断");
        } catch (IOException | RuntimeException e) {
            return new TestResult(false, elapsedMillis(startedNanos), describe(e));
        }
    }

    /** 开始（或恢复）WebSocket 订阅；幂等。 */
    public synchronized void connect() {
        desiredConnected.set(true);
        if (webSocket != null && !webSocket.isOutputClosed()) {
            return;
        }
        openWebSocket();
    }

    /** 停止订阅并关闭连接；幂等。 */
    public synchronized void disconnect() {
        desiredConnected.set(false);
        connected = false;
        WebSocket current = webSocket;
        webSocket = null;
        if (current != null && !current.isOutputClosed()) {
            try {
                current.sendClose(WebSocket.NORMAL_CLOSURE, "bot disabled");
            } catch (RuntimeException e) {
                current.abort();
            }
        }
    }

    /** 最新价；行情超过 {@code quote-stale-ms} 未更新时返回 {@code null}。 */
    public Quote quote(String symbol) {
        Quote quote = quotes.get(symbol);
        if (quote == null) {
            return null;
        }
        long ageMs = Duration.between(quote.receivedAt(), Instant.now()).toMillis();
        return ageMs > config.quoteStaleMs() ? null : quote;
    }

    /** 当前全部最新价（可能包含略微过期的报价，仅供状态展示）。 */
    public Map<String, Quote> quotes() {
        return Map.copyOf(quotes);
    }

    public boolean connected() {
        return connected;
    }

    public String lastError() {
        return lastError;
    }

    public String proxyDescription() {
        return proxyDescription;
    }

    public String streamUrl() {
        return streamUrl;
    }

    @PreDestroy
    public void shutdown() {
        disconnect();
        reconnectScheduler.shutdownNow();
    }

    private synchronized void openWebSocket() {
        if (!desiredConnected.get()) {
            return;
        }
        try {
            WebSocket socket = httpClient.newWebSocketBuilder()
                    .connectTimeout(Duration.ofMillis(config.connectTimeoutMs()))
                    .buildAsync(URI.create(streamUrl), new MiniTickerListener())
                    .get(config.connectTimeoutMs() + 2000L, TimeUnit.MILLISECONDS);
            webSocket = socket;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            onConnectFailure("连接被中断");
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            Throwable cause = e instanceof ExecutionException && e.getCause() != null
                    ? e.getCause()
                    : e;
            onConnectFailure(describe(cause));
        }
    }

    private void onConnectFailure(String reason) {
        connected = false;
        lastError = "币安 WebSocket 连接失败: " + reason;
        log.warn("{}, {}ms 后重试", lastError, config.reconnectDelayMs());
        scheduleReconnect();
    }

    private void scheduleReconnect() {
        if (!desiredConnected.get() || !reconnectScheduled.compareAndSet(false, true)) {
            return;
        }
        reconnectScheduler.schedule(() -> {
            reconnectScheduled.set(false);
            if (!desiredConnected.get()) {
                return;
            }
            synchronized (BinanceMarketClient.this) {
                if (desiredConnected.get() && (webSocket == null || webSocket.isOutputClosed())) {
                    openWebSocket();
                }
            }
        }, config.reconnectDelayMs(), TimeUnit.MILLISECONDS);
    }

    private void onMessage(String text) {
        CombinedStreamMessage message;
        try {
            message = MAPPER.readValue(text, CombinedStreamMessage.class);
        } catch (RuntimeException e) {
            log.debug("binance message ignored: {}", e.toString());
            return;
        }
        if (message == null || message.data() == null) {
            return;
        }
        MiniTicker ticker = message.data();
        String symbol = streamToSymbol.get(message.stream());
        if (symbol == null && ticker.s() != null) {
            symbol = resolveByExchangeSymbol(ticker.s());
        }
        if (symbol == null || ticker.c() == null || ticker.c().isBlank()) {
            return;
        }
        BigDecimal price;
        try {
            price = new BigDecimal(ticker.c());
        } catch (NumberFormatException e) {
            return;
        }
        if (price.signum() <= 0) {
            return;
        }
        Instant exchangeTime = ticker.E() > 0
                ? Instant.ofEpochMilli(ticker.E())
                : Instant.now();
        quotes.put(symbol, new Quote(symbol, price, exchangeTime, Instant.now()));
        lastError = null;
    }

    private String resolveByExchangeSymbol(String exchangeSymbol) {
        String normalized = exchangeSymbol.strip().toUpperCase(Locale.ROOT);
        for (String configured : streamToSymbol.values()) {
            if (configured.replace("-", "").equals(normalized)) {
                return configured;
            }
        }
        return null;
    }

    private void onSocketTerminated(WebSocket source, String reason) {
        if (source != webSocket) {
            return;
        }
        connected = false;
        webSocket = null;
        if (desiredConnected.get()) {
            lastError = reason;
            log.warn("{}, {}ms 后重连", reason, config.reconnectDelayMs());
            scheduleReconnect();
        } else {
        }
    }

    private static String streamName(String symbol) {
        return symbol.replace("-", "").toLowerCase(Locale.ROOT) + STREAM_SUFFIX;
    }

    private static long elapsedMillis(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }

    private static String describe(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        String type = root.getClass().getSimpleName();
        return message == null || message.isBlank() ? type : type + ": " + message;
    }

    private static ProxyChoice resolveProxy(BotProperties.Binance config) {
        // 显式配置优先；未配置时沿用常见的 HTTPS_PROXY 环境变量。
        String raw = firstNonBlank(
                config.proxy(),
                System.getenv("HTTPS_PROXY"),
                System.getenv("https_proxy"),
                System.getenv("HTTP_PROXY"),
                System.getenv("http_proxy"));
        if (raw != null) {
            URI uri = URI.create(raw.contains("://") ? raw : "http://" + raw);
            String host = uri.getHost();
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException(
                        "app.bot.binance.proxy is invalid: " + raw);
            }
            int port = uri.getPort() > 0 ? uri.getPort() : 80;
            return new ProxyChoice(
                    ProxySelector.of(InetSocketAddress.createUnresolved(host, port)),
                    "手动代理 " + host + ":" + port);
        }
        // 未配置代理：使用 JVM 系统代理（启动类静态块已打开 useSystemProxies，
        // Windows 下读取系统 / IE 代理设置；没有系统代理时按 DIRECT 直连）。
        ProxySelector selector = ProxySelector.getDefault();
        return new ProxyChoice(selector, describeSystemProxy(selector, config.pingUrl()));
    }

    private static String describeSystemProxy(ProxySelector selector, String pingUrl) {
        if (selector == null) {
            return "系统代理（直连）";
        }
        try {
            for (java.net.Proxy proxy : selector.select(URI.create(pingUrl))) {
                if (proxy.type() != java.net.Proxy.Type.DIRECT
                        && proxy.address() instanceof InetSocketAddress address) {
                    return "系统代理 " + address.getHostString() + ":" + address.getPort();
                }
            }
        } catch (RuntimeException e) {
            return "系统代理";
        }
        return "系统代理（直连）";
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.strip();
            }
        }
        return null;
    }

    private record ProxyChoice(ProxySelector selector, String description) {
    }

    private record CombinedStreamMessage(String stream, MiniTicker data) {
    }

    private record MiniTicker(String s, String c, long E) {
    }

    /**
     * 一条最新价快照。
     *
     * @param symbol       系统交易对，如 BTC-USDT
     * @param price        币安最新成交价（计价币 / 基础币）
     * @param exchangeTime 币安事件时间
     * @param receivedAt   本地接收时间，用于判断行情是否过期
     */
    public record Quote(String symbol, BigDecimal price, Instant exchangeTime, Instant receivedAt) {
    }

    /**
     * 币安测试接口结果。
     *
     * @param reachable 是否可达
     * @param latencyMs 耗时（毫秒）
     * @param error     失败摘要；成功时为 {@code null}
     */
    public record TestResult(boolean reachable, long latencyMs, String error) {
    }

    private final class MiniTickerListener implements WebSocket.Listener {

        private final StringBuilder textBuffer = new StringBuilder(512);

        @Override
        public void onOpen(WebSocket socket) {
            connected = true;
            lastError = null;
            webSocket = socket;
            socket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket socket, CharSequence data, boolean last) {
            textBuffer.append(data);
            if (last) {
                String text = textBuffer.toString();
                textBuffer.setLength(0);
                onMessage(text);
            }
            socket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket socket, int statusCode, String reason) {
            onSocketTerminated(socket, "币安 WebSocket 已关闭: " + statusCode
                    + (reason == null || reason.isBlank() ? "" : " " + reason));
            return null;
        }

        @Override
        public void onError(WebSocket socket, Throwable error) {
            connected = false;
            onSocketTerminated(socket, "币安 WebSocket 异常: " + describe(error));
        }
    }
}
