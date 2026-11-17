package com.me.bot.vo;

import java.util.List;

/**
 * 机器人模块状态。
 *
 * @param success          请求是否成功（GET 状态恒为 true，开启失败时为 false）
 * @param reason           失败原因码，如 {@code BINANCE_UNREACHABLE}
 * @param message          给前端的说明
 * @param enabled          模块是否启用（{@code app.bot.enabled}）
 * @param trading          自动交易是否正在运行
 * @param userId           机器人用户 ID
 * @param symbols          机器人订阅 / 下单的交易对
 * @param orderIntervalMs  下单间隔（毫秒）
 * @param binanceConnected 币安 WebSocket 是否已连接
 * @param proxy            当前使用的网络方式（手动代理 / 系统代理）
 * @param quotes           最新的币安行情快照
 * @param ordersPlaced     已提交订单数（本地累计）
 * @param ordersRejected   同步拒绝数（价格 / 数量 / 交易对校验等）
 * @param lastOrderAt      最近一次成功提交订单的时间，ISO-8601 UTC
 * @param lastError        最近一次下单错误
 * @param binanceError     最近一次连接 / 行情错误
 */
public record BotStatusView(
        boolean success,
        String reason,
        String message,
        boolean enabled,
        boolean trading,
        long userId,
        List<String> symbols,
        long orderIntervalMs,
        boolean binanceConnected,
        String proxy,
        List<BotQuoteView> quotes,
        long ordersPlaced,
        long ordersRejected,
        String lastOrderAt,
        String lastError,
        String binanceError) {
}
