package com.me.bot.vo;

/**
 * 机器人当前使用的币安最新价（展示单位字符串，保留币安原始精度）。
 *
 * @param symbol    系统交易对，如 BTC-USDT
 * @param price     币安最新成交价，如 {@code 65052.38000000}
 * @param updatedAt 本地接收时间，ISO-8601 UTC
 */
public record BotQuoteView(String symbol, String price, String updatedAt) {
}
