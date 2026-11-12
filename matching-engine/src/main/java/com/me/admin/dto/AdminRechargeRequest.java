package com.me.admin.dto;

/**
 * 管理员充值请求。
 *
 * <p>{@code amount} 是<b>展示金额</b>（按币种单位计的整数，如 1,000,000 USDT），
 * 不是最小单位；服务端按 {@code pair_currency.scale} 换算成账本最小单位后入账。
 * 缺省为 1,000,000（100 万）。
 */
public record AdminRechargeRequest(Long userId, String currency, Long amount) {
}
