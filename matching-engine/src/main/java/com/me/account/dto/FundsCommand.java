package com.me.account.dto;

import com.me.shared.event.FundsLogType;
import java.util.Locale;

/**
 * 资金操作命令，来自 Kafka topic {@code me.account.order-freeze-commands-batch}
 * （ORDER_FREEZE，批量消息的元素）或 {@code me.account.commands}（充值 / 扣账等）。
 *
 * <p>命令携带的是<b>日志类型</b>（如 {@code DEPOSIT} / {@code FREEZE}），
 * 账户服务通过 {@code logType.op()} 取出原子操作类型后执行，不按具体日志类型分支。
 * {@code amount} 为币种最小单位的整数；{@code currency} 统一转为大写。
 *
 * <p>{@code refId} 是业务关联单号（如下单冻结时的 {@code orderId}），没有关联业务时传 0；
 * 账户模块会把它原样放进冻结回执，供上游把结果关联回自己的业务单据。
 * 使用包装类型是为了兼容旧命令 JSON（没有该字段）：Jackson 3 默认拒绝把缺失字段映射成
 * 基本类型，缺失的 {@code refId} 会反序列化为 {@code null}，再由构造器归一成 0。
 *
 * <p>{@code symbol} 只在冻结命令上使用：冻结 topic 按 userId slot 分区后，账户模块
 * 仍需要按 symbol 汇总冻结回执，保证撮合阶段按 symbol 保序。充值等命令为 {@code null}。
 */
public record FundsCommand(
        String requestId,
        long userId,
        String currency,
        FundsLogType logType,
        long amount,
        Long refId,
        String symbol) {

    public FundsCommand {
        if (currency != null) {
            currency = currency.strip().toUpperCase(Locale.ROOT);
        }
        if (refId == null) {
            refId = 0L;
        }
        if (symbol != null) {
            symbol = symbol.strip().toUpperCase(Locale.ROOT);
        }
    }

    /** 兼容充值等不携带 symbol 的资金命令。 */
    public FundsCommand(String requestId, long userId, String currency, FundsLogType logType,
                        long amount, Long refId) {
        this(requestId, userId, currency, logType, amount, refId, null);
    }
}
