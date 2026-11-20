package com.me.currency.service;

import com.me.currency.dto.CancelOrderRequest;
import com.me.currency.vo.TradingPairView;
import com.me.shared.order.OrderCancelCommand;
import java.util.Locale;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * 撤单请求的共享组装器：HTTP 撤单接口与 Aeron 消费者共用。
 *
 * <p>只做同步校验：基础字段 → 交易对存在且上线 → 生成 {@link OrderCancelCommand}。
 * 不查委托、不修改状态、不解冻资金；真正的撤单由委托模块消费
 * {@code me.order.cancels-batch} 后异步完成。
 *
 * <p>{@code requestId} 缺省时由服务端生成 UUID；{@code userId} 必填且必须大于 0，表示
 * 发起撤单的用户。委托模块只在命令的 {@code userId} 与委托归属一致时才撤销，否则丢弃命令。
 * HTTP 路径由前端在 body 中传入当前用户；WebSocket / Aeron 路径由连接登录态注入。
 * 接入登录后，userId 应改为从认证主体（token）获取，不再信任客户端 body。
 */
@Component
public class OrderCancelAssembler {

    private static final int STATUS_ONLINE = 1;

    private final CurrencyConfigCache configCache;

    public OrderCancelAssembler(CurrencyConfigCache configCache) {
        this.configCache = configCache;
    }

    /**
     * 校验并组装一条撤单命令。
     *
     * @return {@code accepted()=false} 时 {@code reason()} 是拒绝原因，{@code command()} 为空
     */
    public Result assemble(CancelOrderRequest request) {
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

        long orderId = request.orderId();
        long userId = request.userId();
        OrderCancelCommand command = new OrderCancelCommand(requestId, orderId, userId, symbol);
        return new Result(requestId, orderId, symbol, command, null);
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
        if (request.userId() == null) {
            return "USER_ID_REQUIRED";
        }
        if (request.userId() <= 0) {
            return "USER_ID_INVALID";
        }
        return null;
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
     * @param orderId   待撤销的委托号；校验失败时为 {@code null}
     * @param symbol    归一化后的交易对；基础字段校验失败时可能为 {@code null}
     * @param command   可投递的撤单命令；校验失败时为 {@code null}
     * @param reason    拒绝原因；成功时为 {@code null}
     */
    public record Result(
            String requestId,
            Long orderId,
            String symbol,
            OrderCancelCommand command,
            String reason) {

        public boolean accepted() {
            return command != null;
        }
    }
}
