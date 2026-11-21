package com.me.shared.order;

/**
 * 委托 / 撤单请求的公共长度限制。
 *
 * <p>REST、WebSocket / Aeron 入口与委托模块消费端的校验共用同一上限，避免
 * “入口接受、查询或消费端拒绝”的口径不一致。
 */
public final class OrderRequestLimits {

    /** {@code requestId} 的最大字符数（strip 后）。 */
    public static final int MAX_REQUEST_ID_LENGTH = 128;

    private OrderRequestLimits() {
    }
}
