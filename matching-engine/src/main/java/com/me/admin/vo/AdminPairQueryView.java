package com.me.admin.vo;

import com.me.currency.vo.TradingPairView;
import java.util.List;

/**
 * 管理端交易对列表结果。
 *
 * <p>返回全部交易对（含下线状态），按 {@code sortOrder}、{@code id} 升序；
 * 失败时 {@code reason} 是错误码，{@code pairs} 为空列表。
 */
public record AdminPairQueryView(
        boolean success,
        String reason,
        List<TradingPairView> pairs) {

    public AdminPairQueryView {
        pairs = pairs == null ? List.of() : List.copyOf(pairs);
    }

    public static AdminPairQueryView success(List<TradingPairView> pairs) {
        return new AdminPairQueryView(true, null, pairs);
    }

    public static AdminPairQueryView rejected(String reason) {
        return new AdminPairQueryView(false, reason, List.of());
    }
}
