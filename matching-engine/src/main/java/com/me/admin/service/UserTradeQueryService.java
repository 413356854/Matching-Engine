package com.me.admin.service;

import com.me.admin.entity.UserTradeRow;
import com.me.shared.trade.UserTradeRole;
import java.util.List;

/**
 * 管理端用户成交记录查询：按用户 / 交易对 / 角色筛选，按成交时间倒序。
 */
public interface UserTradeQueryService {

    /**
     * @param userId 用户 ID；{@code null} 表示不筛选
     * @param symbol 交易对（大写）；{@code null} 表示不筛选
     * @param role   TAKER / MAKER；{@code null} 表示不筛选
     * @param limit  1..1000，超出范围由实现夹取
     */
    List<UserTradeRow> list(Long userId, String symbol, UserTradeRole role, int limit);
}
