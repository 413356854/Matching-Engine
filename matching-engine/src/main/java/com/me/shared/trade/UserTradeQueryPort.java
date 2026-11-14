package com.me.shared.trade;

import java.util.List;

/**
 * 用户成交记录只读查询端口。
 *
 * <p>接口放在 shared 中：币币模块只依赖这份跨模块契约，不直接依赖 admin 的 QuestDB
 * 访问层，也不共享 {@code user_trade} 表；实现由 admin 模块提供。
 */
public interface UserTradeQueryPort {

    /**
     * 查询指定用户的成交记录，按成交时间倒序（最新记录在前）。
     *
     * @param userId 用户 ID，必须为正数
     * @param symbol 可选交易对（大写）；{@code null} 表示全部交易对
     * @param limit  返回条数上限 1..1000，超出范围由实现夹取
     */
    List<UserTradeRecord> listByUser(long userId, String symbol, int limit);
}
