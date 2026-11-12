package com.me.shared.flow;

import java.util.List;

/**
 * 用户历史资金流水只读查询端口。
 *
 * <p>接口放在 shared 中：币币模块只依赖这份跨模块契约，不直接依赖 admin 的 QuestDB
 * 访问层，也不共享 {@code funds_flow} 表；实现由 admin 模块提供。
 */
public interface FundsFlowHistoryQueryPort {

    /**
     * 查询指定用户的历史资金流水，按发生时间倒序（最新记录在前）。
     *
     * @param userId   用户 ID，必须为正数
     * @param currency 可选币种（大写）；{@code null} 表示全部币种
     * @param limit    返回条数上限 1..1000，超出范围由实现夹取
     */
    List<FundsFlowRecord> list(long userId, String currency, int limit);
}
