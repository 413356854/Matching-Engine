package com.me.admin.service;

import com.me.admin.entity.FundsFlowRow;
import com.me.shared.event.FundsLogType;
import java.util.List;

/**
 * 资金流水查询与清理服务：供管理端查询归档流水、一键清空流水。
 */
public interface FundsFlowQueryService {

    /**
     * 查询流水列表，按发生时间倒序。筛选条件均可选。
     *
     * @param userId   用户 ID；{@code null} 表示不筛选
     * @param currency 币种；{@code null} 表示不筛选
     * @param logType  日志类型；{@code null} 表示不筛选
     * @param limit    返回条数上限 1..1000
     */
    List<FundsFlowRow> list(Long userId, String currency, FundsLogType logType, int limit);

    /**
     * 一键删除全部流水，返回删除前的记录数。
     */
    long clearAll();
}
