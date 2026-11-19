package com.me.currency.service;

import com.me.shared.order.OrderHistoryQueryPort;
import com.me.shared.order.OrderHistoryRecord;
import org.springframework.stereotype.Service;

/**
 * 币币模块的用户历史委托查询入口。
 *
 * <p>按 {@code userId + requestId} 查询已进入终态并归档到 QuestDB {@code ord_history}
 * 的委托。数据访问由 admin 模块实现 {@link OrderHistoryQueryPort}，本服务只做参数归一化
 * 和跨模块边界适配，不直接依赖 QuestDB 访问层。
 *
 * <p>当前仍在内存中的委托由委托模块的 {@code OrderService.findByRequestId} 查询；
 * 调用方应先查当前委托，再回退到本服务。
 */
@Service
public class OrderHistoryLookupService {

    private final OrderHistoryQueryPort historyQueryPort;

    public OrderHistoryLookupService(OrderHistoryQueryPort historyQueryPort) {
        this.historyQueryPort = historyQueryPort;
    }

    /**
     * 按 {@code userId + requestId} 查询一条历史委托。
     *
     * @param userId    用户 ID，必须为正数
     * @param requestId 请求标识，不能为空
     * @return 历史委托；不存在或参数非法时返回 {@code null}
     */
    public OrderHistoryRecord findByRequestId(long userId, String requestId) {
        if (userId <= 0 || requestId == null || requestId.isBlank()) {
            return null;
        }
        return historyQueryPort.findByRequestId(userId, requestId.strip());
    }
}
