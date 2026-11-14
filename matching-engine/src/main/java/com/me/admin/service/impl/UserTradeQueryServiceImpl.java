package com.me.admin.service.impl;

import com.me.admin.entity.UserTradeRow;
import com.me.admin.repository.UserTradeRepository;
import com.me.admin.service.UserTradeQueryService;
import com.me.shared.trade.UserTradeQueryPort;
import com.me.shared.trade.UserTradeRecord;
import com.me.shared.trade.UserTradeRole;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * 用户成交记录查询实现：直接委托 QuestDB 访问层。
 *
 * <p>同时实现 shared 的 {@link UserTradeQueryPort}，供币币交易前端查询当前用户的
 * 历史成交；管理端使用带角色筛选的 {@link #list(Long, String, UserTradeRole, int)}。
 */
@Service
public class UserTradeQueryServiceImpl
        implements UserTradeQueryService, UserTradeQueryPort {

    private final UserTradeRepository repository;

    public UserTradeQueryServiceImpl(UserTradeRepository repository) {
        this.repository = repository;
    }

    @Override
    public List<UserTradeRow> list(Long userId, String symbol, UserTradeRole role, int limit) {
        return repository.findTrades(userId, symbol, role, limit);
    }

    @Override
    public List<UserTradeRecord> listByUser(long userId, String symbol, int limit) {
        return repository.findTrades(userId, symbol, null, limit).stream()
                .map(UserTradeQueryServiceImpl::toRecord)
                .toList();
    }

    private static UserTradeRecord toRecord(UserTradeRow row) {
        return new UserTradeRecord(
                row.tradeId(),
                row.symbol(),
                row.userId(),
                row.orderId(),
                row.role(),
                row.counterOrderId(),
                row.side(),
                row.price(),
                row.quantity(),
                row.amount(),
                row.executedAt());
    }
}
