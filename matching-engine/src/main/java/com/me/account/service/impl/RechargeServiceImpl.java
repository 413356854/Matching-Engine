package com.me.account.service.impl;

import com.me.account.dto.FundsCommand;
import com.me.account.mq.AccountCommandPublisher;
import com.me.account.service.RechargeService;
import com.me.shared.event.FundsLogType;
import java.util.Locale;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * 充值入口实现：生成幂等键并投递 {@code DEPOSIT} 命令，余额由账户监听器异步入账。
 */
@Service
public class RechargeServiceImpl implements RechargeService {

    private final AccountCommandPublisher commandPublisher;

    public RechargeServiceImpl(AccountCommandPublisher commandPublisher) {
        this.commandPublisher = commandPublisher;
    }

    @Override
    public String recharge(long userId, String currency, long amount) {
        String requestId = "admin-deposit-" + UUID.randomUUID();
        FundsCommand command = new FundsCommand(
                requestId,
                userId,
                currency.strip().toUpperCase(Locale.ROOT),
                FundsLogType.DEPOSIT,
                amount,
                0L);
        commandPublisher.publishAndWait(command);
        return requestId;
    }
}
