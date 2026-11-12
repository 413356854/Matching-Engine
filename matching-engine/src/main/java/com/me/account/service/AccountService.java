package com.me.account.service;

import com.me.account.dto.AccountApplyResult;
import com.me.account.dto.FundsCommand;
import com.me.account.dto.TradeSettleResult;
import com.me.account.dto.TradeSettlementCommand;
import com.me.account.vo.AccountView;
import java.util.List;

/**
 * 账户业务接口：充值、扣账、成交双边结算、余额查询。跨模块只调用该接口。
 */
public interface AccountService {

    /**
     * 执行充值或扣账命令。扣账余额不足时返回拒绝结果，余额不变。
     */
    AccountApplyResult apply(FundsCommand command);

    /**
     * 执行一条成交结算消息里的全部资金腿（买方 / 卖方扣账与到账）。
     * 所有腿要么全部生效，要么全部不生效。演示版不做 requestId 防重；Kafka 重投会再次执行。
     */
    TradeSettleResult settle(TradeSettlementCommand command);

    /**
     * 查询账户余额；账户不存在时返回余额 0、seq 0。
     */
    AccountView view(long userId, String currency);

    /**
     * 查询用户在全部币种下的余额，按币种升序；没有账户时返回空列表。
     */
    List<AccountView> listBalances(long userId);
}
