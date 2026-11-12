package com.me.account.controller;

import com.me.account.service.AccountService;
import com.me.account.vo.AccountBalancesView;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 账户查询接口。
 *
 * <p>{@code GET /api/v1/accounts/{userId}/balances} 按用户 ID 返回全部币种余额：
 * 每个币种包含可用余额、冻结余额和账户变更序号，按币种升序；没有账户时返回空列表。
 */
@RestController
@RequestMapping("/api/v1/accounts")
public class AccountController {

    private final AccountService accountService;

    public AccountController(AccountService accountService) {
        this.accountService = accountService;
    }

    @GetMapping("/{userId}/balances")
    public ResponseEntity<AccountBalancesView> balances(@PathVariable long userId) {
        if (userId <= 0) {
            return ResponseEntity.badRequest()
                    .body(AccountBalancesView.rejected(userId, "USER_ID_INVALID"));
        }
        return ResponseEntity.ok(
                AccountBalancesView.success(userId, accountService.listBalances(userId)));
    }
}
