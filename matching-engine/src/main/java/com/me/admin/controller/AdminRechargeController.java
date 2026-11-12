package com.me.admin.controller;

import com.me.account.service.RechargeService;
import com.me.admin.dto.AdminRechargeRequest;
import com.me.admin.service.AdminCurrencyCache;
import com.me.admin.vo.AdminRechargeView;
import com.me.currency.vo.CurrencyView;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 管理端充值接口。
 *
 * <p>{@code POST /api/v1/admin/recharges} 接收用户 ID、币种和展示金额（默认 100 万），
 * 按币种精度换算成最小单位后投递 {@code DEPOSIT} 命令；账户模块异步入账，
 * 返回 {@code 202 Accepted} 和本次充值的 {@code requestId}。
 *
 * <p>这是演示用的管理接口，没有鉴权，部署到公网前必须加权限控制。
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminRechargeController {

    private static final Logger log = LoggerFactory.getLogger(AdminRechargeController.class);

    static final long DEFAULT_DISPLAY_AMOUNT = 1_000_000L;
    private static final int MAX_CURRENCY_LENGTH = 16;

    private final RechargeService rechargeService;
    private final AdminCurrencyCache currencyCache;

    public AdminRechargeController(RechargeService rechargeService,
                                   AdminCurrencyCache currencyCache) {
        this.rechargeService = rechargeService;
        this.currencyCache = currencyCache;
    }

    @PostMapping("/recharges")
    public ResponseEntity<AdminRechargeView> recharge(
            @RequestBody(required = false) AdminRechargeRequest request) {
        if (request == null) {
            return badRequest(AdminRechargeView.rejected("REQUEST_REQUIRED"));
        }

        String currency = request.currency() == null
                ? null
                : request.currency().strip().toUpperCase(Locale.ROOT);
        if (request.userId() == null) {
            return badRequest(AdminRechargeView.rejected(0, currency, "USER_ID_REQUIRED"));
        }
        if (request.userId() <= 0) {
            return badRequest(AdminRechargeView.rejected(request.userId(), currency, "USER_ID_INVALID"));
        }
        if (currency == null || currency.isEmpty()) {
            return badRequest(AdminRechargeView.rejected(
                    request.userId(), currency, "CURRENCY_REQUIRED"));
        }
        if (currency.length() > MAX_CURRENCY_LENGTH || !currency.matches("[A-Z0-9]+")) {
            return badRequest(AdminRechargeView.rejected(
                    request.userId(), currency, "CURRENCY_INVALID"));
        }

        CurrencyView currencyView = currencyCache.find(currency);
        if (currencyView == null) {
            return badRequest(AdminRechargeView.rejected(
                    request.userId(), currency, "CURRENCY_NOT_FOUND"));
        }

        long displayAmount = request.amount() == null
                ? DEFAULT_DISPLAY_AMOUNT
                : request.amount();
        if (displayAmount <= 0) {
            return badRequest(AdminRechargeView.rejected(
                    request.userId(), currency, "AMOUNT_INVALID"));
        }

        long ledgerAmount;
        try {
            ledgerAmount = toLedgerAmount(displayAmount, currencyView.scale());
        } catch (ArithmeticException e) {
            return badRequest(AdminRechargeView.rejected(
                    request.userId(), currency, "AMOUNT_OVERFLOW"));
        }

        try {
            String requestId = rechargeService.recharge(
                    request.userId(), currency, ledgerAmount);
            return ResponseEntity.status(HttpStatus.ACCEPTED)
                    .body(AdminRechargeView.success(requestId, request.userId(), currency,
                            ledgerAmount, displayAmount, currencyView.scale()));
        } catch (IllegalStateException e) {
            log.error("admin recharge publish failed: userId={}, currency={}, amount={}",
                    request.userId(), currency, ledgerAmount, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(AdminRechargeView.rejected(
                            request.userId(), currency, "RECHARGE_PUBLISH_FAILED"));
        }
    }

    /** 展示金额 × 10^scale；溢出时抛出 {@link ArithmeticException}。 */
    static long toLedgerAmount(long displayAmount, int scale) {
        long factor = 1L;
        for (int i = 0; i < scale; i++) {
            factor = Math.multiplyExact(factor, 10L);
        }
        return Math.multiplyExact(displayAmount, factor);
    }

    private static ResponseEntity<AdminRechargeView> badRequest(AdminRechargeView view) {
        return ResponseEntity.badRequest().body(view);
    }
}
