package com.me.bot.service;

import com.me.account.service.AccountService;
import com.me.account.service.RechargeService;
import com.me.account.vo.AccountView;
import com.me.bot.config.BotProperties;
import com.me.currency.service.CurrencyConfigCache;
import com.me.currency.vo.CurrencyView;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 启动时为机器人默认用户补齐演示资金。
 *
 * <p>{@code app.bot.initial-deposits} 配置的是<b>目标余额</b>（展示单位）。启动时读取
 * 账户模块内存余额，按「目标 -（可用 + 冻结）」的差额投递 {@code DEPOSIT} 命令；
 * 已达到目标则不重复充值，因此重启不会让余额无限增长。充值仍然走账户模块的 Kafka
 * 命令链路，与后台模拟登录充值完全一致。
 */
@Component
public class BotAccountBootstrap {

    private static final Logger log = LoggerFactory.getLogger(BotAccountBootstrap.class);

    private final BotProperties properties;
    private final AccountService accountService;
    private final RechargeService rechargeService;
    private final CurrencyConfigCache configCache;

    public BotAccountBootstrap(BotProperties properties,
                               AccountService accountService,
                               RechargeService rechargeService,
                               CurrencyConfigCache configCache) {
        this.properties = properties;
        this.accountService = accountService;
        this.rechargeService = rechargeService;
        this.configCache = configCache;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void bootstrap() {
        if (!properties.enabled()) {
            return;
        }

        long userId = properties.userId();
        Map<String, Long> balances = currentBalances(userId);
        properties.initialDeposits().forEach((currencyCode, displayAmount) -> {
            CurrencyView currency = configCache.currency(currencyCode);
            if (currency == null) {
                log.warn("bot initial deposit skipped: currency not found, currency={}",
                        currencyCode);
                return;
            }
            long target;
            try {
                target = toLedgerAmount(displayAmount, currency.rawScale());
            } catch (ArithmeticException e) {
                log.warn("bot initial deposit skipped: amount overflow, currency={}, amount={}",
                        currencyCode, displayAmount);
                return;
            }
            long held = balances.getOrDefault(currency.code(), 0L);
            if (held >= target) {
                return;
            }
            long delta = target - held;
            try {
                String requestId = rechargeService.recharge(userId, currency.code(), delta);
            } catch (RuntimeException e) {
                log.warn("bot initial deposit failed: userId={}, currency={}, amount={}",
                        userId, currency.code(), delta, e);
            }
        });
    }

    private Map<String, Long> currentBalances(long userId) {
        Map<String, Long> balances = new HashMap<>();
        for (AccountView view : accountService.listBalances(userId)) {
            balances.merge(view.currency(), view.available() + view.frozen(), Long::sum);
        }
        return balances;
    }

    /** 展示金额 × 10^rawScale；溢出时抛出 {@link ArithmeticException}。 */
    private static long toLedgerAmount(BigDecimal displayAmount, int scale) {
        return displayAmount.movePointRight(scale)
                .setScale(0, RoundingMode.DOWN)
                .longValueExact();
    }
}
