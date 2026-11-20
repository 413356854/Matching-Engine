package com.me.currency.service.impl;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.me.currency.entity.Currency;
import com.me.currency.mapper.CurrencyMapper;
import com.me.currency.service.CurrencyService;
import com.me.currency.vo.CurrencyView;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.springframework.stereotype.Service;

/**
 * 币种查询实现：直接读 {@code pair_currency} 表，只返回启用中的币种。
 */
@Service
public class CurrencyServiceImpl implements CurrencyService {

    private static final int STATUS_ENABLED = 1;

    private final CurrencyMapper currencyMapper;

    public CurrencyServiceImpl(CurrencyMapper currencyMapper) {
        this.currencyMapper = currencyMapper;
    }

    @Override
    public List<CurrencyView> listEnabledCurrencies() {
        List<Currency> currencies = currencyMapper.selectList(
                Wrappers.<Currency>lambdaQuery()
                        .eq(Currency::getStatus, STATUS_ENABLED)
                        .orderByAsc(Currency::getSortOrder)
                        .orderByAsc(Currency::getId));
        return currencies.stream().map(CurrencyServiceImpl::toView).toList();
    }

    private static CurrencyView toView(Currency currency) {
        // 币种代码在进入缓存前归一化；下游直接使用 view 里的 code。
        int scale = Objects.requireNonNullElse(currency.getScale(), 0);
        // 旧库没有 raw_scale 时退回 scale，保持原有单位；
        // 新库应显式配置 raw_scale >= scale。
        int rawScale = Objects.requireNonNullElse(currency.getRawScale(), scale);
        return new CurrencyView(
                Objects.requireNonNullElse(currency.getId(), 0L),
                normalize(currency.getCode()),
                currency.getName(),
                scale,
                rawScale,
                Objects.requireNonNullElse(currency.getStatus(), 0),
                Objects.requireNonNullElse(currency.getSortOrder(), 0));
    }

    private static String normalize(String value) {
        return value == null ? null : value.strip().toUpperCase(Locale.ROOT);
    }
}
