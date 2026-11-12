package com.me.currency.service.impl;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.me.currency.entity.Currency;
import com.me.currency.mapper.CurrencyMapper;
import com.me.currency.service.CurrencyService;
import com.me.currency.vo.CurrencyView;
import java.util.List;
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
        return new CurrencyView(
                Objects.requireNonNullElse(currency.getId(), 0L),
                currency.getCode(),
                currency.getName(),
                Objects.requireNonNullElse(currency.getScale(), 0),
                Objects.requireNonNullElse(currency.getStatus(), 0),
                Objects.requireNonNullElse(currency.getSortOrder(), 0));
    }
}
