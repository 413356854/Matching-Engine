package com.me.currency.service;

import com.me.currency.vo.CurrencyView;
import java.util.List;

/**
 * 币种查询接口：查询启用中的币种及其精度。
 */
public interface CurrencyService {

    /**
     * 查询全部启用币种，按 {@code sortOrder}、{@code id} 升序。
     */
    List<CurrencyView> listEnabledCurrencies();
}
