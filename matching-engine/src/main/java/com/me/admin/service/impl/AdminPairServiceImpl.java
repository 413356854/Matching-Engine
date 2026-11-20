package com.me.admin.service.impl;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.me.admin.dto.AdminPairCreateRequest;
import com.me.admin.dto.AdminPairResult;
import com.me.admin.dto.AdminPairUpdateRequest;
import com.me.admin.service.AdminPairService;
import com.me.currency.entity.TradingPair;
import com.me.currency.mapper.TradingPairMapper;
import com.me.currency.service.CurrencyConfigCache;
import com.me.currency.service.PairService;
import com.me.currency.vo.CurrencyView;
import com.me.currency.vo.TradingPairView;
import com.me.matching.service.MatchingEngine;
import com.me.matching.service.OrderBook;
import com.me.matching.vo.DepthSnapshot;
import com.me.order.service.OrderConfigCache;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

/**
 * 管理端交易对读写实现。
 *
 * <p>校验规则与撮合链路的单位约定保持一致：
 * <ul>
 *   <li>价格 / 数量精度 {@code 0..18}（{@code Amounts} 的上限）；</li>
 *   <li>{@code quantityScale} 必须等于基础币种的 {@code rawScale}，否则委托数量
 *       与账户最小单位不一致；</li>
 *   <li>{@code priceScale} 不能超过计价币的 {@code rawScale}：amount 的原始整数
 *       来自计价币 raw_scale，price_scale 只是 amount 的计算/展示精度；</li>
 *   <li>价格 / 数量精度创建后不可修改：订单簿里的价格和数量是最小单位整数，
 *       改精度会让新旧委托按不同单位撮合；需要不同精度时请下线、撤单后重建；</li>
 *   <li>费率单位是 bps，允许负值返佣，范围 {@code -10000..10000}。</li>
 * </ul>
 *
 * <p>有挂单的订单簿不能删除交易对，也不能重建同 symbol 的交易对，避免挂单冻结资金
 * 无法撤销。
 *
 * <p>每次写成功都刷新 {@link CurrencyConfigCache} 和 {@link OrderConfigCache}：
 * 前者负责 HTTP / WS 入口校验，后者是下单命令消费路径的配置来源，两者必须同步更新。
 */
@Service
public class AdminPairServiceImpl implements AdminPairService {

    private static final int STATUS_OFFLINE = 0;
    private static final int STATUS_ONLINE = 1;
    private static final int MAX_SCALE = 18;
    private static final int MAX_FEE_BPS = 10_000;
    private static final int MAX_CURRENCY_LENGTH = 32;
    private static final int MAX_SYMBOL_LENGTH = 32;

    private final TradingPairMapper tradingPairMapper;
    private final PairService pairService;
    private final CurrencyConfigCache configCache;
    private final OrderConfigCache orderConfigCache;
    private final MatchingEngine matchingEngine;

    public AdminPairServiceImpl(TradingPairMapper tradingPairMapper,
                                PairService pairService,
                                CurrencyConfigCache configCache,
                                OrderConfigCache orderConfigCache,
                                MatchingEngine matchingEngine) {
        this.tradingPairMapper = tradingPairMapper;
        this.pairService = pairService;
        this.configCache = configCache;
        this.orderConfigCache = orderConfigCache;
        this.matchingEngine = matchingEngine;
    }

    @Override
    public List<TradingPairView> listPairs() {
        return pairService.listPairs();
    }

    @Override
    public AdminPairResult create(AdminPairCreateRequest request) {
        if (request == null) {
            return AdminPairResult.rejected("REQUEST_REQUIRED");
        }

        String baseCurrency = normalize(request.baseCurrency());
        if (!validCurrencyCode(baseCurrency)) {
            return AdminPairResult.rejected("BASE_CURRENCY_INVALID");
        }
        String quoteCurrency = normalize(request.quoteCurrency());
        if (!validCurrencyCode(quoteCurrency)) {
            return AdminPairResult.rejected("QUOTE_CURRENCY_INVALID");
        }
        if (baseCurrency.equals(quoteCurrency)) {
            return AdminPairResult.rejected("CURRENCY_PAIR_INVALID");
        }

        String symbol = baseCurrency + "-" + quoteCurrency;
        if (symbol.length() > MAX_SYMBOL_LENGTH) {
            return AdminPairResult.rejected("SYMBOL_TOO_LONG");
        }

        CurrencyView base = configCache.currency(baseCurrency);
        if (base == null) {
            return AdminPairResult.rejected("BASE_CURRENCY_NOT_FOUND");
        }
        CurrencyView quote = configCache.currency(quoteCurrency);
        if (quote == null) {
            return AdminPairResult.rejected("QUOTE_CURRENCY_NOT_FOUND");
        }

        int priceScale = request.priceScale() == null ? -1 : request.priceScale();
        int quantityScale = request.quantityScale() == null ? -1 : request.quantityScale();
        int status = request.status() == null ? STATUS_ONLINE : request.status();
        String invalid = validateFields(priceScale, quantityScale,
                request.minOrderQuantity(), request.minOrderAmount(),
                request.makerFeeBps(), request.takerFeeBps(), status);
        if (invalid != null) {
            return AdminPairResult.rejected(invalid);
        }
        if (quantityScale != base.rawScale()) {
            return AdminPairResult.rejected("QUANTITY_SCALE_MISMATCH");
        }
        // amount 的原始精度来自计价币 raw_scale；price_scale 是 amount 的计算/展示
        // 精度，不能超过 raw_scale，否则 amount 无法换算回计价币原始整数。
        if (priceScale > quote.rawScale()) {
            return AdminPairResult.rejected("PRICE_SCALE_EXCEEDS_RAW_SCALE");
        }

        // 交易对代码唯一约束兜底并发创建；先查一次能给普通重复请求更明确的错误。
        Long existing = tradingPairMapper.selectCount(
                Wrappers.<TradingPair>lambdaQuery().eq(TradingPair::getSymbol, symbol));
        if (existing != null && existing > 0) {
            return AdminPairResult.rejected("SYMBOL_ALREADY_EXISTS");
        }
        if (hasRestingOrders(symbol)) {
            // 重建已删除的交易对时，旧订单簿可能还有挂单；用新精度继续撮合会混用单位。
            return AdminPairResult.rejected("SYMBOL_BOOK_NOT_EMPTY");
        }

        TradingPair pair = new TradingPair();
        pair.setSymbol(symbol);
        pair.setBaseCurrency(baseCurrency);
        pair.setQuoteCurrency(quoteCurrency);
        pair.setPriceScale(priceScale);
        pair.setQuantityScale(quantityScale);
        pair.setMinOrderQuantity(Objects.requireNonNullElse(request.minOrderQuantity(), 0L));
        pair.setMinOrderAmount(Objects.requireNonNullElse(request.minOrderAmount(), 0L));
        pair.setMakerFeeBps(Objects.requireNonNullElse(request.makerFeeBps(), 0));
        pair.setTakerFeeBps(Objects.requireNonNullElse(request.takerFeeBps(), 0));
        pair.setStatus(status);
        pair.setSortOrder(Objects.requireNonNullElse(request.sortOrder(), 0));

        try {
            tradingPairMapper.insert(pair);
        } catch (DuplicateKeyException e) {
            return AdminPairResult.rejected("SYMBOL_ALREADY_EXISTS");
        }
        reloadConfigCaches();
        return AdminPairResult.ok(pairService.findBySymbol(symbol));
    }

    @Override
    public AdminPairResult update(long id, AdminPairUpdateRequest request) {
        if (request == null) {
            return AdminPairResult.rejected("REQUEST_REQUIRED");
        }
        TradingPair pair = tradingPairMapper.selectById(id);
        if (pair == null) {
            return AdminPairResult.rejected("PAIR_NOT_FOUND");
        }

        CurrencyView base = configCache.currency(pair.getBaseCurrency());
        if (base == null) {
            // 基础币种被停用时不允许继续启用该交易对；先关闭或修正币种配置。
            return AdminPairResult.rejected("BASE_CURRENCY_NOT_FOUND");
        }

        int priceScale = request.priceScale() == null ? -1 : request.priceScale();
        int quantityScale = request.quantityScale() == null ? -1 : request.quantityScale();
        int status = request.status() == null ? -1 : request.status();
        String invalid = validateFields(priceScale, quantityScale,
                request.minOrderQuantity(), request.minOrderAmount(),
                request.makerFeeBps(), request.takerFeeBps(), status);
        if (invalid != null) {
            return AdminPairResult.rejected(invalid);
        }
        if (quantityScale != base.rawScale()) {
            return AdminPairResult.rejected("QUANTITY_SCALE_MISMATCH");
        }
        if (pair.getPriceScale() == null || pair.getPriceScale() != priceScale
                || pair.getQuantityScale() == null || pair.getQuantityScale() != quantityScale) {
            return AdminPairResult.rejected("SCALE_IMMUTABLE");
        }

        pair.setPriceScale(priceScale);
        pair.setQuantityScale(quantityScale);
        pair.setMinOrderQuantity(Objects.requireNonNullElse(request.minOrderQuantity(), 0L));
        pair.setMinOrderAmount(Objects.requireNonNullElse(request.minOrderAmount(), 0L));
        pair.setMakerFeeBps(Objects.requireNonNullElse(request.makerFeeBps(), 0));
        pair.setTakerFeeBps(Objects.requireNonNullElse(request.takerFeeBps(), 0));
        pair.setStatus(status);
        pair.setSortOrder(Objects.requireNonNullElse(request.sortOrder(), 0));
        tradingPairMapper.updateById(pair);
        reloadConfigCaches();
        return AdminPairResult.ok(pairService.findBySymbol(pair.getSymbol()));
    }

    @Override
    public AdminPairResult delete(long id) {
        TradingPair pair = tradingPairMapper.selectById(id);
        if (pair == null) {
            return AdminPairResult.rejected("PAIR_NOT_FOUND");
        }
        if (Objects.requireNonNullElse(pair.getStatus(), STATUS_OFFLINE) != STATUS_OFFLINE) {
            return AdminPairResult.rejected("PAIR_ONLINE");
        }
        if (hasRestingOrders(pair.getSymbol())) {
            // 交易对下线后撤单接口会拒绝该 symbol，挂单会一直冻结资金；先让用户撤完再删。
            return AdminPairResult.rejected("PAIR_HAS_OPEN_ORDERS");
        }

        tradingPairMapper.deleteById(id);
        reloadConfigCaches();
        return AdminPairResult.ok(null);
    }

    /** 入口校验缓存和下单消费缓存必须同时刷新，避免两边看到不同的交易对状态。 */
    private void reloadConfigCaches() {
        configCache.load();
        orderConfigCache.load();
    }

    /**
     * 订单簿里是否还有未成交的限价挂单；没有订单簿视为无挂单。市价单按 IOC 不挂单。
     *
     * <p>用 {@link OrderBook#depth(int)} 而不是逐方向计数：深度快照与撮合写入共用
     * 订单簿锁，不会读到撮合中途的结构。
     */
    private boolean hasRestingOrders(String symbol) {
        OrderBook book = matchingEngine.book(symbol);
        if (book == null) {
            return false;
        }
        DepthSnapshot depth = book.depth(1);
        return !depth.bids().isEmpty() || !depth.asks().isEmpty();
    }

    /** 校验精度、限额、费率与状态；通过返回 {@code null}。 */
    private static String validateFields(int priceScale, int quantityScale,
                                         Long minOrderQuantity, Long minOrderAmount,
                                         Integer makerFeeBps, Integer takerFeeBps,
                                         int status) {
        if (priceScale < 0 || priceScale > MAX_SCALE) {
            return "PRICE_SCALE_INVALID";
        }
        if (quantityScale < 0 || quantityScale > MAX_SCALE) {
            return "QUANTITY_SCALE_INVALID";
        }
        if (minOrderQuantity != null && minOrderQuantity < 0) {
            return "MIN_ORDER_QUANTITY_INVALID";
        }
        if (minOrderAmount != null && minOrderAmount < 0) {
            return "MIN_ORDER_AMOUNT_INVALID";
        }
        if (makerFeeBps != null
                && (makerFeeBps < -MAX_FEE_BPS || makerFeeBps > MAX_FEE_BPS)) {
            return "MAKER_FEE_INVALID";
        }
        if (takerFeeBps != null
                && (takerFeeBps < -MAX_FEE_BPS || takerFeeBps > MAX_FEE_BPS)) {
            return "TAKER_FEE_INVALID";
        }
        if (status != STATUS_OFFLINE && status != STATUS_ONLINE) {
            return "STATUS_INVALID";
        }
        return null;
    }

    private static boolean validCurrencyCode(String value) {
        return value != null
                && value.length() <= MAX_CURRENCY_LENGTH
                && value.matches("[A-Z0-9]+");
    }

    private static String normalize(String value) {
        return value == null ? null : value.strip().toUpperCase(Locale.ROOT);
    }
}
