package com.me.currency.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.time.LocalDateTime;

/**
 * 交易对表 {@code pair_symbol}。
 *
 * <p>价格和数量在撮合与账本里一律是最小单位的整数；本表只定义精度、
 * 最小下单量和费率，不改写金额本身。
 *
 * <p>费率单位是基点（bps）：1 bps = 0.01%，10000 bps = 100%。
 * 允许负值表示返佣（rebate）。
 */
@TableName("pair_symbol")
public class TradingPair {

    /** 主键，自增。 */
    @TableId(type = IdType.AUTO)
    private Long id;

    /** 交易对代码，大写，如 BTC-USDT。 */
    private String symbol;

    /** 基础币种代码（被买卖的币），如 BTC。 */
    private String baseCurrency;

    /** 计价币种代码，如 USDT。 */
    private String quoteCurrency;

    /** 价格小数位数。 */
    private Integer priceScale;

    /** 数量小数位数。 */
    private Integer quantityScale;

    /** 最小下单量，基础币最小单位。 */
    private Long minOrderQuantity;

    /** 最小下单金额，计价币最小单位。 */
    private Long minOrderAmount;

    /** maker 手续费率，基点（1 bps = 0.01%）。 */
    private Integer makerFeeBps;

    /** taker 手续费率，基点（1 bps = 0.01%）。 */
    private Integer takerFeeBps;

    /** 状态：1=上线，0=下线。 */
    private Integer status;

    /** 展示排序，升序。 */
    private Integer sortOrder;

    /** 创建时间，由数据库默认值维护。 */
    private LocalDateTime createdAt;

    /** 更新时间，由数据库默认值维护。 */
    private LocalDateTime updatedAt;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getBaseCurrency() {
        return baseCurrency;
    }

    public void setBaseCurrency(String baseCurrency) {
        this.baseCurrency = baseCurrency;
    }

    public String getQuoteCurrency() {
        return quoteCurrency;
    }

    public void setQuoteCurrency(String quoteCurrency) {
        this.quoteCurrency = quoteCurrency;
    }

    public Integer getPriceScale() {
        return priceScale;
    }

    public void setPriceScale(Integer priceScale) {
        this.priceScale = priceScale;
    }

    public Integer getQuantityScale() {
        return quantityScale;
    }

    public void setQuantityScale(Integer quantityScale) {
        this.quantityScale = quantityScale;
    }

    public Long getMinOrderQuantity() {
        return minOrderQuantity;
    }

    public void setMinOrderQuantity(Long minOrderQuantity) {
        this.minOrderQuantity = minOrderQuantity;
    }

    public Long getMinOrderAmount() {
        return minOrderAmount;
    }

    public void setMinOrderAmount(Long minOrderAmount) {
        this.minOrderAmount = minOrderAmount;
    }

    public Integer getMakerFeeBps() {
        return makerFeeBps;
    }

    public void setMakerFeeBps(Integer makerFeeBps) {
        this.makerFeeBps = makerFeeBps;
    }

    public Integer getTakerFeeBps() {
        return takerFeeBps;
    }

    public void setTakerFeeBps(Integer takerFeeBps) {
        this.takerFeeBps = takerFeeBps;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Integer getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(Integer sortOrder) {
        this.sortOrder = sortOrder;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}
