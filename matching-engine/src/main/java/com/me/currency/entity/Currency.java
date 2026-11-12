package com.me.currency.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.time.LocalDateTime;

/**
 * 币种表 {@code pair_currency}。
 *
 * <p>币种精度用 {@code scale} 表示小数位数，金额一律是最小单位的整数：
 * 1 BTC = 10<sup>8</sup> sat，{@code scale = 8}。
 */
@TableName("pair_currency")
public class Currency {

    /** 主键，自增。 */
    @TableId(type = IdType.AUTO)
    private Long id;

    /** 币种代码，大写，如 BTC、USDT。 */
    private String code;

    /** 币种名称，如 比特币。 */
    private String name;

    /** 小数位数，最小单位 = 10^-scale。 */
    private Integer scale;

    /** 状态：1=启用，0=停用。 */
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

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
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
