/**
 * 跨模块只读查询契约：用户历史资金流水（admin → 币币页面）。
 *
 * <p>只暴露查询接口与快照记录，不暴露 admin 的 QuestDB 访问层或 {@code funds_flow} 表结构。
 */
package com.me.shared.flow;
