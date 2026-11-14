/**
 * 用户成交记录跨模块契约：币币交易前端通过只读查询端口读取 admin 归档的
 * QuestDB {@code user_trade} 投影，不直接依赖 admin 的访问层。
 */
package com.me.shared.trade;
