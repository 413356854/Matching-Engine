/**
 * 委托模块业务接口；跨模块只调用对方 service。
 *
 * <p>{@code OrderMatchingService} 是委托侧的进程内撮合编排入口：把已支付委托同步送入
 * {@code MatchingEngine}，完成成交清算并返回行情/结算输出。
 */
package com.me.order.service;
