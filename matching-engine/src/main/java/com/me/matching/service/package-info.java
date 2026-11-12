/**
 * 撮合内核业务接口：OrderBook、MatchingEngine。纯 JDK、零 Spring、不直接对接 Kafka。
 *
 * <p>撮合引擎与委托模块逻辑上合并为同一模块；{@code MatchingEngine} 由委托模块在冻结回执流程中
 * 进程内同步调用。只读订单簿 HTTP 接入在 {@code com.me.matching.controller}。
 */
package com.me.matching.service;
