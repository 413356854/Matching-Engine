/**
 * 撮合内核 HTTP 接入层：只读订单簿查询（深度 / BBO），不写订单簿、不访问数据库。
 *
 * <p>撮合内核（entity / service）仍然零 Spring；REST 只读取内核内存快照。
 */
package com.me.matching.controller;
