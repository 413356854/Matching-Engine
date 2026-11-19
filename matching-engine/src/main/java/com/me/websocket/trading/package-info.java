/**
 * WebSocket 交易端点（{@code /ws/trade}）：login、下单、撤单与 ingress ack 路由。
 *
 * <p>消息流：
 * <pre>
 * Browser --WS--> TradeWebSocketHandler --Aeron stream 1001--> 币币模块
 *                                                        |
 * Browser <--WS-- TradeAckPoller <--Aeron stream 1002-- AeronAckPublisher
 * </pre>
 *
 * <p><b>ack 语义</b>：{@code orderAck success=true} 只表示请求通过币币模块同步校验并写入
 * 下单 / 撤单 Disruptor（进入 ingress ring），不代表 Kafka ack、委托受理、冻结或成交。
 * 最终状态通过 {@code GET /api/v1/orders} 或按 requestId 查询接口观察。
 *
 * <p><b>失败 / 丢弃</b>：WS 层非法消息、未登录、限流、Aeron offer 失败立即回 {@code error}；
 * 币币校验失败回 {@code orderAck success=false}；ring 满 / 未启动 / 内部异常时记录 error 并
 * 丢弃订单，best-effort 回失败 ack，不重试。
 *
 * <p><b>无幂等</b>：同一 requestId 重发仍会生成新委托；客户端收到 {@code orderAckTimeout}
 * 或连接断开后应查询委托，不能自动重试。当前单实例、同机 {@code aeron:ipc}；WS 独立部署时
 * 需要共享 media driver 或切 UDP，并考虑每实例独立的 ack stream。
 */
package com.me.websocket.trading;
