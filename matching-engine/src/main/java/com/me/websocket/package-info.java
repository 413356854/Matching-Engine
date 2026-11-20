/**
 * WebSocket 接入层（演示用）：盘口推送 + 交易下单 / 撤单。
 *
 * <p><b>盘口端点</b>：前端通过 {@code /ws/orderbook} 建立原生 WebSocket 连接，发送订阅指令
 * 选择交易对；服务端默认每 {@code 500ms} 从撮合引擎读取一次该交易对的内存订单簿快照并推送。
 * 切换交易对时客户端重新发送订阅指令，服务端用最新订阅替换旧订阅（一个连接同一时刻
 * 只推送一个交易对）。
 *
 * <p>最新成交与 K 线不单独订阅：每帧 {@code orderbook} 消息都附带该交易对缓存中的最新
 * 50 笔成交（{@code trades} 字段，最新在前）以及各配置周期当前正在聚合的最新 K 线
 * （{@code klines} 字段），前端与盘口共用同一次订阅与推送。K 线走进程内聚合器，可能领先
 * 已落库数据一个刷新间隔；前端按 {@code bucketStart} 决定覆盖最后一根还是追加新柱子。
 *
 * <p><b>交易端点</b>：{@code /ws/trade} 通过 Aeron IPC 把 login 后的下单 / 撤单请求发给
 * 币币模块，只接收“已进入 ingress ring”的 ack。WS 不 import {@code com.me.currency} /
 * {@code com.me.order}，订单只走 shared 的 Aeron 契约；字段级校验、委托号生成和 ring 入队
 * 由币币模块完成。详细协议、失败 / 丢弃语义和独立部署约束见 README「WebSocket 下单 / 撤单」。
 *
 * <p>盘口端点只读：不修改订单簿、账户或委托状态，也不落库；订阅与推送全部在进程内完成，
 * 不经过 Kafka。交易端点只发布 Aeron 契约，同样不直接写订单簿 / 账户。
 *
 * <p>盘口对外消息协议：
 * <pre>
 * 客户端 → 服务端：
 *   {"action":"subscribe","symbol":"BTC-USDT","depth":10}
 *   {"action":"unsubscribe"}
 *
 * 服务端 → 客户端：
 *   {"type":"subscribed","symbol":"BTC-USDT","depth":10}
 *   {"type":"orderbook","symbol":"BTC-USDT","depth":10,"bids":[...],"asks":[...],
 *    "bestBid":...,"bestAsk":...,
 *    "trades":[{"tradeId":1,"price":...,"quantity":...,"takerSide":"BUY",...}],
 *    "klines":[{"symbol":"BTC-USDT","interval":"1m","bucketStart":"...","openPrice":...,
 *               "highPrice":...,"lowPrice":...,"closePrice":...,"volume":...,...}],
 *    "timestamp":"..."}
 *   {"type":"unsubscribed"}
 *   {"type":"error","symbol":"...","reason":"SYMBOL_INVALID"}
 * </pre>
 */
package com.me.websocket;
