/**
 * 机器人模拟模块（演示用接入层，不是新的业务模块）。
 *
 * <p>本模块把币安现货行情接入撮合系统：启动时用固定用户充值演示资金，
 * 通过币安 WebSocket {@code miniTicker} 流订阅 BTC / ETH 最新价，开启自动交易后
 * 按配置的间隔（默认 500ms）逐交易对提交限价单，价格取币安最新价，买卖方向交替。
 * 下单复用币币模块的 {@code OrderCommandAssembler} + {@code OrderCommandPublisher}，
 * 不直接创建委托、不直接修改账户，也不把机器人命令发送到其它入口。
 *
 * <p>开启自动交易前先调用币安 REST 测试接口（默认 {@code /api/v3/ping}）；
 * 不可达时拒绝开启，前端提示需要配置可访问币安的网络（代理 / 科学联网）。
 * HTTP 与 WebSocket 共用可选的 HTTP 代理；未配置代理时使用 JVM 的系统代理。
 *
 * <p>自动交易状态只保存在内存中，重启后默认关闭，需要重新点击开启。
 */
package com.me.bot;
