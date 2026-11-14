/**
 * 跨模块事件 / DTO 契约：资金流水（account → admin）、冻结回执（account → order）、
 * 撮合对（order → marketdata / admin）。{@link com.me.shared.event.TradeEvent} 现在只作为
 * 撮合内核到委托清算的进程内 DTO，不再写 Kafka；不暴露任何业务模块的内部类型。
 */
package com.me.shared.event;
