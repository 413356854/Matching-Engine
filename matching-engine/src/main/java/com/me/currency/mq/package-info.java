/**
 * 币币模块的 Kafka 适配层：只负责把跨模块命令写入 Kafka，禁止依赖 {@code com.me.order}。
 */
package com.me.currency.mq;
