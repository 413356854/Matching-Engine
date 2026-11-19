package com.me.currency.ingress;

/**
 * 非阻塞写入 ingress ring 的结果。
 *
 * <p>HTTP 入口仍通过 {@code submit} 抛异常返回 503；Aeron 消费者使用 {@code trySubmit}
 * 区分“ring 已满”和“ring 未启动 / 处理异常”，以便回不同的 ack 或直接丢弃。
 */
public enum IngressSubmitResult {

    /** 命令已进入 ring。 */
    ACCEPTED,

    /** ring 已满；当前实现不重试，由调用方按丢弃或 503 处理。 */
    FULL,

    /** ring 未启动或已停止。 */
    NOT_RUNNING,

    /** 入队时发生未预期异常。 */
    ERROR
}
