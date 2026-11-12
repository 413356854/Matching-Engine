package com.me.account.ingress;

/**
 * handler 处理完一批后的结果，随 future 回到 poll 线程。
 */
record AccountIngressOutcome(Object result) {
}
