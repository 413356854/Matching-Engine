package com.me.account.ingress;

/** account 消费入口的两种 poll 批次。 */
enum AccountIngressKind {
    COMMAND,
    SETTLEMENT
}
