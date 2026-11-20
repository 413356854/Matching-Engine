package com.me.shared.money;

import java.math.BigInteger;

/**
 * 价格 × 数量到计价币最小单位金额的换算。
 *
 * <p>价格按 {@code 10^-priceScale}、数量按 {@code 10^-quantityScale} 的整数存储；
 * 计价币金额按 {@code 10^-quoteScale} 的整数存储。这里的 {@code quoteScale} 是
 * 计价币的 <b>raw_scale</b>（账本原始精度），不是币种的展示小数上限。因此：
 *
 * <pre>
 * amount = price × quantity × 10^quoteScale / 10^(priceScale + quantityScale)
 * </pre>
 *
 * <p>例如 BTC-USDT（priceScale=2、quantityScale=8、USDT scale=6）：
 * 价格 8,000,000（80,000.00）× 数量 100,000,000（1 BTC）应得到
 * 80,000,000,000 个 USDT 最小单位，即 80,000 USDT。
 *
 * <p>热路径先用 {@code long} 运算：把两个 10 的幂合并成一次放大或一次整除，
 * 只有 {@code price × quantity} 本身超出 long 的极端输入才回退到 {@link BigInteger}。
 * 结果必须整除且落在 {@code long} 范围内，否则抛出 {@link ArithmeticException}，
 * 由调用方按金额溢出 / 非法处理。
 */
public final class Amounts {

    private static final BigInteger TEN = BigInteger.TEN;
    private static final int MAX_SCALE = 18;

    /** 10^0 .. 10^18；10^18 仍在 long 范围内。 */
    private static final long[] POW10 = {
            1L,
            10L,
            100L,
            1_000L,
            10_000L,
            100_000L,
            1_000_000L,
            10_000_000L,
            100_000_000L,
            1_000_000_000L,
            10_000_000_000L,
            100_000_000_000L,
            1_000_000_000_000L,
            10_000_000_000_000L,
            100_000_000_000_000L,
            1_000_000_000_000_000L,
            10_000_000_000_000_000L,
            100_000_000_000_000_000L,
            1_000_000_000_000_000_000L
    };

    /**
     * 计算成交 / 冻结的计价币最小单位金额。
     *
     * @throws IllegalArgumentException price / quantity 非正或精度越界
     * @throws ArithmeticException      金额无法整除或超出 long 范围
     */
    public static long quoteAmount(long price, long quantity,
                                   int priceScale, int quantityScale, int quoteScale) {
        if (price <= 0) {
            throw new IllegalArgumentException("price must be positive: " + price);
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive: " + quantity);
        }
        checkScale(priceScale);
        checkScale(quantityScale);
        checkScale(quoteScale);

        // amount = price × quantity × 10^quoteScale / 10^(priceScale + quantityScale)
        // 两个 10 的幂合并后，只可能剩一次放大（netScale ≥ 0）或一次整除（netScale < 0）。
        int netScale = quoteScale - priceScale - quantityScale;
        if (netScale >= 0) {
            try {
                long raw = Math.multiplyExact(price, quantity);
                return netScale == 0 ? raw : Math.multiplyExact(raw, POW10[netScale]);
            } catch (ArithmeticException e) {
                throw new ArithmeticException("quote amount overflows long: price=" + price
                        + ", quantity=" + quantity + ", priceScale=" + priceScale
                        + ", quantityScale=" + quantityScale + ", quoteScale=" + quoteScale);
            }
        }

        long raw;
        try {
            raw = Math.multiplyExact(price, quantity);
        } catch (ArithmeticException productOverflow) {
            // 极端输入：price × quantity 超出 long，但除以 10^k 后可能仍落在 long 内。
            return quoteAmountWide(price, quantity, priceScale, quantityScale, quoteScale);
        }

        int divisorScale = -netScale;
        if (divisorScale > MAX_SCALE) {
            // raw > 0 且 raw < 2^63 < 10^19 ≤ 10^divisorScale，必然不能整除。
            throw notInteger(price, quantity, priceScale, quantityScale, quoteScale);
        }
        long divisor = POW10[divisorScale];
        long quotient = raw / divisor;
        if (raw - quotient * divisor != 0) {
            throw notInteger(price, quantity, priceScale, quantityScale, quoteScale);
        }
        return quotient;
    }

    /**
     * 判断委托数量是否对齐交易对的最小数量步长。
     *
     * <p>成交金额按
     * {@code price × quantity × 10^quoteScale / 10^(priceScale + quantityScale)}
     * 计算并要求整除。限价单可能与不同 maker 的价格成交，仅在自身限价上整除
     * 不足以保证每一笔成交都能精确记账；因此当
     * {@code priceScale + quantityScale > quoteScale} 时，数量必须是
     * {@code 10^(priceScale + quantityScale - quoteScale)} 的整数倍。满足该步长后，
     * 任意整数价格与数量的乘积都能换算成计价币最小单位。
     *
     * <p>若步长指数超过 {@link #MAX_SCALE}（步长大于 {@code Long.MAX_VALUE}），
     * 任何正 long 数量都无法满足，返回 {@code false}。这是交易对精度配置本身
     * 不可用，应由入口校验拒绝。
     *
     * @param quantity      委托数量，按 {@code 10^-quantityScale} 的最小单位
     * @param priceScale    价格精度
     * @param quantityScale 数量精度
     * @param quoteScale    计价币账本精度
     * @return 数量是否满足步长
     * @throws IllegalArgumentException scale 越界
     */
    public static boolean isQuantityAligned(long quantity, int priceScale,
                                            int quantityScale, int quoteScale) {
        if (quantity <= 0) {
            return false;
        }
        checkScale(priceScale);
        checkScale(quantityScale);
        checkScale(quoteScale);

        int netScale = priceScale + quantityScale - quoteScale;
        if (netScale <= 0) {
            return true;
        }
        if (netScale > MAX_SCALE) {
            // 步长 >= 10^19 > Long.MAX_VALUE，正 long 数量不可能对齐。
            return false;
        }
        return quantity % POW10[netScale] == 0L;
    }

    /**
     * 给定计价币预算，计算在 {@code price} 价位最多可成交的基础币数量。
     *
     * <p>用于市价买入：前端按「参考价 × 1.05 × 数量」冻结预算，撮合时按实际卖价把预算
     * 换算成可买数量，剩余预算退回。返回值保证：
     * <ul>
     *   <li>{@link #quoteAmount(long, long, int, int, int)} 能整除（金额可精确记账）；</li>
     *   <li>成交金额不超过 {@code budget}；</li>
     *   <li>预算不足以买入最小单位时返回 0。</li>
     * </ul>
     *
     * <p>例如 BTC-USDT（priceScale=2、quantityScale=8、quoteScale=6）预算 52,500 USDT、
     * 价格 50,000.00 时返回 1 BTC（100,000,000）……
     */
    public static long maxQuantityForBudget(long budget, long price,
                                            int priceScale, int quantityScale, int quoteScale) {
        if (budget <= 0 || price <= 0) {
            return 0L;
        }
        checkScale(priceScale);
        checkScale(quantityScale);
        checkScale(quoteScale);

        BigInteger priceValue = BigInteger.valueOf(price);
        BigInteger budgetValue = BigInteger.valueOf(budget);
        // amount = price × quantity × 10^quoteScale / 10^(priceScale + quantityScale)
        // 预算约束：price × quantity ≤ budget × 10^(priceScale + quantityScale - quoteScale)
        int netScale = priceScale + quantityScale - quoteScale;
        if (netScale >= 0) {
            BigInteger unit = TEN.pow(netScale);
            BigInteger maxQuantity = budgetValue.multiply(unit).divide(priceValue);
            if (maxQuantity.signum() <= 0) {
                return 0L;
            }
            // 必须满足 10^netScale | price × quantity，数量只能按 step 递增。
            BigInteger step = unit.divide(unit.gcd(priceValue));
            BigInteger aligned = maxQuantity.divide(step).multiply(step);
            BigInteger longMax = BigInteger.valueOf(Long.MAX_VALUE);
            if (aligned.compareTo(longMax) > 0) {
                // 极端预算：返回不超过 long 范围、且仍满足整除条件的最大值。
                aligned = longMax.divide(step).multiply(step);
            }
            return aligned.longValueExact();
        }
        BigInteger unitAmount = priceValue.multiply(TEN.pow(-netScale));
        BigInteger maxQuantity = budgetValue.divide(unitAmount);
        return maxQuantity.bitLength() >= 64 ? Long.MAX_VALUE : maxQuantity.longValueExact();
    }

    /** 仅用于 price × quantity 超出 long 的极端输入，保持与原始实现完全一致的语义。 */
    private static long quoteAmountWide(long price, long quantity,
                                        int priceScale, int quantityScale, int quoteScale) {
        BigInteger raw = BigInteger.valueOf(price).multiply(BigInteger.valueOf(quantity));
        BigInteger numerator = raw.multiply(TEN.pow(quoteScale));
        BigInteger[] result = numerator.divideAndRemainder(TEN.pow(priceScale + quantityScale));
        if (result[1].signum() != 0) {
            throw notInteger(price, quantity, priceScale, quantityScale, quoteScale);
        }
        return result[0].longValueExact();
    }

    private static ArithmeticException notInteger(long price, long quantity,
                                                  int priceScale, int quantityScale,
                                                  int quoteScale) {
        return new ArithmeticException("quote amount is not an integer: price=" + price
                + ", quantity=" + quantity + ", priceScale=" + priceScale
                + ", quantityScale=" + quantityScale + ", quoteScale=" + quoteScale);
    }

    private static void checkScale(int scale) {
        if (scale < 0 || scale > MAX_SCALE) {
            throw new IllegalArgumentException("scale out of range: " + scale);
        }
    }

    private Amounts() {
    }
}
