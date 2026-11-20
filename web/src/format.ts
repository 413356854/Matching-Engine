/**
 * 数值格式化工具。
 *
 * 后端所有金额都是最小单位整数，这里用 BigInt 做精确的十进制换算，
 * 避免浮点误差；只有画图时才转成 Number。
 */

export interface FormatOptions {
  /** 最少保留的小数位（默认 0）。 */
  minFractionDigits?: number;
  /** 去掉末尾多余的 0（默认 true）。 */
  trim?: boolean;
  /** 整数部分加千分位（默认 true）。 */
  grouping?: boolean;
}

function toBigInt(raw: number | bigint | string): bigint {
  if (typeof raw === 'bigint') {
    return raw;
  }
  if (typeof raw === 'number') {
    if (!Number.isFinite(raw)) {
      return 0n;
    }
    return BigInt(Math.trunc(raw));
  }
  const value = raw.trim();
  if (!/^-?\d+$/.test(value)) {
    return 0n;
  }
  return BigInt(value);
}

/** 整数最小单位 → 展示字符串，例如 formatUnits(123456789, 8) === "1.23456789"。 */
export function formatUnits(
  raw: number | bigint | string,
  scale: number,
  options: FormatOptions = {},
): string {
  const safeScale = Math.max(0, Math.min(Math.trunc(scale), 18));
  const value = toBigInt(raw);
  const negative = value < 0n;
  const digits = (negative ? -value : value).toString().padStart(safeScale + 1, '0');
  const whole = digits.slice(0, digits.length - safeScale);
  let fraction = safeScale > 0 ? digits.slice(digits.length - safeScale) : '';

  if (options.trim !== false) {
    fraction = fraction.replace(/0+$/, '');
  }
  const minFraction = Math.max(0, Math.min(options.minFractionDigits ?? 0, safeScale));
  fraction = fraction.padEnd(minFraction, '0');

  const wholeText =
    options.grouping === false ? whole : whole.replace(/\B(?=(\d{3})+(?!\d))/g, ',');

  return `${negative ? '-' : ''}${wholeText}${fraction ? `.${fraction}` : ''}`;
}

/**
 * 价格 × 数量 → 计价币最小单位金额，无法整除时返回 `null`。
 *
 * 与后端 `com.me.shared.money.Amounts.quoteAmount` 的整除要求一致：不能为了展示
 * 而向下取整，否则不同价格（例如 0.1211 / 0.1299）在较粗的金额精度下会显示成同一个
 * 金额，用户会误以为价格无效。返回 `null` 表示该价格 × 数量无法用当前计价币精度
 * 精确记账，表单应阻止提交，而不是显示一个截断值。
 */
export function exactQuoteAmount(
  price: bigint,
  quantity: bigint,
  priceScale: number,
  quantityScale: number,
  quoteScale: number,
): bigint | null {
  const numerator = price * quantity * 10n ** BigInt(Math.max(0, quoteScale));
  const denominator = 10n ** BigInt(
    Math.max(0, priceScale) + Math.max(0, quantityScale),
  );
  if (numerator % denominator !== 0n) {
    return null;
  }
  return numerator / denominator;
}

/**
 * raw 整数按 `rawScale` 解释，并按 `displayScale` 截断展示。
 *
 * <p>`displayScale` 是展示上限（币种 `scale` 或交易对 `priceScale`），必须
 * `<= rawScale`。账户/撮合始终按 raw 精度计算；截断只影响展示，不改变账本。
 */
export function formatDisplayUnits(
  raw: number | bigint | string,
  rawScale: number,
  displayScale: number,
  options: FormatOptions = {},
): string {
  const safeRawScale = Math.max(0, Math.min(Math.trunc(rawScale), 18));
  const safeDisplayScale = Math.max(
    0,
    Math.min(Math.trunc(displayScale), safeRawScale),
  );
  if (safeDisplayScale >= safeRawScale) {
    return formatUnits(raw, safeRawScale, options);
  }
  const divisor = 10n ** BigInt(safeRawScale - safeDisplayScale);
  return formatUnits(toBigInt(raw) / divisor, safeDisplayScale, options);
}

/**
 * 数量最小步长：保证任意整数价格下 `price × quantity` 都能换算成计价币最小单位。
 *
 * 与后端 `com.me.shared.money.Amounts.isQuantityAligned` 使用同一规则：
 * `10^max(0, priceScale + quantityScale - quoteScale)`。
 */
export function quantityStep(
  priceScale: number,
  quantityScale: number,
  quoteScale: number,
): bigint {
  const netScale = Math.max(
    0,
    Math.trunc(priceScale) + Math.trunc(quantityScale) - Math.trunc(quoteScale),
  );
  return 10n ** BigInt(netScale);
}

/** 数量是否对齐最小步长；正整数且能被步长整除时为 true。 */
export function isQuantityAligned(
  quantity: bigint,
  priceScale: number,
  quantityScale: number,
  quoteScale: number,
): boolean {
  if (quantity <= 0n) {
    return false;
  }
  return quantity % quantityStep(priceScale, quantityScale, quoteScale) === 0n;
}

/** 把数量向下对齐到最小步长；用于 25% / 50% 等按余额估算的快捷填量。 */
export function alignQuantityDown(
  quantity: bigint,
  priceScale: number,
  quantityScale: number,
  quoteScale: number,
): bigint {
  const step = quantityStep(priceScale, quantityScale, quoteScale);
  return step === 1n ? quantity : (quantity / step) * step;
}

/** 市价买入预算倍数：参考价 × 1.05 留出价格波动缓冲。 */
export const MARKET_BUY_BUFFER_NUMERATOR = 105n;
export const MARKET_BUY_BUFFER_DENOMINATOR = 100n;

/**
 * 市价买入冻结预算 = 参考价 × 1.05 × 数量，按计价币最小单位向下取整。
 *
 * 参考价取卖一价；没有卖一时取最新成交价。结果就是传给后端、由账户模块冻结的 `amount`。
 */
export function marketBuyAmount(
  price: bigint,
  quantity: bigint,
  priceScale: number,
  quantityScale: number,
  quoteScale: number,
): bigint {
  const numerator =
    price * quantity * MARKET_BUY_BUFFER_NUMERATOR * 10n ** BigInt(Math.max(0, quoteScale));
  const denominator =
    MARKET_BUY_BUFFER_DENOMINATOR *
    10n ** BigInt(Math.max(0, priceScale) + Math.max(0, quantityScale));
  return numerator / denominator;
}

/** 展示值 → 整数最小单位；非法输入或小数位超限返回 null。 */
export function parseUnits(value: string, scale: number): bigint | null {
  const text = value.trim();
  if (text === '' || !/^(\d+(\.\d*)?|\.\d+)$/.test(text)) {
    return null;
  }

  const safeScale = Math.max(0, Math.min(Math.trunc(scale), 18));
  const [wholePart, fractionPart = ''] = text.split('.');
  if (fractionPart.length > safeScale) {
    return null;
  }

  const digits = `${wholePart || '0'}${fractionPart.padEnd(safeScale, '0')}`.replace(
    /^0+(?=\d)/,
    '',
  );
  try {
    return BigInt(digits);
  } catch {
    return null;
  }
}

/** 价格固定展示到交易对精度，例如 50000 + scale 2 → "500.00"。 */
export function formatPrice(raw: number | bigint | string, priceScale: number): string {
  return formatUnits(raw, priceScale, { minFractionDigits: priceScale });
}

/** 数量按精度展示，去掉末尾多余的 0。 */
export function formatQuantity(raw: number | bigint | string, quantityScale: number): string {
  return formatUnits(raw, quantityScale);
}

/** 输入框回填用：不带千分位、不带多余 0。 */
export function formatInputValue(raw: number | bigint | string, scale: number): string {
  return formatUnits(raw, scale, { grouping: false });
}

/** 把 BigInt 转成可安全传给 JSON 的 number；超出安全整数范围返回 null。 */
export function toSafeNumber(value: bigint): number | null {
  if (value > BigInt(Number.MAX_SAFE_INTEGER) || value < BigInt(Number.MIN_SAFE_INTEGER)) {
    return null;
  }
  return Number(value);
}

export function formatTime(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return '--:--:--';
  }
  const pad = (value: number) => String(value).padStart(2, '0');
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

export function formatDateTime(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return '--';
  }
  const pad = (value: number) => String(value).padStart(2, '0');
  return `${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(
    date.getMinutes(),
  )}:${pad(date.getSeconds())}`;
}

/** 涨跌幅（百分比数值，例如 1.23 表示 +1.23%）。 */
export function percentChange(current: number, previous: number): number | null {
  if (!Number.isFinite(current) || !Number.isFinite(previous) || previous === 0) {
    return null;
  }
  return ((current - previous) / previous) * 100;
}

export function formatPercent(value: number | null): string {
  if (value === null) {
    return '--';
  }
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(2)}%`;
}
