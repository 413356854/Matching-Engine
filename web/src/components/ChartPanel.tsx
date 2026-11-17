import { useEffect, useRef, useState } from 'react';
import {
  CandlestickSeries,
  ColorType,
  CrosshairMode,
  HistogramSeries,
  createChart,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
  type Time,
  type UTCTimestamp,
} from 'lightweight-charts';
import {
  KLINE_INTERVALS,
  type BotStatus,
  type Kline,
  type KlineInterval,
  type TradingPair,
} from '../api';
import {
  formatPercent,
  formatPrice,
  formatQuantity,
  formatTime,
  percentChange,
} from '../format';
import { BotTradingButton } from './BotTradingButton';
import { ChevronDownIcon } from './icons';

interface ChartPanelProps {
  pair?: TradingPair;
  klines: Kline[];
  interval: KlineInterval;
  onIntervalChange: (interval: KlineInterval) => void;
  error: string | null;
  onOpenMarkets: () => void;
  botStatus: BotStatus | null;
  botBusy: boolean;
  onToggleBot: () => void;
}

function toSeconds(iso: string): number {
  return Math.floor(new Date(iso).getTime() / 1000);
}

function pad(value: number): string {
  return String(value).padStart(2, '0');
}

/**
 * lightweight-charts 默认按 UTC 渲染时间轴；这里统一改成浏览器本地时间。
 * 周期越大刻度越粗：分钟级显示时分，日线显示月-日，月线显示年-月。
 */
function chartTimeLabel(time: Time, interval: KlineInterval, detailed = false): string {
  if (typeof time !== 'number') {
    return '';
  }
  const date = new Date(time * 1000);
  const yearMonth = `${date.getFullYear()}-${pad(date.getMonth() + 1)}`;
  const monthDay = `${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
  const hourMinute = `${pad(date.getHours())}:${pad(date.getMinutes())}`;
  if (interval === '1M') {
    return yearMonth;
  }
  if (interval === '1d') {
    return detailed ? `${yearMonth}-${pad(date.getDate())}` : monthDay;
  }
  if (interval === '12h') {
    return detailed
      ? `${yearMonth}-${pad(date.getDate())} ${hourMinute}`
      : `${monthDay} ${hourMinute}`;
  }
  return detailed
    ? `${yearMonth}-${pad(date.getDate())} ${hourMinute}:${pad(date.getSeconds())}`
    : hourMinute;
}

export function ChartPanel({
  pair,
  klines,
  interval,
  onIntervalChange,
  error,
  onOpenMarkets,
  botStatus,
  botBusy,
  onToggleBot,
}: ChartPanelProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volumeRef = useRef<ISeriesApi<'Histogram'> | null>(null);
  const previousViewRef = useRef('');
  const hasDataRef = useRef(false);
  // 已经写入图表的最后一根柱子的 bucket 秒值；用于增量 update，让图表自动跟随最新柱子。
  const seriesLastRef = useRef<number | null>(null);
  const klineMapRef = useRef(new Map<number, Kline>());
  // 图表创建时注册的刻度格式化回调只执行一次，通过 ref 读取最新周期。
  const intervalRef = useRef<KlineInterval>(interval);
  const [hoveredTime, setHoveredTime] = useState<number | null>(null);

  useEffect(() => {
    intervalRef.current = interval;
  }, [interval]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) {
      return;
    }

    const chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: '#768390',
        attributionLogo: false,
        fontFamily:
          '"SF Mono", "JetBrains Mono", "Cascadia Code", Consolas, "Roboto Mono", monospace',
        fontSize: 11,
      },
      grid: {
        vertLines: { color: 'rgba(38, 46, 56, 0.55)' },
        horzLines: { color: 'rgba(38, 46, 56, 0.55)' },
      },
      rightPriceScale: {
        borderVisible: false,
        // 顶部留出 OHLC 浮层的高度，避免最高价的蜡烛被图例盖住。
        scaleMargins: { top: 0.12, bottom: 0.26 },
      },
      timeScale: {
        borderVisible: false,
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 4,
        barSpacing: 8,
        minBarSpacing: 3,
        // 追加新柱子时保持可视区域跟随最新数据（配合 series.update）。
        shiftVisibleRangeOnNewBar: true,
        tickMarkFormatter: (time: Time) => chartTimeLabel(time, intervalRef.current),
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: {
          color: 'rgba(132, 142, 156, 0.45)',
          width: 1,
          style: 3,
          labelBackgroundColor: '#2a323c',
        },
        horzLine: {
          color: 'rgba(132, 142, 156, 0.45)',
          width: 1,
          style: 3,
          labelBackgroundColor: '#2a323c',
        },
      },
      localization: {
        locale: 'zh-CN',
        timeFormatter: (time: Time) =>
          chartTimeLabel(time, intervalRef.current, true),
      },
    });

    const candles = chart.addSeries(CandlestickSeries, {
      upColor: '#0ecb81',
      downColor: '#f6465d',
      borderVisible: false,
      wickUpColor: '#0ecb81',
      wickDownColor: '#f6465d',
      priceLineColor: 'rgba(240, 185, 11, 0.55)',
      lastValueVisible: true,
    });

    const volume = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
      lastValueVisible: false,
      priceLineVisible: false,
    });
    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.82, bottom: 0 },
    });

    chart.subscribeCrosshairMove((param) => {
      if (!param.point || param.time === undefined || typeof param.time !== 'number') {
        setHoveredTime(null);
        return;
      }
      setHoveredTime(param.time);
    });

    chartRef.current = chart;
    candleRef.current = candles;
    volumeRef.current = volume;

    return () => {
      chart.remove();
      chartRef.current = null;
      candleRef.current = null;
      volumeRef.current = null;
      previousViewRef.current = '';
      seriesLastRef.current = null;
    };
  }, []);

  useEffect(() => {
    const chart = chartRef.current;
    const candles = candleRef.current;
    const volume = volumeRef.current;
    if (!chart || !candles || !volume || !pair) {
      return;
    }

    const precision = Math.max(0, Math.min(pair.priceScale, 8));
    const priceDivisor = 10 ** pair.priceScale;
    const quantityDivisor = 10 ** pair.quantityScale;
    const unique = new Map<number, Kline>();

    for (const kline of klines) {
      // 切换周期后旧请求的响应可能还在填充状态，只渲染当前周期的数据。
      if (kline.interval !== interval) {
        continue;
      }
      const time = toSeconds(kline.bucketStart);
      if (Number.isFinite(time)) {
        unique.set(time, kline);
      }
    }

    const times = [...unique.keys()].sort((a, b) => a - b);
    const candleData: CandlestickData<Time>[] = [];
    const volumeData: HistogramData<Time>[] = [];

    for (const time of times) {
      const kline = unique.get(time)!;
      candleData.push({
        time: time as UTCTimestamp,
        open: kline.openPrice / priceDivisor,
        high: kline.highPrice / priceDivisor,
        low: kline.lowPrice / priceDivisor,
        close: kline.closePrice / priceDivisor,
      });
      volumeData.push({
        time: time as UTCTimestamp,
        value: kline.volume / quantityDivisor,
        color:
          kline.closePrice >= kline.openPrice
            ? 'rgba(14, 203, 129, 0.42)'
            : 'rgba(246, 70, 93, 0.42)',
      });
    }

    klineMapRef.current = unique;
    candles.applyOptions({
      priceFormat: {
        type: 'price',
        precision,
        minMove: Number((10 ** -precision).toFixed(precision)),
      },
    });
    // 交易对或周期变化时重置视图：重新铺满可视区域，并清掉旧的十字光标状态。
    const viewKey = `${pair.symbol}|${interval}`;
    const viewChanged = previousViewRef.current !== viewKey;
    if (viewChanged) {
      previousViewRef.current = viewKey;
      setHoveredTime(null);
    }

    const lastTime = times.length > 0 ? times[times.length - 1] : null;
    const previousLastTime = seriesLastRef.current;
    const previousBarTime = times.length > 1 ? times[times.length - 2] : null;
    const lastIndex = candleData.length - 1;

    if (lastTime === null) {
      candles.setData(candleData);
      volume.setData(volumeData);
    } else if (viewChanged || !hasDataRef.current || previousLastTime === null) {
      // 首次加载 / 切换交易对或周期：整批写入并铺满可视区域。
      candles.setData(candleData);
      volume.setData(volumeData);
    } else if (lastTime === previousLastTime) {
      // 同一根柱子继续成交：只刷新最后一根，不重建整段数据。
      candles.update(candleData[lastIndex]);
      volume.update(volumeData[lastIndex]);
    } else if (lastTime > previousLastTime && previousBarTime === previousLastTime) {
      // 正常跨入新周期：update 追加一根，时间轴按 shiftVisibleRangeOnNewBar 跟随最新柱子。
      candles.update(candleData[lastIndex]);
      volume.update(volumeData[lastIndex]);
    } else {
      // 断线补齐 / REST 重新加载 / 管理端清空：数据整体变化时整批替换。
      candles.setData(candleData);
      volume.setData(volumeData);
    }

    if (lastTime !== null && (viewChanged || !hasDataRef.current)) {
      chart.timeScale().fitContent();
    }
    hasDataRef.current = lastTime !== null;
    seriesLastRef.current = lastTime;
  }, [interval, klines, pair]);

  const latest =
    klines.length > 0 && klines[klines.length - 1].interval === interval
      ? klines[klines.length - 1]
      : undefined;
  const hovered =
    hoveredTime !== null ? klineMapRef.current.get(hoveredTime) : undefined;
  const legend = hovered ?? latest;
  const change =
    latest === undefined ? null : percentChange(latest.closePrice, latest.openPrice);
  const priceDirection =
    latest === undefined ? 'flat' : latest.closePrice > latest.openPrice
      ? 'up'
      : latest.closePrice < latest.openPrice
        ? 'down'
        : 'flat';
  const legendDirection =
    legend === undefined ? 'flat' : legend.closePrice >= legend.openPrice ? 'up' : 'down';

  const displaySymbol = pair ? `${pair.baseCurrency}/${pair.quoteCurrency}` : '--';

  return (
    <section className="panel chart-panel">
      <div className="chart-head">
        <div className="pair-heading">
          <div className="pair-symbol">
            <button
              type="button"
              className="pair-select"
              onClick={onOpenMarkets}
              title="切换交易对"
              aria-label="切换交易对"
            >
              <b>{displaySymbol}</b>
              <ChevronDownIcon />
            </button>
            <span className="interval-badge">{interval}</span>
          </div>
          <div className={`last-price ${priceDirection}`}>
            {pair && latest ? formatPrice(latest.closePrice, pair.priceScale) : '--'}
            <small>{pair?.quoteCurrency ?? ''}</small>
            <em className={change !== null && change < 0 ? 'down' : 'up'}>
              {formatPercent(change)}
            </em>
          </div>
        </div>

        <div className="chart-head-right">
          <div className="chart-meta">
            <span className="live-dot" title="K 线通过 WebSocket 实时推送" />
            <span>{interval} K 线</span>
            <span className="meta-divider">·</span>
            <span>{klines.length} 根</span>
            {error && <span className="meta-error">{error}</span>}
          </div>
          <BotTradingButton
            status={botStatus}
            busy={botBusy}
            onToggle={onToggleBot}
          />
        </div>
      </div>

      <div className="chart-intervals" role="group" aria-label="K 线周期">
        {KLINE_INTERVALS.map((value) => (
          <button
            key={value}
            type="button"
            className={value === interval ? 'interval-switch active' : 'interval-switch'}
            aria-pressed={value === interval}
            onClick={() => onIntervalChange(value)}
          >
            {value}
          </button>
        ))}
      </div>

      <div className="chart-shell">
        <div className={`ohlc-legend ${legendDirection}`}>
          {legend ? (
            <>
              <span className="legend-time">{formatTime(legend.bucketStart)}</span>
              <span>
                开 <b>{pair ? formatPrice(legend.openPrice, pair.priceScale) : '--'}</b>
              </span>
              <span>
                高 <b>{pair ? formatPrice(legend.highPrice, pair.priceScale) : '--'}</b>
              </span>
              <span>
                低 <b>{pair ? formatPrice(legend.lowPrice, pair.priceScale) : '--'}</b>
              </span>
              <span>
                收 <b>{pair ? formatPrice(legend.closePrice, pair.priceScale) : '--'}</b>
              </span>
              <span>
                量 <b>{pair ? formatQuantity(legend.volume, pair.quantityScale) : '--'}</b>
                {pair ? ` ${pair.baseCurrency}` : ''}
              </span>
            </>
          ) : (
            <span className="legend-idle">暂无 K 线数据</span>
          )}
        </div>

        <div ref={containerRef} className="chart-container" />

        {klines.length === 0 && (
          <div className="chart-empty">
            <div className="chart-empty-icon" aria-hidden="true">
              <span />
              <span />
              <span />
              <span />
              <span />
            </div>
            <strong>{error ? 'K 线加载失败' : '等待第一笔成交'}</strong>
            <p>
              {error
                ? error
                : `行情模块会把每笔成交聚合成 ${interval} K 线，撮合产生成交后这里会自动出现蜡烛图。`}
            </p>
          </div>
        )}
      </div>
    </section>
  );
}
