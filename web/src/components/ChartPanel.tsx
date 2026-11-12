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
import type { Kline, TradingPair } from '../api';
import {
  formatPercent,
  formatPrice,
  formatQuantity,
  formatTime,
  percentChange,
} from '../format';
import { ChevronDownIcon } from './icons';

interface ChartPanelProps {
  pair?: TradingPair;
  klines: Kline[];
  error: string | null;
  onOpenMarkets: () => void;
}

function toSeconds(iso: string): number {
  return Math.floor(new Date(iso).getTime() / 1000);
}

/** lightweight-charts 默认按 UTC 渲染时间轴；这里统一改成浏览器本地时间。 */
function chartTimeLabel(time: Time, withSeconds = false): string {
  if (typeof time !== 'number') {
    return '';
  }
  const date = new Date(time * 1000);
  const pad = (value: number) => String(value).padStart(2, '0');
  const base = `${pad(date.getHours())}:${pad(date.getMinutes())}`;
  return withSeconds ? `${base}:${pad(date.getSeconds())}` : base;
}

export function ChartPanel({ pair, klines, error, onOpenMarkets }: ChartPanelProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volumeRef = useRef<ISeriesApi<'Histogram'> | null>(null);
  const previousSymbolRef = useRef('');
  const hasDataRef = useRef(false);
  const klineMapRef = useRef(new Map<number, Kline>());
  const [hoveredTime, setHoveredTime] = useState<number | null>(null);

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
        scaleMargins: { top: 0.08, bottom: 0.26 },
      },
      timeScale: {
        borderVisible: false,
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 4,
        barSpacing: 8,
        minBarSpacing: 3,
        tickMarkFormatter: (time: Time) => chartTimeLabel(time),
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
        timeFormatter: (time: Time) => chartTimeLabel(time, true),
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
      previousSymbolRef.current = '';
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
    candles.setData(candleData);
    volume.setData(volumeData);

    const symbolChanged = previousSymbolRef.current !== pair.symbol;
    if (symbolChanged) {
      previousSymbolRef.current = pair.symbol;
      setHoveredTime(null);
    }
    if (candleData.length > 0 && (symbolChanged || !hasDataRef.current)) {
      chart.timeScale().fitContent();
    }
    hasDataRef.current = candleData.length > 0;
  }, [klines, pair]);

  const latest = klines.length > 0 ? klines[klines.length - 1] : undefined;
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
            <span className="interval-badge">1m</span>
          </div>
          <div className={`last-price ${priceDirection}`}>
            {pair && latest ? formatPrice(latest.closePrice, pair.priceScale) : '--'}
            <small>{pair?.quoteCurrency ?? ''}</small>
            <em className={change !== null && change < 0 ? 'down' : 'up'}>
              {formatPercent(change)}
            </em>
          </div>
        </div>

        <div className="chart-meta">
          <span className="live-dot" title="K 线每 5 秒刷新" />
          <span>1m K 线</span>
          <span className="meta-divider">·</span>
          <span>{klines.length} 根</span>
          {error && <span className="meta-error">{error}</span>}
        </div>
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
                : '行情模块会把每笔成交聚合成 1m K 线，撮合产生成交后这里会自动出现蜡烛图。'}
            </p>
          </div>
        )}
      </div>
    </section>
  );
}
