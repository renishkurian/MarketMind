import React, { useEffect, useRef } from 'react';
import { createChart, ColorType, LineSeries } from 'lightweight-charts';

const calculateRSI = (prices, period) => {
  let rsi = [];
  let gains = 0, losses = 0;
  for (let i = 1; i < prices.length; i++) {
    const diff = prices[i].close - prices[i - 1].close;
    const gain = diff > 0 ? diff : 0;
    const loss = diff < 0 ? -diff : 0;
    if (i <= period) {
      gains += gain; losses += loss;
      if (i === period) {
        let avgGain = gains / period, avgLoss = losses / period;
        let rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
        rsi.push({ time: prices[i].time, value: 100 - (100 / (1 + rs)), avgGain, avgLoss });
      }
    } else {
      const prev = rsi[rsi.length - 1];
      let avgGain = (prev.avgGain * (period - 1) + gain) / period;
      let avgLoss = (prev.avgLoss * (period - 1) + loss) / period;
      let rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
      rsi.push({ time: prices[i].time, value: 100 - (100 / (1 + rs)), avgGain, avgLoss });
    }
  }
  return rsi;
};

const RSIChart = ({ data, range = '3M', theme = 'dark', length = 14 }) => {
  const rangeMap = { '1W': 7, '1M': 21, '3M': 63, '6M': 126, '1Y': 252, 'ALL': 9999 };
  const chartContainerRef = useRef();
  const chartRef = useRef(null);
  const seriesRef = useRef(null);

  // ── Create chart once on mount ──────────────────────────────────────────
  useEffect(() => {
    if (!chartContainerRef.current) return;
    const isDark = theme === 'dark';

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: isDark ? '#111827' : '#ffffff' },
        textColor: isDark ? '#9CA3AF' : '#4B5563',
      },
      grid: {
        vertLines: { color: isDark ? '#1F2937' : '#E5E7EB' },
        horzLines: { color: isDark ? '#1F2937' : '#E5E7EB' },
      },
      width: chartContainerRef.current.clientWidth,
      height: 100,
      timeScale: { borderColor: isDark ? '#1F2937' : '#E5E7EB', visible: false },
      rightPriceScale: { borderColor: isDark ? '#1F2937' : '#E5E7EB' },
    });

    const rsiSeries = chart.addSeries(LineSeries, { color: '#A855F7', lineWidth: 2 });
    rsiSeries.createPriceLine({ price: 70, color: '#EF4444', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: 'Overbought' });
    rsiSeries.createPriceLine({ price: 30, color: '#10B981', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: 'Oversold' });

    chartRef.current = chart;
    seriesRef.current = rsiSeries;

    const handleResize = () => {
      if (chartContainerRef.current) chart.applyOptions({ width: chartContainerRef.current.clientWidth });
    };
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [theme]);

  // ── Update data while preserving zoom ─────────────────────────────────
  useEffect(() => {
    if (!seriesRef.current || !data || data.length < length) return;

    const visibleRange = chartRef.current.timeScale().getVisibleRange();

    const formattedPrices = data
      .map(d => ({ time: Math.floor(new Date(d.date).getTime() / 1000), close: d.close }))
      .sort((a, b) => a.time - b.time);

    const rsiData = calculateRSI(formattedPrices, length);
    const limit = rangeMap[range] ?? 63;
    seriesRef.current.setData(rsiData.slice(-limit).map(d => ({ time: d.time, value: d.value })));

    if (visibleRange) chartRef.current.timeScale().setVisibleRange(visibleRange);
  }, [data, length, range]);

  return (
    <div className="relative">
      <div className="absolute top-2 left-2 z-10 text-[10px] font-bold text-dark-muted uppercase">RSI (14)</div>
      <div ref={chartContainerRef} className="w-full h-[100px]" />
    </div>
  );
};

export default RSIChart;
