import React, { useEffect, useRef } from 'react';
import { createChart, ColorType, LineSeries, HistogramSeries } from 'lightweight-charts';

const calculateEMA = (data, period) => {
  if (data.length < period) return [];
  const k = 2 / (period + 1);
  const seed = data.slice(0, period).reduce((sum, d) => sum + d.value, 0) / period;
  let prevEma = seed;
  return data.slice(period - 1).map((d, i) => {
    const currentEma = i === 0 ? seed : (d.value - prevEma) * k + prevEma;
    prevEma = currentEma;
    return { time: d.time, value: currentEma };
  });
};

const MACDChart = ({ data, range = '3M', theme = 'dark' }) => {
  const rangeMap = { '1W': 7, '1M': 21, '3M': 63, '6M': 126, '1Y': 252, 'ALL': 9999 };
  const chartContainerRef = useRef();
  const chartRef = useRef(null);
  const macdRef = useRef(null);
  const signalRef = useRef(null);
  const histRef = useRef(null);

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
      height: 120,
      timeScale: { borderColor: isDark ? '#1F2937' : '#E5E7EB' },
      rightPriceScale: { borderColor: isDark ? '#1F2937' : '#E5E7EB' },
    });

    chartRef.current = chart;
    macdRef.current   = chart.addSeries(LineSeries,      { color: '#2563EB', lineWidth: 1, title: 'MACD' });
    signalRef.current = chart.addSeries(LineSeries,      { color: '#F59E0B', lineWidth: 1, title: 'Signal' });
    histRef.current   = chart.addSeries(HistogramSeries, { title: 'Histogram' });

    const handleResize = () => {
      if (chartContainerRef.current) chart.applyOptions({ width: chartContainerRef.current.clientWidth });
    };
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
      chartRef.current = macdRef.current = signalRef.current = histRef.current = null;
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [theme]);

  // ── Update data while preserving zoom ─────────────────────────────────
  useEffect(() => {
    if (!macdRef.current || !data || data.length < 26) return;

    const visibleRange = chartRef.current.timeScale().getVisibleRange();

    const formattedData = data
      .map(d => ({ time: Math.floor(new Date(d.date).getTime() / 1000), value: d.close }))
      .sort((a, b) => a.time - b.time);

    const ema12 = calculateEMA(formattedData, 12);
    const ema26 = calculateEMA(formattedData, 26);
    
    // Align: MACD line starts only where both series exist (effectively where EMA26 starts)
    const macdLine = ema26.map(e26 => {
      const e12 = ema12.find(e => e.time === e26.time);
      return e12 ? { time: e26.time, value: e12.value - e26.value } : null;
    }).filter(Boolean);

    const signalLine = calculateEMA(macdLine, 9);
    const histogram  = macdLine.map((ml, i) => {
      const sl = signalLine.find(s => s.time === ml.time);
      if (!sl) return null;
      const val = ml.value - sl.value;
      return { time: ml.time, value: val, color: val >= 0 ? '#10B981' : '#EF4444' };
    }).filter(Boolean);

    const limit = rangeMap[range] ?? 63;
    macdRef.current.setData(macdLine.slice(-limit));
    signalRef.current.setData(signalLine.slice(-limit));
    histRef.current.setData(histogram.slice(-limit));

    if (visibleRange) chartRef.current.timeScale().setVisibleRange(visibleRange);
  }, [data, range]);

  return (
    <div className="relative">
      <div className="absolute top-2 left-2 z-10 text-[10px] font-bold text-dark-muted uppercase">MACD (12, 26, 9)</div>
      <div ref={chartContainerRef} className="w-full h-[120px]" />
    </div>
  );
};

export default MACDChart;
