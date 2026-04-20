import React, { useEffect, useRef } from 'react';
import { createChart, ColorType, CandlestickSeries, LineSeries } from 'lightweight-charts';

const CandlestickChart = ({ data, theme = 'dark', trendLines = [] }) => {
  const chartContainerRef = useRef();
  const chartRef = useRef(null);
  const seriesRef = useRef(null);
  const trendSeriesRef = useRef([]);

  // ── Init chart ONCE on mount ──────────────────────────────────────────────
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
      height: 400,
      timeScale: {
        borderColor: isDark ? '#1F2937' : '#E5E7EB',
        timeVisible: true,
        secondsVisible: false,
      },
      rightPriceScale: {
        borderColor: isDark ? '#1F2937' : '#E5E7EB',
      },
    });

    const candlestickSeries = chart.addSeries(CandlestickSeries, {
      upColor: '#10B981',
      downColor: '#EF4444',
      borderVisible: false,
      wickUpColor: '#10B981',
      wickDownColor: '#EF4444',
    });

    chartRef.current = chart;
    seriesRef.current = candlestickSeries;

    const handleResize = () => {
      if (chartContainerRef.current) {
        chart.applyOptions({ width: chartContainerRef.current.clientWidth });
      }
    };
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
      trendSeriesRef.current = [];
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [theme]); // recreate only if theme changes

  // ── Update OHLCV data without destroying zoom state ───────────────────────
  useEffect(() => {
    if (!seriesRef.current || !data || data.length === 0) return;

    const chart = chartRef.current;

    // Save current visible range so we can restore it after update
    const visibleRange = chart.timeScale().getVisibleRange();

    const formattedData = data
      .map(d => ({
        time: Math.floor(new Date(d.date).getTime() / 1000),
        open: d.open,
        high: d.high,
        low: d.low,
        close: d.close,
      }))
      .sort((a, b) => a.time - b.time);

    seriesRef.current.setData(formattedData);

    // Restore the zoom/pan if the user had one set
    if (visibleRange) {
      chart.timeScale().setVisibleRange(visibleRange);
    }
  }, [data]);

  // ── Update trend lines without destroying zoom state ─────────────────────
  useEffect(() => {
    if (!chartRef.current) return;

    // Remove old trend series
    trendSeriesRef.current.forEach(s => {
      try { chartRef.current.removeSeries(s); } catch (_) {}
    });
    trendSeriesRef.current = [];

    if (!trendLines || trendLines.length === 0) return;

    const colorMap = {
      'green':  '#10B981',
      'red':    '#EF4444',
      'blue':   '#3B82F6',
      'white':  '#FFFFFF',
      'yellow': '#EAB308',
    };

    trendLines.forEach(line => {
      const mappedColor = colorMap[line.color] || colorMap['blue'];

      const lineSeries = chartRef.current.addSeries(LineSeries, {
        color: mappedColor,
        lineWidth: 2,
        lineStyle: 1,
        title: line.label || '',
      });

      const t1 = Math.floor(new Date(line.start_date).getTime() / 1000);
      const t2 = Math.floor(new Date(line.end_date).getTime() / 1000);
      const p1 = { time: t1, value: line.start_price };
      const p2 = { time: t2, value: line.end_price };
      const lineData = t1 > t2 ? [p2, p1] : [p1, p2];

      lineSeries.setData(lineData);
      trendSeriesRef.current.push(lineSeries);
    });
  }, [trendLines]);

  return <div ref={chartContainerRef} className="w-full h-[400px]" />;
};

export default CandlestickChart;
