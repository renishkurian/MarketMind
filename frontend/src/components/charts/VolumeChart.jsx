import React, { useEffect, useRef } from 'react';
import { createChart, ColorType, HistogramSeries } from 'lightweight-charts';

const VolumeChart = ({ data, theme = 'dark' }) => {
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

    chartRef.current = chart;
    seriesRef.current = chart.addSeries(HistogramSeries, {
      color: '#3B82F6',
      priceFormat: { type: 'volume' },
    });

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
    if (!seriesRef.current || !data || data.length === 0) return;

    const visibleRange = chartRef.current.timeScale().getVisibleRange();

    const formattedData = data.map((d, i) => {
      const prevClose = data[i - 1]?.close ?? d.close;
      return {
        time: Math.floor(new Date(d.date || d.time).getTime() / 1000),
        value: d.volume,
        color: d.close >= prevClose ? '#10B981' : '#EF4444',
      };
    }).sort((a, b) => a.time - b.time);

    seriesRef.current.setData(formattedData);

    if (visibleRange) chartRef.current.timeScale().setVisibleRange(visibleRange);
  }, [data]);

  return <div ref={chartContainerRef} className="w-full h-[100px]" />;
};

export default VolumeChart;
