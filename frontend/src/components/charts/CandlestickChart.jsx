import React, { useEffect, useRef } from 'react';
import { createChart, ColorType, CandlestickSeries, LineSeries } from 'lightweight-charts';

const CandlestickChart = ({ data, theme = 'dark', trendLines = [] }) => {
  const chartContainerRef = useRef();

  useEffect(() => {
    if (!chartContainerRef.current || !data || data.length === 0) return;

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

    // Format data for lightweight-charts
    const formattedData = data.map(d => ({
      time: Math.floor(new Date(d.date).getTime() / 1000),
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
    })).sort((a, b) => a.time - b.time);

    candlestickSeries.setData(formattedData);

    // AI Trend Lines mapping
    if (trendLines && trendLines.length > 0) {
      trendLines.forEach(line => {
         const colorMap = {
           'green': '#10B981',
           'red': '#EF4444',
           'blue': '#3B82F6',
           'white': '#FFFFFF',
           'yellow': '#EAB308'
         };
         const mappedColor = colorMap[line.color] || colorMap['blue'];

         const lineSeries = chart.addSeries(LineSeries, {
            color: mappedColor,
            lineWidth: 2,
            lineStyle: 1, // Dotted
            title: line.label || '',
         });
         
         const t1 = Math.floor(new Date(line.start_date).getTime() / 1000);
         const t2 = Math.floor(new Date(line.end_date).getTime() / 1000);
         
         // In lightweight-charts, line data must be chronologically sorted.
         const p1 = { time: t1, value: line.start_price };
         const p2 = { time: t2, value: line.end_price };
         const lineData = t1 > t2 ? [p2, p1] : [p1, p2];

         lineSeries.setData(lineData);
      });
    }

    const handleResize = () => {
      chart.applyOptions({ width: chartContainerRef.current.clientWidth });
    };

    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
    };
  }, [data, theme, trendLines]);

  return <div ref={chartContainerRef} className="w-full h-[400px]" />;
};

export default CandlestickChart;
