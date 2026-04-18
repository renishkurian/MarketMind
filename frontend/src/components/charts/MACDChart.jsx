import React, { useEffect, useRef } from 'react';
import { createChart, ColorType } from 'lightweight-charts';

const MACDChart = ({ data, theme = 'dark' }) => {
  const chartContainerRef = useRef();

  useEffect(() => {
    if (!chartContainerRef.current || !data || data.length < 26) return;

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
      timeScale: {
        borderColor: isDark ? '#1F2937' : '#E5E7EB',
      },
      rightPriceScale: {
        borderColor: isDark ? '#1F2937' : '#E5E7EB',
      },
    });

    const macdSeries = chart.addLineSeries({ color: '#2563EB', lineWidth: 1, title: 'MACD' });
    const signalSeries = chart.addLineSeries({ color: '#F59E0B', lineWidth: 1, title: 'Signal' });
    const histSeries = chart.addHistogramSeries({ title: 'Histogram' });

    // Simple EMA calculation
    const calculateEMA = (data, period) => {
      let ema = [];
      const k = 2 / (period + 1);
      let prevEma = data[0].value;
      
      for (let i = 0; i < data.length; i++) {
        const val = data[i].value;
        const currentEma = (val - prevEma) * k + prevEma;
        ema.push({ time: data[i].time, value: currentEma });
        prevEma = currentEma;
      }
      return ema;
    };

    const formattedData = data.map(d => ({
      time: Math.floor(new Date(d.date).getTime() / 1000),
      value: d.close
    })).sort((a, b) => a.time - b.time);

    const ema12 = calculateEMA(formattedData, 12);
    const ema26 = calculateEMA(formattedData, 26);
    
    const macdLine = ema12.map((e12, i) => {
        const e26 = ema26[i];
        return { time: e12.time, value: e12.value - e26.value };
    });

    const signalLine = calculateEMA(macdLine, 9);
    
    const histogram = macdLine.map((ml, i) => {
        const sl = signalLine.find(s => s.time === ml.time);
        if (!sl) return null;
        const val = ml.value - sl.value;
        return { 
            time: ml.time, 
            value: val,
            color: val >= 0 ? '#10B981' : '#EF4444'
        };
    }).filter(v => v !== null);

    macdSeries.setData(macdLine);
    signalSeries.setData(signalLine);
    histSeries.setData(histogram);

    const handleResize = () => {
      chart.applyOptions({ width: chartContainerRef.current.clientWidth });
    };

    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
    };
  }, [data, theme]);

  return (
    <div className="relative">
      <div className="absolute top-2 left-2 z-10 text-[10px] font-bold text-dark-muted uppercase">MACD (12, 26, 9)</div>
      <div ref={chartContainerRef} className="w-full h-[120px]" />
    </div>
  );
};

export default MACDChart;
