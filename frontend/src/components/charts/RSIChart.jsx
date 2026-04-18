import React, { useEffect, useRef } from 'react';
import { createChart, ColorType, LineSeries } from 'lightweight-charts';

const RSIChart = ({ data, theme = 'dark', length = 14 }) => {
  const chartContainerRef = useRef();

  useEffect(() => {
    if (!chartContainerRef.current || !data || data.length < length) return;

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
      timeScale: {
        borderColor: isDark ? '#1F2937' : '#E5E7EB',
        visible: false,
      },
      rightPriceScale: {
        borderColor: isDark ? '#1F2937' : '#E5E7EB',
      },
    });

    const rsiSeries = chart.addSeries(LineSeries, {
      color: '#A855F7',
      lineWidth: 2,
    });

    // Helper to calculate RSI
    const calculateRSI = (prices, period) => {
      let rsi = [];
      let gains = 0;
      let losses = 0;

      for (let i = 1; i < prices.length; i++) {
        const diff = prices[i].close - prices[i-1].close;
        const gain = diff > 0 ? diff : 0;
        const loss = diff < 0 ? -diff : 0;

        if (i <= period) {
          gains += gain;
          losses += loss;
          if (i === period) {
            let avgGain = gains / period;
            let avgLoss = losses / period;
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

    const formattedPrices = data.map(d => ({
      time: Math.floor(new Date(d.date).getTime() / 1000),
      close: d.close
    })).sort((a, b) => a.time - b.time);

    const rsiData = calculateRSI(formattedPrices, length);
    rsiSeries.setData(rsiData.map(d => ({ time: d.time, value: d.value })));

    // Reference lines
    rsiSeries.createPriceLine({
        price: 70,
        color: '#EF4444',
        lineWidth: 1,
        lineStyle: 2,
        axisLabelVisible: true,
        title: 'Overbought',
    });
    rsiSeries.createPriceLine({
        price: 30,
        color: '#10B981',
        lineWidth: 1,
        lineStyle: 2,
        axisLabelVisible: true,
        title: 'Oversold',
    });

    const handleResize = () => {
      chart.applyOptions({ width: chartContainerRef.current.clientWidth });
    };

    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
    };
  }, [data, theme, length]);

  return (
    <div className="relative">
      <div className="absolute top-2 left-2 z-10 text-[10px] font-bold text-dark-muted uppercase">RSI (14)</div>
      <div ref={chartContainerRef} className="w-full h-[100px]" />
    </div>
  );
};

export default RSIChart;
