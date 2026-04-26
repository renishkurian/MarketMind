import React, { useEffect, useRef } from 'react';
import { createChart, ColorType, CandlestickSeries, LineSeries } from 'lightweight-charts';

const calculateSMA = (data, period) => {
  if (data.length < period) return [];
  const sma = [];
  for (let i = period - 1; i < data.length; i++) {
    const sum = data.slice(i - period + 1, i + 1).reduce((acc, d) => acc + d.close, 0);
    sma.push({ time: data[i].time, value: sum / period });
  }
  return sma;
};

const CandlestickChart = ({ data, theme = 'dark', trendLines = [], showSMAs = true }) => {
  const chartContainerRef = useRef();
  const chartRef = useRef(null);
  const seriesRef = useRef(null);
  const trendSeriesRef = useRef([]);
  const smaSeriesRef = useRef({ sma20: null, sma50: null, sma200: null });

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
      smaSeriesRef.current = { sma20: null, sma50: null, sma200: null };
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [theme]); // recreate only if theme changes

  // ── Update OHLCV data without destroying zoom state ───────────────────────
  useEffect(() => {
    if (!seriesRef.current || !data || data.length === 0) return;

    const chart = chartRef.current;

    // Save current visible range so we can restore it after update
    const visibleRange = chart.timeScale().getVisibleRange();

    const formattedData = [];
    const seenTimes = new Set();
    
    // Sort raw data first to ensure we pick the LATEST entry if there are duplicates
    const sortedRaw = [...data].sort((a, b) => new Date(a.date) - new Date(b.date));

    sortedRaw.forEach(d => {
      const t = Math.floor(new Date(d.date).getTime() / 1000);
      if (t && !seenTimes.has(t)) {
        formattedData.push({
          time: t,
          open: d.open,
          high: d.high,
          low: d.low,
          close: d.close,
        });
        seenTimes.add(t);
      }
    });

    seriesRef.current.setData(formattedData);

    // ── SMA Overlays ──────────────────────────────────────────────────────
    // Remove if exists
    ['sma20', 'sma50', 'sma200'].forEach(key => {
      if (smaSeriesRef.current[key]) {
        try { chart.removeSeries(smaSeriesRef.current[key]); } catch (_) {}
        smaSeriesRef.current[key] = null;
      }
    });

    if (showSMAs && formattedData.length >= 20) {
      const config = [
        { key: 'sma20',  period: 20,  color: '#3B82F6', title: 'SMA 20' },
        { key: 'sma50',  period: 50,  color: '#F59E0B', title: 'SMA 50' },
        { key: 'sma200', period: 200, color: '#A855F7', title: 'SMA 200' },
      ];

      config.forEach(c => {
        if (formattedData.length >= c.period) {
          const smaData = calculateSMA(formattedData, c.period);
          const series = chart.addSeries(LineSeries, {
            color: c.color,
            lineWidth: 1,
            priceLineVisible: false,
            lastValueVisible: false,
            crosshairMarkerVisible: false,
            title: c.title,
          });
          series.setData(smaData);
          smaSeriesRef.current[c.key] = series;
        }
      });
    }

    // Restore the zoom/pan if the user had one set, 
    // BUT only if the data length is similar (incremental update).
    // If it's a massive shift (range change), let the chart fit the new data.
    const isRangeSwitch = !seriesRef.current.previousDataLength || Math.abs(seriesRef.current.previousDataLength - data.length) > 10;
    
    if (!isRangeSwitch && visibleRange && visibleRange.from && visibleRange.to) {
      try {
        chart.timeScale().setVisibleRange(visibleRange);
      } catch (e) {
        chart.timeScale().fitContent();
      }
    } else {
      chart.timeScale().fitContent();
    }
    
    seriesRef.current.previousDataLength = data.length;
  }, [data, showSMAs]);

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
      
      // Safety: t1 and t2 must be distinct and valid for lightweight-charts
      if (!t1 || !t2 || t1 === t2) return;

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
