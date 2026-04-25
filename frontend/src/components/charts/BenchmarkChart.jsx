import React, { useEffect, useRef } from 'react';
import { createChart, ColorType, LineSeries } from 'lightweight-charts';

const BenchmarkChart = ({ data, theme = 'dark' }) => {
  const chartContainerRef = useRef();
  const chartRef = useRef(null);
  const portfolioRef = useRef(null);
  const benchmarkRef = useRef(null);

  useEffect(() => {
    if (!chartContainerRef.current) return;

    const isDark = theme === 'dark';

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: isDark ? 'transparent' : '#ffffff' },
        textColor: isDark ? '#9CA3AF' : '#4B5563',
      },
      grid: {
        vertLines: { color: isDark ? '#1F2937' : '#E5E7EB' },
        horzLines: { color: isDark ? '#1F2937' : '#E5E7EB' },
      },
      width: chartContainerRef.current.clientWidth,
      height: 480,
      timeScale: {
        borderColor: isDark ? '#1F2937' : '#E5E7EB',
        timeVisible: true,
      },
      rightPriceScale: {
        borderColor: isDark ? '#1F2937' : '#E5E7EB',
      },
    });

    const portfolioSeries = chart.addSeries(LineSeries, {
      color: '#3B82F6', // Blue
      lineWidth: 3,
      title: 'Your Portfolio (%)',
    });

    const benchmarkSeries = chart.addSeries(LineSeries, {
      color: '#EAB308', // Yellow/Gold
      lineWidth: 2,
      lineStyle: 1, // Dotted/dashed
      title: 'Nifty 50 (%)',
    });

    chartRef.current = chart;
    portfolioRef.current = portfolioSeries;
    benchmarkRef.current = benchmarkSeries;

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
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [theme]);

  useEffect(() => {
    if (!portfolioRef.current || !benchmarkRef.current || !data) return;

    portfolioRef.current.setData(
      data.map(d => ({ 
        time: d.time, 
        value: d.portfolio 
      }))
    );
    benchmarkRef.current.setData(
      data.map(d => ({ 
        time: d.time, 
        value: d.benchmark 
      }))
    );

    chartRef.current.timeScale().fitContent();
  }, [data]);

  return <div ref={chartContainerRef} className="w-full h-[480px]" />;
};

export default BenchmarkChart;
