import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { useStockStore } from '../store/stockStore';
import { BarChart3, TrendingUp, TrendingDown, Clock, Activity, Target, Sparkles } from 'lucide-react';
import BenchmarkChart from '../components/charts/BenchmarkChart';
import Loader from '../components/Loader';
import MetricCard from '../components/MetricCard';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const BenchmarkDashboard = () => {
  const [timeframe, setTimeframe] = useState('yearly');
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const { theme } = useStockStore();

  const fetchPerformance = async () => {
    setLoading(true);
    setError(null);
    try {
      const token = localStorage.getItem('mm_token') || localStorage.getItem('token');
      const res = await axios.get(`${API_URL}/api/portfolio-performance/benchmark-comparison`, {
        params: { timeframe },
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (res.data.error) {
        setError(res.data.error);
      } else {
        setData(res.data);
      }
    } catch (err) {
      setError(err.response?.data?.detail || "Failed to fetch performance data");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPerformance();
  }, [timeframe]);

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-8 animate-in fade-in duration-700">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-6">
        <div>
          <h1 className="text-3xl font-black text-dark-text tracking-tight flex items-center gap-3 italic">
            <BarChart3 className="text-accent" size={32} />
            Institutional Performance Dashboard
          </h1>
          <p className="text-dark-muted mt-2 text-sm font-medium">
            Benchmarking your portfolio against the <span className="text-accent">Nifty 50</span> index.
          </p>
        </div>

        <div className="flex items-center bg-dark-card border border-dark-border p-1 rounded-2xl shadow-xl shadow-black/20">
          {[
            { id: 'weekly', label: '1W' },
            { id: 'monthly', label: '1M' },
            { id: '3month', label: '3M' },
            { id: 'yearly', label: '1Y' },
          ].map((tf) => (
            <button
              key={tf.id}
              onClick={() => setTimeframe(tf.id)}
              className={`px-6 py-2.5 rounded-xl text-xs font-black uppercase tracking-widest transition-all duration-300 ${
                timeframe === tf.id
                  ? 'bg-accent text-white shadow-lg shadow-accent/20'
                  : 'text-dark-muted hover:text-white hover:bg-white/5'
              }`}
            >
              {tf.label}
            </button>
          ))}
        </div>
      </div>

      {loading ? (
        <div className="flex flex-col items-center justify-center py-32 gap-6 bg-dark-card/30 rounded-3xl border border-dark-border/50">
          <Loader size="lg" />
          <p className="text-dark-muted font-mono animate-pulse">Calculating Portfolio Equity Curve...</p>
        </div>
      ) : error ? (
        <div className="flex flex-col items-center justify-center py-32 gap-4 bg-signal-sell/5 border border-signal-sell/20 rounded-3xl">
          <Activity size={48} className="text-signal-sell/40" />
          <div className="text-center">
            <p className="text-lg font-bold text-dark-text">{error}</p>
            <p className="text-sm text-dark-muted mt-1">Please ensure your portfolio has stocks with historical data.</p>
          </div>
          <button 
            onClick={fetchPerformance}
            className="mt-4 px-6 py-2 bg-dark-card border border-dark-border rounded-xl text-xs font-bold hover:bg-dark-border transition-all"
          >
            Retry Sync
          </button>
        </div>
      ) : (
        <div className="space-y-8 animate-in slide-in-from-bottom-6 duration-700">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <MetricCard 
              label="Portfolio Return" 
              value={`${data.metrics.portfolio_return > 0 ? '+' : ''}${data.metrics.portfolio_return}%`}
              color={data.metrics.portfolio_return >= 0 ? "text-signal-buy" : "text-signal-sell"}
              icon={TrendingUp}
              sub={`Performance since ${data.metrics.start_date}`}
            />
            <MetricCard 
              label="Nifty 50 Return" 
              value={`${data.metrics.benchmark_return > 0 ? '+' : ''}${data.metrics.benchmark_return}%`}
              color={data.metrics.benchmark_return >= 0 ? "text-signal-buy" : "text-signal-sell"}
              icon={Target}
              sub="Market Benchmark Index"
            />
            <MetricCard 
              label="Alpha (Excess Return)" 
              value={`${data.metrics.alpha > 0 ? '+' : ''}${data.metrics.alpha}%`}
              color={data.metrics.alpha >= 0 ? "text-accent" : "text-signal-sell"}
              icon={Sparkles}
              sub={data.metrics.alpha >= 0 ? "Outperforming the market" : "Underperforming the market"}
            />
          </div>

          <div className="bg-dark-card border border-dark-border rounded-[2.5rem] p-8 shadow-2xl overflow-hidden relative group">
            <div className="absolute top-0 right-0 p-8 opacity-10 group-hover:opacity-20 transition-opacity">
               <Activity size={120} className="text-accent" />
            </div>
            <div className="relative z-10 space-y-6">
              <div className="flex items-center justify-between">
                <h3 className="text-xl font-black text-white italic tracking-tight uppercase flex items-center gap-2">
                   <Clock size={20} className="text-accent" /> Equity Curve Comparison
                </h3>
                <div className="flex items-center gap-4 text-[10px] font-black uppercase tracking-widest bg-black/20 px-4 py-2 rounded-full border border-dark-border">
                   <div className="flex items-center gap-1.5"><div className="w-3 h-3 bg-accent rounded-full" /> Portfolio</div>
                   <div className="flex items-center gap-1.5"><div className="w-3 h-3 bg-yellow-500 rounded-full" /> Nifty 50</div>
                </div>
              </div>
              <BenchmarkChart data={data.chart_data} theme={theme} />
            </div>
          </div>
          
          <div className="p-6 bg-accent/5 border border-accent/20 rounded-2xl flex items-start gap-4">
             <div className="p-2 bg-accent/10 rounded-lg shrink-0">
               <Target size={20} className="text-accent" />
             </div>
             <div className="space-y-1">
                <h4 className="text-sm font-bold text-white uppercase tracking-tight">Institutional Analysis</h4>
                <p className="text-xs text-dark-muted leading-relaxed">
                   Your alpha of <span className={data.metrics.alpha >= 0 ? 'text-signal-buy font-bold' : 'text-signal-sell font-bold'}>{data.metrics.alpha}%</span> is calculated relative to the Nifty 50 base. 
                   The chart normalizes both capital bases to 100 on <span className="text-dark-text font-bold">{data.metrics.start_date}</span> to provide a clean percentage-based comparison of compounding efficiency.
                </p>
             </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default BenchmarkDashboard;
