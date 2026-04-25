import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { useStockStore } from '../store/stockStore';
import { BarChart3, TrendingUp, TrendingDown, Clock, Activity, Target, Sparkles, PieChart } from 'lucide-react';
import BenchmarkChart from '../components/charts/BenchmarkChart';
import Loader from '../components/Loader';
import MetricCard from '../components/MetricCard';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, RadialBarChart, RadialBar, Legend } from 'recharts';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const SECTOR_COLORS = ['#3B82F6','#10B981','#F59E0B','#EF4444','#8B5CF6','#06B6D4','#F97316','#84CC16','#EC4899','#6366F1'];

const BenchmarkDashboard = () => {
  const [timeframe, setTimeframe] = useState('yearly');
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  // Year breakdown state
  const [yearData, setYearData] = useState(null);
  const [yearLoading, setYearLoading] = useState(true);
  const [selectedYear, setSelectedYear] = useState(null);
  
  // Sector state
  const [sectorData, setSectorData] = useState(null);
  const [sectorLoading, setSectorLoading] = useState(false);

  const { theme } = useStockStore();

  const getToken = () => localStorage.getItem('mm_token') || localStorage.getItem('token');

  const fetchPerformance = async () => {
    setLoading(true); setError(null);
    try {
      const res = await axios.get(`${API_URL}/api/portfolio-performance/benchmark-comparison`, {
        params: { timeframe }, headers: { 'Authorization': `Bearer ${getToken()}` }
      });
      res.data.error ? setError(res.data.error) : setData(res.data);
    } catch (err) {
      setError(err.response?.data?.detail || "Failed to fetch performance data");
    } finally { setLoading(false); }
  };

  const fetchYearBreakdown = async () => {
    setYearLoading(true);
    try {
      const res = await axios.get(`${API_URL}/api/portfolio-performance/yearly-breakdown`, {
        headers: { 'Authorization': `Bearer ${getToken()}` }
      });
      if (!res.data.error) {
        setYearData(res.data);
        // Default selected year to most recent
        const years = res.data.available_years;
        if (years?.length) setSelectedYear(years[years.length - 1]);
      }
    } catch (e) { console.error(e); }
    finally { setYearLoading(false); }
  };

  const fetchSectorPerformance = async (year) => {
    setSectorLoading(true);
    try {
      const res = await axios.get(`${API_URL}/api/portfolio-performance/sector-performance`, {
        params: year ? { year } : {},
        headers: { 'Authorization': `Bearer ${getToken()}` }
      });
      if (!res.data.error) setSectorData(res.data);
    } catch (e) { console.error(e); }
    finally { setSectorLoading(false); }
  };

  useEffect(() => { fetchPerformance(); }, [timeframe]);
  useEffect(() => { fetchYearBreakdown(); fetchSectorPerformance(null); }, []);
  useEffect(() => { if (selectedYear) fetchSectorPerformance(selectedYear); }, [selectedYear]);

  const selectedYearStats = yearData?.years?.find(y => y.year === selectedYear);

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-8 animate-in fade-in duration-700">
      {/* Header */}
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
          {[{ id: 'weekly', label: '1W' }, { id: 'monthly', label: '1M' }, { id: '3month', label: '3M' }, { id: 'yearly', label: '1Y' }].map((tf) => (
            <button key={tf.id} onClick={() => setTimeframe(tf.id)}
              className={`px-6 py-2.5 rounded-xl text-xs font-black uppercase tracking-widest transition-all duration-300 ${timeframe === tf.id ? 'bg-accent text-white shadow-lg shadow-accent/20' : 'text-dark-muted hover:text-white hover:bg-white/5'}`}>
              {tf.label}
            </button>
          ))}
        </div>
      </div>

      {/* Equity Curve Section */}
      {loading ? (
        <div className="flex flex-col items-center justify-center py-32 gap-6 bg-dark-card/30 rounded-3xl border border-dark-border/50">
          <Loader size="lg" />
          <p className="text-dark-muted font-mono animate-pulse">Calculating Portfolio Equity Curve...</p>
        </div>
      ) : error ? (
        <div className="flex flex-col items-center justify-center py-32 gap-4 bg-signal-sell/5 border border-signal-sell/20 rounded-3xl">
          <Activity size={48} className="text-signal-sell/40" />
          <p className="text-lg font-bold text-dark-text">{error}</p>
          <button onClick={fetchPerformance} className="mt-4 px-6 py-2 bg-dark-card border border-dark-border rounded-xl text-xs font-bold hover:bg-dark-border transition-all">Retry Sync</button>
        </div>
      ) : (
        <div className="space-y-8 animate-in slide-in-from-bottom-6 duration-700">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <MetricCard label="Portfolio Return" value={`${data.metrics.portfolio_return > 0 ? '+' : ''}${data.metrics.portfolio_return}%`} color={data.metrics.portfolio_return >= 0 ? "text-signal-buy" : "text-signal-sell"} icon={TrendingUp} sub={`Since ${data.metrics.start_date}`} />
            <MetricCard label="Nifty 50 Return" value={`${data.metrics.benchmark_return > 0 ? '+' : ''}${data.metrics.benchmark_return}%`} color={data.metrics.benchmark_return >= 0 ? "text-signal-buy" : "text-signal-sell"} icon={Target} sub="Market Benchmark Index" />
            <MetricCard label="Alpha (Excess Return)" value={`${data.metrics.alpha > 0 ? '+' : ''}${data.metrics.alpha}%`} color={data.metrics.alpha >= 0 ? "text-accent" : "text-signal-sell"} icon={Sparkles} sub={data.metrics.alpha >= 0 ? "Outperforming the market" : "Underperforming the market"} />
          </div>
          <div className="bg-dark-card border border-dark-border rounded-[2.5rem] p-8 shadow-2xl overflow-hidden relative group">
            <div className="absolute top-0 right-0 p-8 opacity-10 group-hover:opacity-20 transition-opacity"><Activity size={120} className="text-accent" /></div>
            <div className="relative z-10 space-y-6">
              <div className="flex items-center justify-between">
                <h3 className="text-xl font-black text-white italic tracking-tight uppercase flex items-center gap-2"><Clock size={20} className="text-accent" /> Equity Curve Comparison</h3>
                <div className="flex items-center gap-4 text-[10px] font-black uppercase tracking-widest bg-black/20 px-4 py-2 rounded-full border border-dark-border">
                  <div className="flex items-center gap-1.5"><div className="w-3 h-3 bg-accent rounded-full" /> Portfolio</div>
                  <div className="flex items-center gap-1.5"><div className="w-3 h-3 bg-yellow-500 rounded-full" /> Nifty 50</div>
                </div>
              </div>
              <BenchmarkChart data={data.chart_data} theme={theme} />
            </div>
          </div>
        </div>
      )}

      {/* ── Year-by-Year Breakdown ── */}
      <div className="bg-dark-card border border-dark-border rounded-[2.5rem] p-8 shadow-2xl space-y-6">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
          <h3 className="text-xl font-black text-white italic tracking-tight uppercase flex items-center gap-2">
            <BarChart3 size={20} className="text-accent" /> Year-by-Year vs Nifty 50
          </h3>
          {yearData?.available_years?.length > 0 && (
            <select
              value={selectedYear || ''}
              onChange={e => setSelectedYear(Number(e.target.value))}
              className="bg-dark-bg border border-dark-border rounded-xl px-4 py-2 text-sm font-bold text-white focus:outline-none focus:border-accent"
            >
              {yearData.available_years.map(yr => (
                <option key={yr} value={yr}>{yr}</option>
              ))}
            </select>
          )}
        </div>

        {yearLoading ? (
          <div className="flex justify-center py-12"><Loader size="md" /></div>
        ) : yearData?.years?.length ? (
          <>
            {/* Bar chart — all years */}
            <div className="w-full h-[280px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={yearData.years} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
                  <XAxis dataKey="year" tick={{ fill: '#6B7280', fontSize: 11 }} />
                  <YAxis tick={{ fill: '#6B7280', fontSize: 11 }} unit="%" />
                  <Tooltip
                    contentStyle={{ background: '#1F2937', border: '1px solid #374151', borderRadius: 12 }}
                    labelStyle={{ color: '#F9FAFB', fontWeight: 700 }}
                    formatter={(val, name) => [`${val > 0 ? '+' : ''}${val}%`, name === 'portfolio_return' ? 'Portfolio' : 'Nifty 50']}
                  />
                  <Bar dataKey="portfolio_return" name="Portfolio" fill="#3B82F6" radius={[4,4,0,0]} />
                  <Bar dataKey="nifty_return" name="Nifty 50" fill="#EAB308" radius={[4,4,0,0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>

            {/* Selected year stat cards */}
            {selectedYearStats && (
              <div className="grid grid-cols-3 gap-4 pt-2">
                {[
                  { label: `${selectedYear} Portfolio`, val: selectedYearStats.portfolio_return, color: selectedYearStats.portfolio_return >= 0 ? 'text-signal-buy' : 'text-signal-sell' },
                  { label: `${selectedYear} Nifty 50`, val: selectedYearStats.nifty_return, color: selectedYearStats.nifty_return >= 0 ? 'text-yellow-400' : 'text-signal-sell' },
                  { label: `${selectedYear} Alpha`, val: selectedYearStats.alpha, color: selectedYearStats.alpha >= 0 ? 'text-accent' : 'text-signal-sell' },
                ].map(c => (
                  <div key={c.label} className="bg-dark-bg border border-dark-border rounded-2xl p-4 text-center">
                    <p className="text-xs text-dark-muted font-semibold uppercase tracking-wide">{c.label}</p>
                    <p className={`text-2xl font-black mt-1 ${c.color}`}>{c.val > 0 ? '+' : ''}{c.val}%</p>
                  </div>
                ))}
              </div>
            )}
          </>
        ) : (
          <p className="text-dark-muted text-center py-8">Insufficient historical data for yearly breakdown.</p>
        )}
      </div>

      {/* ── Sector Performance ── */}
      <div className="bg-dark-card border border-dark-border rounded-[2.5rem] p-8 shadow-2xl space-y-6">
        <h3 className="text-xl font-black text-white italic tracking-tight uppercase flex items-center gap-2">
          <PieChart size={20} className="text-accent" /> Sector Performance
          {selectedYear && <span className="text-sm text-dark-muted font-normal normal-case ml-2">— {selectedYear}</span>}
        </h3>

        {sectorLoading ? (
          <div className="flex justify-center py-12"><Loader size="md" /></div>
        ) : sectorData?.sector_breakdown?.length ? (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
            {/* Allocation bar */}
            <div>
              <p className="text-xs text-dark-muted font-bold uppercase tracking-widest mb-4">Allocation %</p>
              <div className="space-y-3">
                {sectorData.sector_breakdown.map((s, i) => (
                  <div key={s.sector}>
                    <div className="flex justify-between text-xs mb-1">
                      <span className="text-dark-text font-semibold">{s.sector}</span>
                      <span className="text-dark-muted">{s.allocation_pct}%</span>
                    </div>
                    <div className="h-2 bg-dark-bg rounded-full overflow-hidden">
                      <div className="h-full rounded-full transition-all duration-700" style={{ width: `${s.allocation_pct}%`, background: SECTOR_COLORS[i % SECTOR_COLORS.length] }} />
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Return per sector */}
            <div className="h-[300px]">
              <p className="text-xs text-dark-muted font-bold uppercase tracking-widest mb-4">Return %</p>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={sectorData.sector_breakdown} layout="vertical" margin={{ left: 10, right: 30 }}>
                  <XAxis type="number" tick={{ fill: '#6B7280', fontSize: 10 }} unit="%" />
                  <YAxis type="category" dataKey="sector" width={110} tick={{ fill: '#9CA3AF', fontSize: 10 }} />
                  <Tooltip
                    contentStyle={{ background: '#1F2937', border: '1px solid #374151', borderRadius: 10 }}
                    formatter={(v) => [`${v > 0 ? '+' : ''}${v}%`, 'Return']}
                  />
                  <Bar dataKey="return_pct" radius={[0,4,4,0]}>
                    {sectorData.sector_breakdown.map((s, i) => (
                      <Cell key={s.sector} fill={s.return_pct >= 0 ? SECTOR_COLORS[i % SECTOR_COLORS.length] : '#EF4444'} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        ) : (
          <p className="text-dark-muted text-center py-8">No sector data available. Ensure stocks have sectors assigned.</p>
        )}
      </div>
    </div>
  );
};

export default BenchmarkDashboard;
