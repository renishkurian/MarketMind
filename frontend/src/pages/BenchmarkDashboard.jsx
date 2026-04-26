import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { useStockStore } from '../store/stockStore';
import { BarChart3, TrendingUp, TrendingDown, Clock, Activity, Target, Sparkles, Brain, X, AlertTriangle, PieChart as PieChartIcon } from 'lucide-react';
import BenchmarkChart from '../components/charts/BenchmarkChart';
import Loader from '../components/Loader';
import MetricCard from '../components/MetricCard';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, RadialBarChart, RadialBar, Legend, PieChart, Pie, AreaChart, Area } from 'recharts';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const SECTOR_COLORS = ['#3B82F6','#10B981','#F59E0B','#EF4444','#8B5CF6','#06B6D4','#F97316','#84CC16','#EC4899','#6366F1'];

const BENCHMARKS = [
  { id: '^NSEI', name: 'Nifty 50' },
  { id: '^NSEBANK', name: 'Nifty Bank' },
  { id: '^CNXIT', name: 'Nifty IT' },
  { id: '^CNX100', name: 'Nifty 100' },
];

const BenchmarkDashboard = () => {
  const [timeframe, setTimeframe] = useState('yearly');
  const [selectedBenchmark, setSelectedBenchmark] = useState('^NSEI');
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  const [yearData, setYearData] = useState(null);
  const [yearLoading, setYearLoading] = useState(true);
  const [selectedYear, setSelectedYear] = useState(null);
  const [sectorData, setSectorData] = useState(null);
  const [sectorLoading, setSectorLoading] = useState(false);

  const [yearlyExplainer, setYearlyExplainer]     = useState(null);  // holds full explainer object
  const [explainerLoading, setExplainerLoading]   = useState(null);  // holds year number being loaded
  const [explainerModal, setExplainerModal]       = useState(false);

  const { theme } = useStockStore();

  const getToken = () => localStorage.getItem('mm_token') || localStorage.getItem('token');

  const fetchPerformance = async (force = false) => {
    setLoading(true); setError(null);
    try {
      const res = await axios.get(`${API_URL}/api/portfolio-performance/benchmark-comparison`, {
        params: { timeframe, benchmark: selectedBenchmark, refresh: force }, 
        headers: { 'Authorization': `Bearer ${getToken()}` }
      });
      res.data.error ? setError(res.data.error) : setData(res.data);
    } catch (err) {
      setError(err.response?.data?.detail || "Failed to fetch performance data");
    } finally { setLoading(false); }
  };
  
  const fetchYearBreakdown = async (force = false) => {
    setYearLoading(true);
    try {
      const res = await axios.get(`${API_URL}/api/portfolio-performance/yearly-breakdown`, {
        params: { refresh: force },
        headers: { 'Authorization': `Bearer ${getToken()}` }
      });
      if (!res.data.error) {
        setYearData(res.data);
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
  const fetchYearlyExplainer = async (year, portfolioReturn, niftyReturn, alpha) => {
    if (explainerLoading === year) return;
    setExplainerLoading(year);
    try {
      const token = localStorage.getItem('mm_token');
      const params = new URLSearchParams({
        year,
        portfolio_return: portfolioReturn,
        nifty_return:     niftyReturn,
        alpha,
      });
      const res = await fetch(
        `${API_URL}/api/portfolio-performance/yearly-explainer?${params}`,
        { headers: { 'Authorization': `Bearer ${token}` } }
      );
      if (res.ok) {
        const data = await res.json();
        setYearlyExplainer(data);
        setExplainerModal(true);
      } else {
        toast.error('Failed to load year analysis');
      }
    } catch (e) {
      toast.error('Connection error');
    } finally {
      setExplainerLoading(null);
    }
  };

  const handleRegenerate = () => {
    fetchPerformance(true);
    fetchYearBreakdown(true);
    if (selectedYear) fetchSectorPerformance(selectedYear);
  };

  useEffect(() => { fetchPerformance(); }, [timeframe, selectedBenchmark]);
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
          <div className="flex items-center gap-4 mt-2">
            <p className="text-dark-muted text-sm font-medium">
              Benchmarking against
            </p>
            <select 
              value={selectedBenchmark}
              onChange={(e) => setSelectedBenchmark(e.target.value)}
              className="bg-dark-card border border-dark-border rounded-lg px-3 py-1 text-xs font-black text-accent uppercase tracking-widest focus:outline-none focus:border-accent/50 cursor-pointer hover:bg-white/5 transition-colors"
            >
              {BENCHMARKS.map(b => (
                <option key={b.id} value={b.id} className="bg-dark-card text-dark-text">{b.name}</option>
              ))}
            </select>
          </div>
        </div>
        
        <div className="flex items-center gap-4">
          <button
            onClick={handleRegenerate}
            disabled={loading || yearLoading}
            className="flex items-center gap-2 bg-dark-card border border-dark-border hover:border-accent/50 text-dark-muted hover:text-accent px-4 py-2 rounded-xl text-xs font-black uppercase tracking-widest transition-all active:scale-95 disabled:opacity-50"
          >
            <Sparkles size={14} className={loading || yearLoading ? 'animate-spin' : ''} />
            Regenerate
          </button>

          <div className="flex items-center bg-dark-card border border-dark-border p-1 rounded-2xl shadow-xl shadow-black/20">
            {[{ id: 'weekly', label: '1W' }, { id: 'monthly', label: '1M' }, { id: '3month', label: '3M' }, { id: 'yearly', label: '1Y' }].map((tf) => (
              <button key={tf.id} onClick={() => setTimeframe(tf.id)}
                className={`px-6 py-2.5 rounded-xl text-xs font-black uppercase tracking-widest transition-all duration-300 ${timeframe === tf.id ? 'bg-accent text-white shadow-lg shadow-accent/20' : 'text-dark-muted hover:text-white hover:bg-white/5'}`}>
                {tf.label}
              </button>
            ))}
          </div>
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
          <button onClick={() => fetchPerformance()} className="mt-4 px-6 py-2 bg-dark-card border border-dark-border rounded-xl text-xs font-bold hover:bg-dark-border transition-all">Retry Sync</button>
        </div>
      ) : (
        <div className="space-y-8 animate-in slide-in-from-bottom-6 duration-700">
          {/* Top Summary Cards */}
          {data?.metrics && (
            <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
              <MetricCard
                label="Total Alpha"
                value={`${data.metrics.alpha > 0 ? '+' : ''}${data.metrics.alpha}%`}
                icon={Target}
                color="text-[#A855F7]"
                sub={`vs Benchmark (${timeframe})`}
              />
              <MetricCard
                label="Portfolio Return"
                value={`${data.metrics.portfolio_return > 0 ? '+' : ''}${data.metrics.portfolio_return}%`}
                icon={TrendingUp}
                color={data.metrics.portfolio_return >= 0 ? 'text-signal-buy' : 'text-signal-sell'}
                sub={`Inception: ${data.metrics.start_date}`}
              />
              <MetricCard
                label="Benchmark Return"
                value={`${data.metrics.benchmark_return > 0 ? '+' : ''}${data.metrics.benchmark_return}%`}
                icon={Activity}
                color={data.metrics.benchmark_return >= 0 ? 'text-signal-buy' : 'text-signal-sell'}
                sub={BENCHMARKS.find(b => b.id === selectedBenchmark)?.name || "Index"}
              />
              <MetricCard
                label="Window Range"
                value={timeframe.toUpperCase()}
                icon={Clock}
                sub={`${data.metrics.start_date} → ${data.metrics.end_date}`}
              />
            </div>
          )}
          {/* Visual Pulse Cards */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Sector Pie Card */}
            <div className="bg-dark-card border border-dark-border rounded-3xl p-6 shadow-xl flex items-center">
              <div className="w-1/2 h-[180px]">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={sectorData?.sector_breakdown || []}
                      dataKey="allocation_pct"
                      nameKey="sector"
                      cx="50%"
                      cy="50%"
                      innerRadius={45}
                      outerRadius={70}
                      paddingAngle={5}
                    >
                      {sectorData?.sector_breakdown?.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={SECTOR_COLORS[index % SECTOR_COLORS.length]} stroke="none" />
                      ))}
                    </Pie>
                    <Tooltip 
                      contentStyle={{ background: '#111827', border: '1px solid #374151', borderRadius: 10, fontSize: 10 }}
                    />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div className="w-1/2 space-y-2">
                <h4 className="text-[10px] font-black text-dark-muted uppercase tracking-widest flex items-center gap-2">
                  <PieChartIcon size={12} className="text-accent" /> Sector Allocation
                </h4>
                <div className="grid grid-cols-1 gap-1">
                  {sectorData?.sector_breakdown?.slice(0, 4).map((s, i) => (
                    <div key={s.sector} className="flex items-center justify-between group">
                      <div className="flex items-center gap-2">
                        <div className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: SECTOR_COLORS[i % SECTOR_COLORS.length] }} />
                        <span className="text-[11px] font-bold text-dark-text truncate w-24">{s.sector}</span>
                      </div>
                      <span className="text-[11px] font-mono text-dark-muted">{s.allocation_pct}%</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            {/* Performance Consistency Card */}
            <div className="bg-dark-card border border-dark-border rounded-3xl p-6 shadow-xl flex flex-col justify-between">
              <h4 className="text-[10px] font-black text-dark-muted uppercase tracking-widest flex items-center gap-2 mb-4">
                <Activity size={12} className="text-accent" /> Yearly Accuracy
              </h4>
              <div className="h-[130px] w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={yearData?.years?.slice(-6) || []} margin={{ top: 10, right: 10, left: -25, bottom: 5 }}>
                    <XAxis 
                      dataKey="year" 
                      axisLine={false} 
                      tickLine={false} 
                      tick={{ fill: '#9CA3AF', fontSize: 9, fontWeight: 700 }}
                      dy={5}
                    />
                    <YAxis 
                      hide 
                      domain={['auto', 'auto']} 
                    />
                    <Tooltip 
                      contentStyle={{ background: '#111827', border: '1px solid #374151', borderRadius: 10, fontSize: 10 }}
                      labelStyle={{ color: '#9CA3AF', fontWeight: 800, marginBottom: 2 }}
                      itemStyle={{ color: '#F9FAFB', fontSize: 11, fontWeight: 600 }}
                      formatter={(val) => [`${val}%`, 'Alpha']}
                    />
                    <Bar dataKey="alpha">
                      {yearData?.years?.slice(-6).map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.alpha >= 0 ? '#10B981' : '#EF4444'} radius={[3, 3, 0, 0]} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
              <p className="text-[10px] text-dark-muted mt-2 italic font-medium">Excess returns (Alpha) over the last {yearData?.years?.slice(-6).length} active years.</p>
            </div>
          </div>

          <div className="bg-dark-card border border-dark-border rounded-[2.5rem] p-8 shadow-2xl overflow-hidden relative group">
            <div className="absolute top-0 right-0 p-8 opacity-10 group-hover:opacity-20 transition-opacity"><Activity size={120} className="text-accent" /></div>
            <div className="relative z-10 space-y-6">
              <div className="flex items-center justify-between">
                <h3 className="text-xl font-black text-white italic tracking-tight uppercase flex items-center gap-2"><Clock size={20} className="text-accent" /> Equity Curve Comparison</h3>
                <div className="flex items-center gap-4 text-[10px] font-black uppercase tracking-widest bg-black/20 px-4 py-2 rounded-full border border-dark-border">
                  <div className="flex items-center gap-1.5"><div className="w-3 h-3 bg-accent rounded-full" /> Portfolio</div>
                  <div className="flex items-center gap-1.5"><div className="w-3 h-3 bg-yellow-500 rounded-full" /> Benchmark</div>
                </div>
              </div>

              <div className="relative">
                <div className="absolute -left-12 top-1/2 -rotate-90 origin-center text-[10px] font-black text-dark-muted uppercase tracking-[0.2em] whitespace-nowrap">
                  Cumulative Performance (%)
                </div>
                {data?.chart_data && <BenchmarkChart data={data.chart_data} theme={theme} />}
                <div className="text-center text-[10px] font-black text-dark-muted uppercase tracking-[0.2em] mt-4">
                  Trading Timeline (Date)
                </div>
              </div>
            </div>
          </div>

          {/* Yearly Performance Trend Chart */}
          <div className="bg-dark-card border border-dark-border rounded-[2.5rem] p-8 shadow-2xl relative overflow-hidden group">
            <div className="absolute top-0 right-0 p-8 opacity-10 group-hover:opacity-20 transition-opacity"><BarChart3 size={120} className="text-accent" /></div>
            <div className="relative z-10 space-y-6">
              <div className="flex items-center justify-between">
                <h3 className="text-xl font-black text-white italic tracking-tight uppercase flex items-center gap-2">
                  <Activity size={20} className="text-accent" /> Annual Performance Velocity
                </h3>
              </div>
              <div className="w-full h-[300px]">
                {yearLoading ? (
                  <div className="flex justify-center items-center h-full"><Loader size="md" /></div>
                ) : (
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={yearData?.years || []} margin={{ top: 20, right: 30, left: 0, bottom: 0 }}>
                      <defs>
                        <linearGradient id="colorPortfolio" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#3B82F6" stopOpacity={0.3}/>
                          <stop offset="95%" stopColor="#3B82F6" stopOpacity={0}/>
                        </linearGradient>
                        <linearGradient id="colorBenchmark" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#9CA3AF" stopOpacity={0.1}/>
                          <stop offset="95%" stopColor="#9CA3AF" stopOpacity={0}/>
                        </linearGradient>
                      </defs>
                      <XAxis dataKey="year" axisLine={false} tickLine={false} tick={{ fill: '#9CA3AF', fontSize: 10, fontWeight: 700 }} />
                      <YAxis axisLine={false} tickLine={false} tick={{ fill: '#9CA3AF', fontSize: 10, fontWeight: 700 }} unit="%" />
                      <Tooltip
                        contentStyle={{ background: '#111827', border: '1px solid #374151', borderRadius: 12 }}
                        itemStyle={{ color: '#F9FAFB', fontSize: 12 }}
                      />
                      <Legend 
                        verticalAlign="top" 
                        align="right" 
                        iconType="circle"
                        wrapperStyle={{ paddingBottom: 20, fontSize: 10, fontWeight: 800, textTransform: 'uppercase', letterSpacing: '0.1em' }}
                      />
                      <Area 
                        type="monotone" 
                        dataKey="portfolio_return" 
                        name="Portfolio" 
                        stroke="#3B82F6" 
                        strokeWidth={4}
                        fillOpacity={1} 
                        fill="url(#colorPortfolio)" 
                      />
                      <Area 
                        type="monotone" 
                        dataKey="nifty_return" 
                        name="Benchmark" 
                        stroke="#9CA3AF" 
                        strokeWidth={2}
                        fillOpacity={1}
                        fill="url(#colorBenchmark)" 
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                )}
              </div>
              <p className="text-xs text-dark-muted font-medium italic">
                This chart displays discrete annual growth rates. Use this to track year-on-year volatility and recovery.
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Year-by-Year Breakdown Section */}
      <div className="bg-dark-card border border-dark-border rounded-[2.5rem] p-8 shadow-2xl space-y-6">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
          <h3 className="text-xl font-black text-white italic tracking-tight uppercase flex items-center gap-2">
            <BarChart3 size={20} className="text-accent" /> Year-by-Year Comparison
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
            <div className="w-full h-[280px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={yearData.years} margin={{ top: 10, right: 10, left: 20, bottom: 20 }}>
                  <XAxis dataKey="year" tick={{ fill: '#6B7280', fontSize: 11 }} dy={10} />
                  <YAxis tick={{ fill: '#6B7280', fontSize: 11 }} unit="%" />
                  <Tooltip
                    contentStyle={{ background: '#1F2937', border: '1px solid #374151', borderRadius: 12 }}
                    itemStyle={{ color: '#F9FAFB', fontSize: 12 }}
                    formatter={(val) => [`${val > 0 ? '+' : ''}${val}%`]}
                  />
                  <Legend 
                    verticalAlign="top" 
                    align="right" 
                    wrapperStyle={{ paddingBottom: 20, fontSize: 11, fontWeight: 700 }}
                  />
                  <Bar dataKey="portfolio_return" name="Portfolio" fill="#3B82F6" radius={[4,4,0,0]} />
                  <Bar dataKey="nifty_return" name="Benchmark" fill="#EAB308" radius={[4,4,0,0]} />
                  <Bar dataKey="alpha" name="Alpha" fill="#A855F7" radius={[4,4,0,0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>

            {selectedYearStats && (
              <>
                <div className="grid grid-cols-3 gap-4">
                  {[
                    { label: "Portfolio", val: selectedYearStats.portfolio_return, color: selectedYearStats.portfolio_return >= 0 ? 'text-signal-buy' : 'text-signal-sell' },
                    { label: "Benchmark", val: selectedYearStats.nifty_return, color: selectedYearStats.nifty_return >= 0 ? 'text-yellow-400' : 'text-signal-sell' },
                    { label: "Alpha", val: selectedYearStats.alpha, color: 'text-[#A855F7]' },
                  ].map(c => (
                    <div key={c.label} className="bg-dark-bg border border-dark-border rounded-2xl p-4 text-center">
                      <p className="text-xs text-dark-muted font-bold uppercase tracking-wider">{selectedYear} {c.label}</p>
                      <p className={`text-2xl font-black mt-1 ${c.color}`}>{c.val > 0 ? '+' : ''}{c.val}%</p>
                    </div>
                  ))}
                </div>
                
                {/* AI Explainer Trigger */}
                <button
                  onClick={() => fetchYearlyExplainer(selectedYear, selectedYearStats.portfolio_return, selectedYearStats.nifty_return, selectedYearStats.alpha)}
                  disabled={explainerLoading === selectedYear}
                  className="mt-6 w-full flex items-center justify-center gap-2 py-4 rounded-3xl border border-dark-border hover:border-accent hover:bg-accent/5 transition-all text-xs font-black text-dark-muted hover:text-accent disabled:opacity-50 overflow-hidden relative group/ai"
                >
                  <div className="absolute inset-0 bg-gradient-to-r from-accent/0 via-accent/5 to-accent/0 translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-1000" />
                  {explainerLoading === selectedYear
                    ? <><Brain size={14} className="animate-pulse text-accent" /> Analysing {selectedYear} Performance...</>
                    : <><Sparkles size={14} className="group-hover:scale-110 transition-transform" /> Why this year? Get AI Narrative Analysis</>
                  }
                </button>
              </>
            )}
          </>
        ) : (
          <p className="text-dark-muted text-center py-8">No yearly data found.</p>
        )}
      </div>

      {/* Sector Performance Section */}
      <div className="bg-dark-card border border-dark-border rounded-[2.5rem] p-8 shadow-2xl space-y-6">
        <h3 className="text-xl font-black text-white italic tracking-tight uppercase flex items-center gap-2">
          <PieChartIcon size={20} className="text-accent" /> Sector Analysis
          {selectedYear && <span className="text-sm text-dark-muted font-normal normal-case ml-2">— {selectedYear}</span>}
        </h3>

        {sectorLoading ? (
          <div className="flex justify-center py-12"><Loader size="md" /></div>
        ) : sectorData?.sector_breakdown?.length ? (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-12">
            <div>
              <p className="text-xs text-dark-muted font-bold uppercase tracking-widest mb-4">Weight Allocation</p>
              <div className="space-y-4">
                {sectorData.sector_breakdown.map((s, i) => (
                  <div key={s.sector}>
                    <div className="flex justify-between text-xs mb-1 font-bold italic">
                      <span className="text-dark-text">{s.sector}</span>
                      <span className="text-dark-muted">{s.allocation_pct}%</span>
                    </div>
                    <div className="h-1.5 bg-dark-bg rounded-full overflow-hidden">
                      <div className="h-full rounded-full" style={{ width: `${s.allocation_pct}%`, background: SECTOR_COLORS[i % SECTOR_COLORS.length] }} />
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div className="h-[300px]">
              <p className="text-xs text-dark-muted font-bold uppercase tracking-widest mb-4">Contribution to Growth</p>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={sectorData.sector_breakdown} layout="vertical" margin={{ left: 20, right: 30 }}>
                  <XAxis type="number" hide />
                  <YAxis type="category" dataKey="sector" width={100} tick={{ fill: '#9CA3AF', fontSize: 10, fontWeight: 700 }} axisLine={false} tickLine={false} />
                  <Tooltip
                    contentStyle={{ background: '#1F2937', border: '1px solid #374151', borderRadius: 12 }}
                    itemStyle={{ color: '#F9FAFB', fontSize: 11 }}
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
          <p className="text-dark-muted text-center py-8">Analysis pending...</p>
        )}
      </div>

      {/* Yearly Explainer Modal */}
      {explainerModal && yearlyExplainer && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4 animate-in fade-in duration-300"
          onClick={() => setExplainerModal(false)}
        >
          <div
            className="bg-dark-card border border-dark-border rounded-2xl w-full max-w-2xl shadow-2xl overflow-hidden animate-in slide-in-from-bottom-4"
            onClick={e => e.stopPropagation()}
          >
            {/* Modal Header */}
            <div className={`p-5 border-b border-dark-border flex items-start justify-between ${
              yearlyExplainer.sentiment === 'Strong' || yearlyExplainer.sentiment === 'Good'
                ? 'bg-signal-buy/5'
                : yearlyExplainer.sentiment === 'Tough' || yearlyExplainer.sentiment === 'Difficult'
                ? 'bg-signal-sell/5'
                : 'bg-signal-hold/5'
            }`}>
              <div>
                <div className="flex items-center gap-2 mb-1.5">
                  <span className={`text-xs font-black px-2.5 py-0.5 rounded-full ${
                    yearlyExplainer.sentiment === 'Strong' || yearlyExplainer.sentiment === 'Good'
                      ? 'bg-signal-buy/20 text-signal-buy'
                      : yearlyExplainer.sentiment === 'Tough' || yearlyExplainer.sentiment === 'Difficult'
                      ? 'bg-signal-sell/20 text-signal-sell'
                      : 'bg-signal-hold/20 text-signal-hold'
                  }`}>{yearlyExplainer.sentiment} Year</span>
                  <span className="text-xs text-dark-muted font-mono">{yearlyExplainer.year}</span>
                </div>
                <h2 className="text-lg font-black text-white leading-snug max-w-lg">
                  {yearlyExplainer.headline}
                </h2>
              </div>
              <button
                onClick={() => setExplainerModal(false)}
                className="text-dark-muted hover:text-white p-1 shrink-0 ml-4"
              >
                <X size={20}/>
              </button>
            </div>

            {/* Performance Strip */}
            <div className="grid grid-cols-3 divide-x divide-dark-border border-b border-dark-border">
              {[
                { label: 'Portfolio', value: yearlyExplainer.portfolio_return, colored: true },
                { label: 'Nifty 50',  value: yearlyExplainer.nifty_return,     colored: true },
                { label: 'Alpha',     value: yearlyExplainer.alpha,             colored: true },
              ].map(({ label, value, colored }) => (
                <div key={label} className="p-4 text-center">
                  <p className="text-[10px] text-dark-muted font-bold uppercase tracking-widest mb-1">{label}</p>
                  <p className={`text-2xl font-black font-mono ${
                    !colored ? 'text-white' :
                    value >= 0 ? 'text-signal-buy' : 'text-signal-sell'
                  }`}>
                    {value >= 0 ? '+' : ''}{value?.toFixed(2)}%
                  </p>
                </div>
              ))}
            </div>

            {/* Body */}
            <div className="p-5 space-y-4 max-h-[50vh] overflow-y-auto custom-scrollbar">

              {/* What Worked */}
              <div className="bg-signal-buy/5 border border-signal-buy/20 rounded-xl p-4">
                <div className="flex items-center gap-2 mb-2">
                  <TrendingUp size={14} className="text-signal-buy" />
                  <p className="text-xs font-black text-signal-buy uppercase tracking-widest">What Worked</p>
                </div>
                <p className="text-sm text-dark-text leading-relaxed">{yearlyExplainer.what_worked}</p>
              </div>

              {/* What Didn't */}
              <div className="bg-signal-sell/5 border border-signal-sell/20 rounded-xl p-4">
                <div className="flex items-center gap-2 mb-2">
                  <TrendingDown size={14} className="text-signal-sell" />
                  <p className="text-xs font-black text-signal-sell uppercase tracking-widest">
                    {yearlyExplainer.portfolio_return >= 0 ? 'What Could Have Been Better' : "What Didn't Work"}
                  </p>
                </div>
                <p className="text-sm text-dark-text leading-relaxed">{yearlyExplainer.what_didnt}</p>
              </div>

              {/* Macro Drivers */}
              <div className="bg-dark-bg border border-dark-border rounded-xl p-4">
                <div className="flex items-center gap-2 mb-2">
                  <Brain size={14} className="text-accent" />
                  <p className="text-xs font-black text-accent uppercase tracking-widest">Macro Context</p>
                </div>
                <p className="text-sm text-dark-text leading-relaxed">{yearlyExplainer.macro_drivers}</p>
              </div>

              {/* Risk Flags */}
              {yearlyExplainer.risk_flags?.length > 0 && (
                <div className="bg-yellow-500/5 border border-yellow-500/20 rounded-xl p-4">
                  <div className="flex items-center gap-2 mb-2">
                    <AlertTriangle size={14} className="text-yellow-500" />
                    <p className="text-xs font-black text-yellow-500 uppercase tracking-widest">Risk Flags</p>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {yearlyExplainer.risk_flags.map((flag, i) => (
                      <span
                        key={i}
                        className="px-2.5 py-1 bg-yellow-500/10 border border-yellow-500/20 rounded-full text-[11px] text-yellow-400 font-medium"
                      >
                        {flag}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {/* Lesson */}
              <div className="flex items-start gap-3 p-4 bg-accent/5 border border-accent/20 rounded-xl">
                <span className="text-xl shrink-0">💡</span>
                <div>
                  <p className="text-[10px] font-black text-accent uppercase tracking-widest mb-1">Lesson</p>
                  <p className="text-sm text-dark-text leading-relaxed font-medium">{yearlyExplainer.lesson}</p>
                </div>
              </div>

            </div>

            {/* Footer */}
            <div className="p-4 border-t border-dark-border flex items-center justify-between bg-dark-bg">
              <p className="text-[10px] text-dark-muted">
                AI analysis · {yearlyExplainer.year} portfolio data
              </p>
              <button
                onClick={() => setExplainerModal(false)}
                className="px-4 py-2 bg-accent hover:bg-accent/80 text-white rounded-lg text-xs font-black transition-all"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default BenchmarkDashboard;
