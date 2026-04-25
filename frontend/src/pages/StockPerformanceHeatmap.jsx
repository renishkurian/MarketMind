import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { TrendingUp, TrendingDown, Info, Download, Search, RefreshCw, Target, ArrowUpDown, PieChart } from 'lucide-react';
import Loader from '../components/Loader';
import toast from 'react-hot-toast';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const StockPerformanceHeatmap = () => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [yearSort, setYearSort] = useState('desc'); 
  const [sortConfig, setSortConfig] = useState({ key: null, direction: 'desc' }); // key can be 'symbol', 'optimal_weight', or a year string
  const [refreshing, setRefreshing] = useState(false);

  const getToken = () => localStorage.getItem('mm_token') || localStorage.getItem('token');

  const fetchMatrix = async (force = false) => {
    if (force) setRefreshing(true);
    else setLoading(true);

    try {
      const res = await axios.get(`${API_URL}/api/portfolio-performance/stock-performance-matrix?refresh=${force}`, {
        headers: { 'Authorization': `Bearer ${getToken()}` }
      });
      if (res.data.error) setError(res.data.error);
      else setData(res.data);
    } catch (err) {
      setError("Failed to load performance matrix. Ensure historical data is synced.");
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    fetchMatrix();
  }, []);

  const getHeatmapColor = (val) => {
    if (val === 'N/A') return 'bg-dark-bg text-dark-muted opacity-30';
    if (val > 25) return 'bg-emerald-500/30 text-emerald-400 border border-emerald-500/20';
    if (val > 10) return 'bg-emerald-500/20 text-emerald-400/90';
    if (val > 0) return 'bg-emerald-500/10 text-emerald-400/80';
    if (val < -25) return 'bg-rose-500/30 text-rose-400 border border-rose-500/20';
    if (val < -10) return 'bg-rose-500/20 text-rose-400/90';
    if (val < 0) return 'bg-rose-500/10 text-rose-400/80';
    return 'bg-dark-bg text-dark-text';
  };

  const filteredMatrix = data?.matrix?.filter(row => 
    row.symbol.toLowerCase().includes(searchTerm.toLowerCase()) ||
    row.company.toLowerCase().includes(searchTerm.toLowerCase())
  );

  // Sorting Logic for Rows
  if (sortConfig.key && filteredMatrix) {
    filteredMatrix.sort((a, b) => {
      let aVal, bVal;
      
      if (sortConfig.key === 'symbol') {
        aVal = a.symbol;
        bVal = b.symbol;
      } else if (sortConfig.key === 'optimal_weight') {
        aVal = a.optimal_weight;
        bVal = b.optimal_weight;
      } else {
        // Sort by year performance
        aVal = a.years[sortConfig.key];
        bVal = b.years[sortConfig.key];
        
        // Handle N/A
        if (aVal === 'N/A') return 1;
        if (bVal === 'N/A') return -1;
      }

      if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
      if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
      return 0;
    });
  }

  const sortedYears = data ? [...data.years].sort((a, b) => yearSort === 'desc' ? b - a : a - b) : [];

  const handleSort = (key) => {
    setSortConfig(current => ({
      key,
      direction: current.key === key && current.direction === 'desc' ? 'asc' : 'desc'
    }));
  };

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-8 animate-in fade-in duration-700">
      {/* Header Section */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-6">
        <div>
          <h1 className="text-3xl font-black text-dark-text tracking-tight flex items-center gap-3 italic">
            <TrendingUp className="text-accent" size={32} />
            Institutional Alpha Heatmap
          </h1>
          <p className="text-dark-muted text-sm font-medium mt-1">
            Historical Year-over-Year performance breakdown per individual holding.
          </p>
        </div>

        <div className="flex items-center gap-4">
           {/* Search Bar */}
           <div className="relative group">
            <Search size={14} className="absolute left-4 top-1/2 -translate-y-1/2 text-dark-muted group-focus-within:text-accent transition-colors" />
            <input 
              type="text" 
              placeholder="Filter by Stock..." 
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="bg-dark-card border border-dark-border rounded-xl pl-10 pr-4 py-2.5 text-xs font-bold text-white focus:outline-none focus:border-accent/50 w-64 transition-all"
            />
          </div>
          
          <button 
            onClick={() => fetchMatrix(true)}
            disabled={refreshing}
            className={`flex items-center gap-2 bg-dark-card border border-dark-border hover:border-accent/50 text-dark-text px-4 py-2.5 rounded-xl text-[10px] font-black uppercase tracking-widest transition-all ${refreshing ? 'opacity-50 cursor-not-allowed' : ''}`}
          >
            <RefreshCw size={14} className={refreshing ? 'animate-spin' : ''} />
            {refreshing ? 'Syncing...' : 'Regenerate'}
          </button>

          <button className="flex items-center gap-2 bg-dark-card border border-dark-border hover:border-accent/50 text-dark-muted hover:text-accent px-4 py-2.5 rounded-xl text-xs font-black uppercase tracking-widest transition-all">
            <Download size={14} /> Export
          </button>
        </div>
      </div>

      {loading ? (
        <div className="flex flex-col items-center justify-center py-48 gap-6 bg-dark-card/30 rounded-[3rem] border border-dark-border/50">
          <Loader size="lg" />
          <p className="text-dark-muted font-mono animate-pulse tracking-widest uppercase text-[10px]">Generating Performance Matrix...</p>
        </div>
      ) : error ? (
        <div className="flex flex-col items-center justify-center py-32 gap-6 bg-rose-500/5 border border-rose-500/20 rounded-[3rem]">
          <Info size={48} className="text-rose-500/40" />
          <p className="text-lg font-bold text-dark-text">{error}</p>
          <button onClick={() => window.location.reload()} className="px-8 py-3 bg-dark-card border border-dark-border rounded-2xl text-xs font-black uppercase tracking-widest hover:border-rose-500/50 transition-all">Retry Computation</button>
        </div>
      ) : (
        <div className="bg-dark-card border border-dark-border rounded-[2.5rem] shadow-2xl overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse">
              <thead>
                <tr className="bg-dark-bg/50 backdrop-blur-md">
                  <th 
                    onClick={() => handleSort('symbol')}
                    className="p-6 text-[10px] font-black text-dark-muted uppercase tracking-[0.2em] border-b border-dark-border sticky left-0 bg-dark-card z-30 w-48 shadow-lg cursor-pointer hover:text-accent transition-colors"
                  >
                    <div className="flex items-center gap-2">
                        Stock Asset
                        {sortConfig.key === 'symbol' && <ArrowUpDown size={10} className="text-accent" />}
                    </div>
                  </th>
                  <th 
                    onClick={() => handleSort('optimal_weight')}
                    className="p-6 text-[10px] font-black text-dark-muted uppercase tracking-[0.2em] border-b border-dark-border text-center z-20 cursor-pointer hover:text-accent transition-colors"
                  >
                    <div className="flex flex-col items-center gap-1">
                      <Target size={14} className={sortConfig.key === 'optimal_weight' ? 'text-accent' : 'text-dark-muted'} />
                      Target %
                    </div>
                  </th>
                  {sortedYears.map(yr => (
                    <th 
                      key={yr} 
                      onClick={() => handleSort(String(yr))}
                      className="p-6 text-[10px] font-black text-dark-muted uppercase tracking-[0.2em] border-b border-dark-border text-center min-w-[120px] cursor-pointer hover:text-accent transition-colors group/th"
                    >
                      <div className="flex items-center justify-center gap-2">
                        {yr}
                        {(sortConfig.key === String(yr)) ? (
                            <ArrowUpDown size={12} className="text-accent" />
                        ) : (
                            <ArrowUpDown size={12} className="opacity-0 group-hover/th:opacity-100 transition-opacity" />
                        )}
                      </div>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filteredMatrix.map((row, idx) => (
                  <tr key={row.symbol} className="group hover:bg-white/[0.02] transition-colors border-b border-dark-border/30 last:border-0">
                    <td className="p-6 sticky left-0 bg-dark-card group-hover:bg-dark-bg transition-colors z-30 shadow-lg">
                      <div className="flex flex-col">
                        <span className="text-sm font-black text-white italic group-hover:text-accent transition-colors">{row.symbol}</span>
                        <span className="text-[10px] text-dark-muted font-bold truncate max-w-[150px]">{row.company}</span>
                      </div>
                    </td>
                    <td className="p-6 text-center border-r border-dark-border/20 z-20">
                       <div className="flex flex-col items-center">
                          <span className="text-sm font-black text-accent">{row.optimal_weight}%</span>
                          <div className="w-12 h-1 bg-dark-border rounded-full mt-1 overflow-hidden">
                             <div 
                                className="h-full bg-accent transition-all duration-1000" 
                                style={{ width: `${Math.min(row.optimal_weight * 2, 100)}%` }} 
                             />
                          </div>
                       </div>
                    </td>
                    {sortedYears.map(yr => {
                      const val = row.years[yr];
                      return (
                        <td key={yr} className="p-2 border-r border-dark-border/20 last:border-0">
                          <div className={`h-16 w-full flex flex-col items-center justify-center rounded-2xl transition-all duration-500 group-hover:scale-[0.98] ${getHeatmapColor(val)}`}>
                            <span className="text-xs font-black">
                              {val === 'N/A' ? 'N/A' : (
                                <span className="flex items-center gap-0.5">
                                  {val > 0 ? '+' : ''}{val}%
                                  {val !== 0 && (val > 0 ? <TrendingUp size={10} /> : <TrendingDown size={10} />)}
                                </span>
                              )}
                            </span>
                          </div>
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          
          {/* Legend / Info Footer */}
          <div className="p-6 bg-dark-bg/50 border-t border-dark-border flex items-center justify-between">
            <div className="flex items-center gap-6">
               <div className="flex items-center gap-2">
                 <div className="w-3 h-3 bg-emerald-500/30 rounded-md border border-emerald-500/20" />
                 <span className="text-[10px] font-bold text-dark-muted uppercase tracking-widest">Major Gain (&gt;25%)</span>
               </div>
               <div className="flex items-center gap-2">
                 <div className="w-3 h-3 bg-rose-500/30 rounded-md border border-rose-500/20" />
                 <span className="text-[10px] font-bold text-dark-muted uppercase tracking-widest">Major Loss (&lt; -25%)</span>
               </div>
               <div className="flex items-center gap-2">
                 <div className="w-3 h-3 bg-dark-bg rounded-md opacity-30" />
                 <span className="text-[10px] font-bold text-dark-muted uppercase tracking-widest">Non-Holding Period (N/A)</span>
               </div>
            </div>
            <p className="text-[10px] font-black italic text-accent uppercase tracking-widest flex items-center gap-2 animate-pulse">
              <Info size={12} /> Institutional Grade Data Sync
            </p>
          </div>
        </div>
      )}
    </div>
  );
};

export default StockPerformanceHeatmap;
