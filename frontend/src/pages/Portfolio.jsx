import React, { useMemo, useRef, useState } from 'react';
import { useStockStore } from '../store/stockStore';
import { useNavigate } from 'react-router-dom';
import MetricCard from '../components/MetricCard';
import PortfolioTable from '../components/PortfolioTable';
import { Briefcase, TrendingUp, BarChart, ArrowDown, Upload, IndianRupee, Activity, PieChart, Search, RefreshCw } from 'lucide-react';
import axios from 'axios';
import toast from 'react-hot-toast';
import Loader from '../components/Loader';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export default function Portfolio() {
  const { stocks, isConnected } = useStockStore();
  const navigate = useNavigate();


  const fileInputRef = useRef(null);
  const [isImporting, setIsImporting] = useState(false);

  // ── Modal / feature state — must be declared before any early returns ────
  const [showAllocateModal, setShowAllocateModal] = useState(false);
  const [allocateAmount, setAllocateAmount] = useState(10000);
  const [allocateLimit, setAllocateLimit] = useState('');
  const [strategy, setStrategy] = useState('AI_PULSE');
  const [isAllocating, setIsAllocating] = useState(false);
  const [allocationResult, setAllocationResult] = useState(null);
  const [isSyncing, setIsSyncing] = useState(false);

  const portfolioStocks = useMemo(() =>
    Object.values(stocks).filter(s => s.type === 'PORTFOLIO' || !s.type),
    [stocks]
  );
  
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCategory, setSelectedCategory] = useState('ALL');
  const [selectedSector, setSelectedSector] = useState('ALL');

  const filteredStocks = useMemo(() => {
    let filtered = portfolioStocks;
    
    // Filter by Search
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase();
      filtered = filtered.filter(s => 
        s.symbol.toLowerCase().includes(q) || 
        (s.company_name?.toLowerCase().includes(q)) ||
        (s.scp_name?.toLowerCase().includes(q))
      );
    }
    
    // Filter by Category (Market Cap)
    if (selectedCategory !== 'ALL') {
      filtered = filtered.filter(s => s.market_cap_cat === selectedCategory);
    }
    
    // Filter by Sector
    if (selectedSector !== 'ALL') {
      filtered = filtered.filter(s => s.sector === selectedSector);
    }
    
    return filtered;
  }, [portfolioStocks, searchQuery, selectedCategory, selectedSector]);

  // Extract unique sectors from portfolio for the filter
  const sectors = useMemo(() => {
    const s = new Set();
    portfolioStocks.forEach(st => {
      if (st.sector) s.add(st.sector);
    });
    return Array.from(s).sort();
  }, [portfolioStocks]);

  const stats = useMemo(() => {
    let invested = 0;
    let currentVal = 0;
    let yesterdayVal = 0;

    portfolioStocks.forEach(stock => {
      const qty = stock.quantity || 0;
      const buy = stock.avg_buy_price || 0;
      const sig = stock.signal || {};
      
      const current = parseFloat(sig.current_price || sig.prev_close || 0);
      const prev = parseFloat(sig.prev_close || 0);
      const changePct = parseFloat(sig.change_pct || 0);

      invested += qty * buy;
      currentVal += qty * current;

      if (prev > 0) {
        yesterdayVal += qty * prev;
      } else if (changePct !== 0 && current > 0) {
        // Derive yesterday's value from change percentage if PC is missing
        const derivedPrev = current / (1 + changePct / 100);
        yesterdayVal += qty * derivedPrev;
      } else {
        yesterdayVal += qty * current;
      }
    });

    const totalPnl = invested > 0 ? currentVal - invested : 0;
    const totalPnlPct = invested > 0 ? (totalPnl / invested) * 100 : 0;
    
    const dayPnl = currentVal - yesterdayVal;
    const dayPnlPct = yesterdayVal > 0 ? (dayPnl / yesterdayVal) * 100 : 0;

    return {
      total: portfolioStocks.length,
      buy: portfolioStocks.filter(s => s.signal?.st_signal === 'BUY').length,
      hold: portfolioStocks.filter(s => s.signal?.st_signal === 'HOLD').length,
      sell: portfolioStocks.filter(s => s.signal?.st_signal === 'SELL').length,
      invested,
      currentVal,
      totalPnl,
      totalPnlPct,
      dayPnl,
      dayPnlPct
    };
  }, [portfolioStocks]);

  // Show loader while initial sync is happening
  if (Object.keys(stocks).length === 0 && !isConnected) {
    return (
      <div className="flex flex-col items-center justify-center h-[70vh] gap-6 animate-in fade-in duration-500">
        <Loader size="lg" />
      </div>
    );
  }

  const formatCurrency = (val) => {
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      maximumFractionDigits: 0
    }).format(val);
  };

  const handleFileUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    const formData = new FormData();
    formData.append('file', file);

    setIsImporting(true);
    try {
      const res = await axios.post(`${API_URL}/api/portfolio/import`, formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
          'Authorization': `Bearer ${localStorage.getItem('mm_token')}`
        }
      });
      toast.success(res.data.message);
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Failed to import portfolio');
    } finally {
      setIsImporting(false);
      if (fileInputRef.current) fileInputRef.current.value = '';
    }
  };


  const handleAllocate = async () => {
    if (allocateAmount <= 0) return toast.error("Amount must be greater than 0");
    setIsAllocating(true);
    setAllocationResult(null);
    try {
      const payload = {
        amount: parseFloat(allocateAmount),
        strategy: strategy
      };
      if (allocateLimit.trim() !== '') {
         payload.limit = parseInt(allocateLimit, 10);
      }
      
      const res = await axios.post(`${API_URL}/api/portfolio/allocate`, payload, {
        headers: { 'Authorization': `Bearer ${localStorage.getItem('mm_token')}` }
      });
      setAllocationResult(res.data);
      toast.success("Allocation calculated successfully!");
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Allocation failed');
    } finally {
      setIsAllocating(false);
    }
  };

  const handleSyncSignals = async () => {
    setIsSyncing(true);
    const toastId = toast.loading(`Syncing signals for ${portfolioStocks.length} stocks…`);
    try {
      const res = await axios.post(`${API_URL}/api/portfolio/sync-signals`, {}, {
        headers: { 'Authorization': `Bearer ${localStorage.getItem('mm_token')}` }
      });
      const { success_count, error_count, failed_symbols } = res.data;
      toast.dismiss(toastId);
      if (error_count === 0) {
        toast.success(`✅ All ${success_count} signals synced!`);
      } else {
        toast.success(`Synced ${success_count} stocks. ⚠️ ${error_count} failed: ${failed_symbols.join(', ')}`);
      }
    } catch (err) {
      toast.dismiss(toastId);
      toast.error(err.response?.data?.detail || 'Sync failed');
    } finally {
      setIsSyncing(false);
    }
  };

  return (
    <div className="flex flex-col min-h-screen p-6 space-y-6">
      <div className="flex justify-between items-end">
        <div>
          <h2 className="text-3xl font-extrabold tracking-tight text-dark-text">Portfolio</h2>
          <p className="text-dark-muted text-sm mt-1 font-medium">{portfolioStocks.length} assets under management</p>
        </div>
        <div className="flex items-center gap-4">
          <input 
            type="file" 
            accept=".xlsx, .xls" 
            className="hidden" 
            ref={fileInputRef} 
            onChange={handleFileUpload} 
          />
          <button
            onClick={handleSyncSignals}
            disabled={isSyncing}
            title="Recompute signals for all portfolio stocks"
            className="flex items-center gap-2 px-4 py-2 bg-dark-card border border-dark-border rounded-xl text-sm font-semibold hover:border-signal-buy hover:text-signal-buy transition-all disabled:opacity-50"
          >
            <RefreshCw size={16} className={isSyncing ? 'animate-spin' : ''} />
            {isSyncing ? 'Syncing…' : 'Sync Signals'}
          </button>
          <button 
            onClick={() => setShowAllocateModal(true)}
            className="flex items-center gap-2 px-4 py-2 bg-dark-card border border-dark-border rounded-xl text-sm font-semibold hover:border-accent hover:text-accent transition-all"
          >
            <PieChart size={16} />
            Smart Allocate
          </button>
          <button 
            onClick={() => fileInputRef.current?.click()}
            disabled={isImporting}
            className="flex items-center gap-2 px-4 py-2 bg-dark-card border border-dark-border rounded-xl text-sm font-semibold hover:border-accent hover:text-accent transition-all disabled:opacity-50"
          >
            {isImporting ? (
               <div className="w-4 h-4 border-2 border-accent/40 border-t-accent rounded-full animate-spin" />
            ) : <Upload size={16} />}
            {isImporting ? 'Importing...' : 'Import XLSX'}
          </button>
          
          <div className="text-right border-l border-dark-border pl-4">
              <span className="text-[10px] font-bold text-dark-muted uppercase tracking-widest">Global Status</span>
              <div className="flex items-center gap-2 text-signal-buy font-mono font-bold">
                  <TrendingUp size={14} /> ACTIVE FEED
              </div>
          </div>
        </div>
      </div>

      {/* Portfolio Financial Summaries */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <MetricCard 
            label="Total Invested" 
            value={formatCurrency(stats.invested)} 
            color="text-dark-text" 
            icon={IndianRupee}
            sub={`${stats.total} Active positions`}
        />
        <MetricCard 
            label="Total Profit / Loss" 
            value={formatCurrency(stats.totalPnl)} 
            color={stats.totalPnl >= 0 ? "text-signal-buy" : "text-signal-sell"} 
            icon={PieChart}
            sub={`${stats.totalPnl >= 0 ? '+' : ''}${stats.totalPnlPct.toFixed(2)}% Over all time`}
        />
        <MetricCard 
            label="Today's P&L" 
            value={formatCurrency(stats.dayPnl)} 
            color={stats.dayPnl >= 0 ? "text-signal-buy" : "text-signal-sell"} 
            icon={Activity}
            sub={`${stats.dayPnl >= 0 ? '+' : ''}${stats.dayPnlPct.toFixed(2)}% vs Yesterday`}
        />
      </div>

      {/* Table Actions / Search */}
      <div className="flex flex-col md:flex-row gap-4 items-center justify-between">
        <div className="flex flex-col md:flex-row gap-3 w-full md:w-auto items-center">
          <div className="relative w-full md:w-80">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-dark-muted" size={18} />
            <input 
              type="text"
              placeholder="Search by symbol or name..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full bg-dark-card border border-dark-border rounded-xl py-2 pr-4 pl-10 text-dark-text placeholder:text-dark-muted focus:border-accent focus:ring-1 focus:ring-accent transition-all outline-none"
            />
          </div>
          
          <select
            value={selectedCategory}
            onChange={(e) => setSelectedCategory(e.target.value)}
            className="w-full md:w-40 bg-dark-card border border-dark-border rounded-xl py-2 px-3 text-dark-text focus:border-accent focus:ring-1 focus:ring-accent transition-all outline-none appearance-none cursor-pointer font-medium text-sm"
          >
            <option value="ALL">All Sizes</option>
            <option value="LARGE">Large Cap</option>
            <option value="MID">Mid Cap</option>
            <option value="SMALL">Small Cap</option>
          </select>

          <select
            value={selectedSector}
            onChange={(e) => setSelectedSector(e.target.value)}
            className="w-full md:w-48 bg-dark-card border border-dark-border rounded-xl py-2 px-3 text-dark-text focus:border-accent focus:ring-1 focus:ring-accent transition-all outline-none appearance-none cursor-pointer font-medium text-sm"
          >
            <option value="ALL">All Sectors</option>
            {sectors.map(s => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>
        <div className="text-dark-muted text-xs font-medium">
          Showing {filteredStocks.length} of {portfolioStocks.length} stocks
        </div>
      </div>

      {/* Signal Distribution */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <MetricCard 
            label="Total Stocks" 
            value={stats.total} 
            color="text-accent" 
            icon={Briefcase}
            sub="Under management"
        />
        <MetricCard 
            label="BUY Signals" 
            value={stats.buy} 
            color="text-signal-buy" 
            icon={TrendingUp}
            sub="Accumulation"
        />
        <MetricCard 
            label="HOLD Signals" 
            value={stats.hold} 
            color="text-signal-hold" 
            icon={BarChart}
            sub="Stability"
        />
        <MetricCard 
            label="SELL Signals" 
            value={stats.sell} 
            color="text-signal-sell" 
            icon={ArrowDown}
            sub="Profit taking"
        />
      </div>

      {/* Main Table */}
      <div className="flex-1">
        <PortfolioTable stocks={filteredStocks} />
      </div>

      {portfolioStocks.length === 0 && (
        <div className="flex flex-col items-center justify-center py-20 bg-dark-card rounded-2xl border-2 border-dashed border-dark-border opacity-60">
            <Briefcase size={48} className="text-dark-muted mb-4" />
            <p className="text-lg font-bold text-dark-text">No portfolio stocks loaded</p>
            <p className="text-sm text-dark-muted">Import your holdings via XLSX or add symbols from the Watchlist.</p>
        </div>
      )}

      {/* Allocation Modal */}
      {showAllocateModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4 animate-in fade-in">
          <div className="bg-dark-bg border border-dark-border rounded-2xl w-full max-w-2xl max-h-[90vh] overflow-hidden flex flex-col shadow-2xl">
            <div className="p-6 border-b border-dark-border flex items-center justify-between">
              <div>
                <h3 className="text-xl font-black text-dark-text tracking-tight flex items-center gap-2">
                  <PieChart className="text-accent" /> Smart Allocate
                </h3>
                <p className="text-sm text-dark-muted mt-1">Distribute capital optimally across your portfolio</p>
              </div>
              <button 
                onClick={() => setShowAllocateModal(false)}
                className="text-dark-muted hover:text-dark-text transition-colors"
              >
                Close
              </button>
            </div>
            
            <div className="p-6 overflow-y-auto flex-1 space-y-6 text-left">
              <div className="flex flex-col md:flex-row gap-4 items-center">
                <div className="flex-1 w-full">
                  <label className="block text-xs font-bold text-dark-muted uppercase tracking-wider mb-2">Amount to Allocate (₹)</label>
                  <input 
                    type="number" 
                    value={allocateAmount}
                    onChange={(e) => setAllocateAmount(e.target.value)}
                    className="w-full bg-dark-card border border-dark-border text-lg font-mono font-bold text-dark-text rounded-xl p-3 focus:outline-none focus:border-accent transition-colors"
                  />
                </div>
                <div className="w-full md:w-48">
                  <label className="block text-xs font-bold text-dark-muted uppercase tracking-wider mb-2">Strategy</label>
                  <select 
                    value={strategy}
                    onChange={(e) => setStrategy(e.target.value)}
                    className="w-full bg-dark-card border border-dark-border rounded-xl p-3 text-sm font-semibold text-dark-text focus:outline-none focus:border-accent transition-colors cursor-pointer"
                  >
                    <option value="AI_PULSE">AI Pulse (Conviction)</option>
                    <option value="HRP">Smart Stability (HRP)</option>
                    <option value="MVO">Modern Classic (MVO)</option>
                    <option value="BLACK_LITTERMAN">Confidence (BL)</option>
                    <option value="ERC">Balanced Risk (ERC)</option>
                    <option value="CVAR">Tail-Risk (CVaR)</option>
                  </select>
                </div>
                <div className="w-full md:w-32">
                  <label className="block text-xs font-bold text-dark-muted uppercase tracking-wider mb-2">Max Stocks</label>
                  <input 
                    type="number" 
                    placeholder="All"
                    value={allocateLimit}
                    onChange={(e) => setAllocateLimit(e.target.value)}
                    className="w-full bg-dark-card border border-dark-border text-lg font-mono font-bold text-dark-text rounded-xl p-2.5 focus:outline-none focus:border-accent transition-colors placeholder:text-gray-600/50"
                  />
                </div>
              </div>

              <p className="text-[11px] text-dark-muted leading-relaxed">
                {strategy === 'AI_PULSE' && "Weighted by AI conviction scores. Fastest but doesn't account for stock correlations."}
                {strategy === 'HRP' && "Gold standard for stability. Clusters stocks behaviorally to ensure true diversification."}
                {strategy === 'MVO' && "Maximizes return-per-unit-of-risk. Math-heavy model based on Markowitz's efficient frontier."}
                {strategy === 'BLACK_LITTERMAN' && "Blends 10-year market equilibrium with our AI's proprietary conviction 'views'."}
                {strategy === 'ERC' && "Conservative approach where every stock contributes the exact same risk to the total."}
                {strategy === 'CVAR' && "Survival-focused. Specifically optimizes to minimize potential for heavy losses during crashes."}
              </p>

              <button 
                onClick={handleAllocate}
                disabled={isAllocating}
                className="w-full bg-accent hover:bg-accent-hover text-white font-bold py-3.5 rounded-xl transition-all shadow-lg shadow-accent/20 active:scale-[0.98] disabled:opacity-50 flex items-center justify-center gap-2"
              >
                {isAllocating ? (
                  <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                ) : <PieChart size={18} />}
                Generate Allocation
              </button>

              {allocationResult && (
                <div className="space-y-4 pt-6 border-t border-dark-border animate-in slide-in-from-top-4 flex flex-col">
                  <div className="flex flex-wrap gap-3">
                    <div className="flex-1 min-w-[120px] bg-dark-card border border-dark-border rounded-xl p-3">
                      <p className="text-[10px] font-bold text-dark-muted uppercase mb-1">Lookback</p>
                      <p className="text-sm font-bold text-dark-text tracking-tight">{allocationResult.lookback_days || 'Latest'} Days</p>
                    </div>
                    {allocationResult.metrics?.expected_sharpe > 0 && (
                      <div className="flex-1 min-w-[120px] bg-dark-card border border-dark-border rounded-xl p-3">
                        <p className="text-[10px] font-bold text-dark-muted uppercase mb-1">Est. Sharpe</p>
                        <p className="text-sm font-bold text-signal-buy tracking-tight">{allocationResult.metrics.expected_sharpe.toFixed(2)}</p>
                      </div>
                    )}
                    {allocationResult.metrics?.expected_volatility > 0 && (
                      <div className="flex-1 min-w-[120px] bg-dark-card border border-dark-border rounded-xl p-3">
                        <p className="text-[10px] font-bold text-dark-muted uppercase mb-1">Est. Volatility</p>
                        <p className="text-sm font-bold text-signal-sell tracking-tight">{(allocationResult.metrics.expected_volatility * 100).toFixed(1)}%</p>
                      </div>
                    )}
                  </div>
                  
                  {allocationResult.rationale && (
                    <div className="bg-accent/5 border border-accent/20 rounded-xl p-4 text-xs text-dark-text leading-relaxed italic">
                      {allocationResult.rationale}
                    </div>
                  )}
                  <div className="border border-dark-border rounded-xl bg-dark-card overflow-hidden">
                    <table className="w-full text-sm text-left">
                      <thead className="bg-gray-900/40 text-dark-muted border-b border-dark-border text-xs uppercase tracking-wider">
                        <tr>
                          <th className="px-4 py-3">Symbol</th>
                          <th className="px-4 py-3 text-right">Allocation (₹)</th>
                          <th className="px-4 py-3 text-right">Est. Qty</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-dark-border">
                        {allocationResult.allocations?.map((item, idx) => (
                          <tr key={idx} className="hover:bg-gray-800/40 transition-colors">
                            <td className="px-4 py-3">
                              <span className="font-mono font-bold text-accent">{item.symbol}</span>
                              <div className="text-[10px] text-dark-muted font-sans font-normal mt-0.5 line-clamp-2" title={item.reason}>
                                {item.reason}
                              </div>
                            </td>
                            <td className="px-4 py-3 text-right font-mono font-bold text-signal-buy whitespace-nowrap">
                              ₹{parseFloat(item.allocated_amount).toLocaleString('en-IN', { maximumFractionDigits: 0 })}
                            </td>
                            <td className="px-4 py-3 text-right font-mono text-dark-muted whitespace-nowrap">
                              {parseFloat(item.estimated_qty).toFixed(1)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
