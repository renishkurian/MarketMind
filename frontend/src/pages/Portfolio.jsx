import React, { useMemo, useRef, useState } from 'react';
import { useStockStore } from '../store/stockStore';
import { useNavigate } from 'react-router-dom';
import MetricCard from '../components/MetricCard';
import PortfolioTable from '../components/PortfolioTable';
import { Briefcase, TrendingUp, BarChart, ArrowDown, Upload, IndianRupee, Activity, PieChart, Search } from 'lucide-react';
import axios from 'axios';
import toast from 'react-hot-toast';
import Loader from '../components/Loader';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export default function Portfolio() {
  const { stocks, isConnected } = useStockStore();
  const navigate = useNavigate();


  const fileInputRef = useRef(null);
  const [isImporting, setIsImporting] = useState(false);

  const portfolioStocks = useMemo(() =>
    Object.values(stocks).filter(s => s.type === 'PORTFOLIO' || !s.type),
    [stocks]
  );
  
  const [searchQuery, setSearchQuery] = useState('');

  const filteredStocks = useMemo(() => {
    if (!searchQuery.trim()) return portfolioStocks;
    const q = searchQuery.toLowerCase();
    return portfolioStocks.filter(s => 
      s.symbol.toLowerCase().includes(q) || 
      (s.company_name?.toLowerCase().includes(q)) ||
      (s.scp_name?.toLowerCase().includes(q))
    );
  }, [portfolioStocks, searchQuery]);

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
        <div className="relative w-full md:w-96">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-dark-muted" size={18} />
          <input 
            type="text"
            placeholder="Search by symbol or name..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full bg-dark-card border border-dark-border rounded-xl py-2.5 pl-10 pr-4 text-dark-text placeholder:text-dark-muted focus:border-accent focus:ring-1 focus:ring-accent transition-all outline-none"
          />
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
    </div>
  );
}
