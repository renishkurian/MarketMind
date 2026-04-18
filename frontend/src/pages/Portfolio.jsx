import React, { useMemo, useRef, useState } from 'react';
import { useStockStore } from '../store/stockStore';
import { useNavigate } from 'react-router-dom';
import MetricCard from '../components/MetricCard';
import PortfolioTable from '../components/PortfolioTable';
import { Briefcase, TrendingUp, BarChart, ArrowDown, Upload } from 'lucide-react';
import axios from 'axios';
import toast from 'react-hot-toast';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export default function Portfolio() {
  const { stocks } = useStockStore();
  const fileInputRef = useRef(null);
  const [isImporting, setIsImporting] = useState(false);

  const portfolioStocks = useMemo(() =>
    Object.values(stocks).filter(s => s.type === 'PORTFOLIO' || !s.type),
    [stocks]
  );

  const stats = useMemo(() => ({
    total: portfolioStocks.length,
    buy: portfolioStocks.filter(s => s.signal?.st_signal === 'BUY').length,
    hold: portfolioStocks.filter(s => s.signal?.st_signal === 'HOLD').length,
    sell: portfolioStocks.filter(s => s.signal?.st_signal === 'SELL').length,
  }), [portfolioStocks]);

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

      {/* Summary Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <MetricCard 
            label="Total Stocks" 
            value={stats.total} 
            color="text-accent" 
            icon={Briefcase}
            sub="Active holdings"
        />
        <MetricCard 
            label="BUY Signals" 
            value={stats.buy} 
            color="text-signal-buy" 
            icon={TrendingUp}
            sub="Accumulation phase"
        />
        <MetricCard 
            label="HOLD Signals" 
            value={stats.hold} 
            color="text-signal-hold" 
            icon={BarChart}
            sub="Stability monitoring"
        />
        <MetricCard 
            label="SELL Signals" 
            value={stats.sell} 
            color="text-signal-sell" 
            icon={ArrowDown}
            sub="Profit taking phase"
        />
      </div>

      {/* Main Table */}
      <div className="flex-1">
        <PortfolioTable stocks={portfolioStocks} />
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
