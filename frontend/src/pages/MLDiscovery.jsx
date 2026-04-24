import React, { useEffect, useState, useMemo } from 'react';
import { useStockStore } from '../store/stockStore';
import Loader from '../components/Loader';
import { Brain, TrendingUp, Shield, BarChart2, Zap, ArrowRight, Target, Clock, History, ChevronRight, Info } from 'lucide-react';
import axios from 'axios';
import toast from 'react-hot-toast';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export default function MLDiscovery() {
  const [loading, setLoading] = useState(true);
  const [currentSnapshot, setCurrentSnapshot] = useState(null);
  const [history, setHistory] = useState([]);
  const [showHistory, setShowHistory] = useState(false);
  const [sortBy, setSortBy] = useState('confidence'); // 'confidence' or 'return'
  const theme = useStockStore(state => state.theme);

  const fetchMLData = async () => {
    console.log("ML Discovery: Running new analysis...");
    setLoading(true);
    try {
      const token = localStorage.getItem('mm_token') || localStorage.getItem('token');
      const res = await axios.get(`${API_URL}/api/ml/portfolio-alpha?save=true`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      setCurrentSnapshot(res.data);
      fetchHistory(); // Refresh history list
    } catch (err) {
      console.error("ML Discovery Error:", err);
      toast.error(err.response?.data?.detail || "Failed to fetch ML insights");
    } finally {
      setLoading(false);
    }
  };

  const fetchHistory = async () => {
    try {
      const token = localStorage.getItem('mm_token') || localStorage.getItem('token');
      const res = await axios.get(`${API_URL}/api/ml/history`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      setHistory(res.data);
    } catch (err) {
      console.error("History fetch failed:", err);
    }
  };

  const loadSnapshot = async (id) => {
    setLoading(true);
    setShowHistory(false);
    try {
      const token = localStorage.getItem('mm_token') || localStorage.getItem('token');
      const res = await axios.get(`${API_URL}/api/ml/snapshot/${id}`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      setCurrentSnapshot(res.data);
    } catch (err) {
      toast.error("Failed to load snapshot");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchMLData();
    fetchHistory();
  }, []);

  const sortedPredictions = useMemo(() => {
    if (!currentSnapshot?.data) return [];
    const data = [...currentSnapshot.data];
    if (sortBy === 'confidence') {
      return data.sort((a, b) => b.confidence_score - a.confidence_score);
    } else {
      return data.sort((a, b) => b.prediction_5d_return - a.prediction_5d_return);
    }
  }, [currentSnapshot, sortBy]);

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-[70vh] gap-6">
        <div className="relative">
             <Brain size={64} className="text-accent animate-pulse" />
             <Zap size={24} className="text-yellow-400 absolute -top-1 -right-1 animate-bounce" />
        </div>
        <div className="text-center">
            <h2 className="text-xl font-bold text-dark-text">Processing ML Snapshot</h2>
            <p className="text-dark-muted text-sm mt-1">Training models and calculating confidence scores...</p>
        </div>
        <Loader size="lg" />
      </div>
    );
  }

  // Display date formatting helper
  const displayDate = (dateStr) => {
    if (!dateStr) return "Just now";
    const date = new Date(dateStr);
    return isNaN(date.getTime()) ? "Just now" : date.toLocaleString();
  };

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-8 animate-in fade-in duration-700 relative">
      
      {/* ── Sidebar Overlay for History ─────────────────────────────────── */}
      {showHistory && (
        <div className="fixed inset-0 z-[60] flex justify-end">
          <div className="absolute inset-0 bg-black/40 backdrop-blur-sm" onClick={() => setShowHistory(false)} />
          <div className="relative w-80 bg-dark-card border-l border-dark-border h-full shadow-2xl p-6 animate-in slide-in-from-right duration-300">
            <div className="flex items-center justify-between mb-8">
              <h3 className="text-lg font-bold text-dark-text flex items-center gap-2">
                <History size={18} className="text-accent" />
                Past Snapshots
              </h3>
              <button onClick={() => setShowHistory(false)} className="p-2 hover:bg-white/5 rounded-lg text-dark-muted"><ChevronRight /></button>
            </div>
            
            <div className="space-y-3 overflow-y-auto h-[calc(100vh-120px)] pr-2 custom-scrollbar">
              {history.length === 0 && <p className="text-center text-dark-muted py-10 text-sm italic">No past snapshots found.</p>}
              {history.map(s => (
                <button 
                  key={s.id}
                  onClick={() => loadSnapshot(s.id)}
                  className="w-full text-left p-4 rounded-xl border border-dark-border bg-dark-bg/50 hover:border-accent/40 hover:bg-accent/5 transition-all group"
                >
                  <div className="flex justify-between items-start mb-2">
                    <span className="text-[10px] font-bold text-dark-muted uppercase tracking-wider">
                      {new Date(s.created_at).toLocaleDateString('en-IN', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })}
                    </span>
                    <span className="text-[10px] bg-accent/10 text-accent px-1.5 py-0.5 rounded">ID: {s.id}</span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-bold text-dark-text">{s.summary?.avg_projected_return || 0}% Avg</span>
                    <span className="text-xs text-dark-muted">{s.summary?.stock_count || 0} stocks</span>
                  </div>
                </button>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-dark-text tracking-tight flex items-center gap-3">
            <Brain className="text-accent" size={32} />
            QuantDiscovery <span className="text-xs bg-accent/20 text-accent px-3 py-1 rounded-full uppercase tracking-widest font-black">AI Snapshot</span>
          </h1>
          <div className="flex flex-col gap-1 mt-2">
            <p className="text-dark-muted text-sm max-w-3xl leading-relaxed">
              This engine uses <span className="text-accent font-bold">Random Forest Regressors</span> to analyze historical SMA crossovers and volatility clusters. 
              It predicts the <span className="text-dark-text font-semibold">most probable price movement over the next 5 days</span> based on current technical anomalies.
            </p>
            <p className="text-[10px] text-dark-muted flex items-center gap-3">
              <span>Snapshot: <span className="text-dark-text font-bold">{displayDate(currentSnapshot?.created_at)}</span></span>
              <span className="w-1 h-1 bg-dark-border rounded-full" />
              <button onClick={() => setShowHistory(true)} className="text-accent font-bold hover:underline flex items-center gap-1 uppercase tracking-tighter">
                <History size={10} /> View Past Cycles
              </button>
            </p>
          </div>
        </div>
        
        <div className="flex items-center gap-3">
          <div className="flex items-center bg-dark-bg border border-dark-border rounded-xl p-1">
             <button 
               onClick={() => setSortBy('confidence')}
               className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all ${sortBy === 'confidence' ? 'bg-accent text-white shadow-lg' : 'text-dark-muted hover:text-dark-text'}`}
             >
               Confidence
             </button>
             <button 
               onClick={() => setSortBy('return')}
               className={`px-3 py-1.5 rounded-lg text-xs font-bold transition-all ${sortBy === 'return' ? 'bg-accent text-white shadow-lg' : 'text-dark-muted hover:text-dark-text'}`}
             >
               Return
             </button>
          </div>
          <button 
            onClick={fetchMLData}
            className="flex items-center gap-2 px-5 py-2.5 bg-accent hover:bg-blue-600 shadow-xl shadow-accent/20 rounded-xl transition-all text-sm font-bold text-white"
          >
            <RefreshCw size={18} />
            New Snapshot
          </button>
        </div>
      </div>

      {/* Stats Overview */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-dark-card border border-dark-border p-6 rounded-2xl">
          <p className="text-[10px] font-bold text-dark-muted uppercase tracking-widest mb-1">Avg Projected Return</p>
          <p className="text-2xl font-black text-signal-buy">
            {currentSnapshot?.summary?.avg_projected_return?.toFixed(3) || "0.000"}%
          </p>
        </div>
        <div className="bg-dark-card border border-dark-border p-6 rounded-2xl">
          <p className="text-[10px] font-bold text-dark-muted uppercase tracking-widest mb-1">High Confidence Signals</p>
          <p className="text-2xl font-black text-accent">{currentSnapshot?.summary?.high_confidence_count}</p>
        </div>
        <div className="bg-dark-card border border-dark-border p-6 rounded-2xl">
          <p className="text-[10px] font-bold text-dark-muted uppercase tracking-widest mb-1">Total Analysed</p>
          <p className="text-2xl font-black text-dark-text">{currentSnapshot?.summary?.stock_count}</p>
        </div>
      </div>

      {/* Top Gainers predicted */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {sortedPredictions.map((pred, idx) => (
          <div 
            key={pred.symbol}
            className="bg-dark-card border border-dark-border p-6 rounded-2xl relative overflow-hidden group hover:border-accent/30 transition-all shadow-xl"
          >
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-3">
                <div className="p-2.5 bg-dark-bg rounded-xl border border-dark-border group-hover:border-accent/20">
                   <Target className="text-accent" size={20} />
                </div>
                <div>
                   <h3 className="text-lg font-bold text-dark-text">{pred.symbol}</h3>
                   <span className="text-[10px] uppercase tracking-wider text-dark-muted font-bold">{pred.model_type}</span>
                </div>
              </div>
              <div className={`px-2 py-1 rounded-lg text-xs font-black ${pred.prediction_5d_return > 0 ? 'bg-signal-buy/10 text-signal-buy' : 'bg-signal-sell/10 text-signal-sell'}`}>
                {pred.prediction_5d_return > 0 ? '+' : ''}{pred.prediction_5d_return}%
              </div>
            </div>

            <div className="space-y-4">
              <div className="flex justify-between items-end">
                <div className="space-y-1">
                  <p className="text-[10px] font-bold text-dark-muted uppercase tracking-wider">Projected 5D Target</p>
                  <p className="text-xl font-black text-dark-text">₹{pred.projected_price}</p>
                </div>
                <div className="text-right">
                   <p className="text-[10px] font-bold text-dark-muted uppercase tracking-wider">Confidence</p>
                   <div className="flex items-center gap-1.5 justify-end">
                      <div className="h-1.5 w-12 bg-dark-bg rounded-full overflow-hidden border border-dark-border">
                        <div className="h-full bg-accent" style={{ width: `${pred.confidence_score * 100}%` }} />
                      </div>
                      <span className="text-xs font-bold text-dark-text">{Math.round(pred.confidence_score * 100)}%</span>
                   </div>
                </div>
              </div>

              <div className="pt-4 border-t border-dark-border flex items-center justify-between">
                 <div className="flex flex-wrap gap-1">
                    {pred.features_used.map(f => (
                      <span key={f} className="text-[8px] bg-dark-bg px-1.5 py-0.5 rounded border border-dark-border text-dark-muted font-mono">
                        {f.replace('dist_', '')}
                      </span>
                    ))}
                 </div>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* ── Terminologies & Meanings ───────────────────────────────────── */}
      <div className="pt-12 border-t border-dark-border/40">
        <h2 className="text-xl font-bold text-dark-text mb-6 flex items-center gap-2">
          <Info size={20} className="text-accent" />
          Concept Glossary
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <div className="space-y-2">
            <h4 className="text-xs font-bold text-accent uppercase tracking-widest">Projected 5D Return</h4>
            <p className="text-xs text-dark-muted leading-relaxed">
              The AI's estimated percentage price movement over the next 5 trading days. This represents the "Alpha" target for short-term opportunities.
            </p>
          </div>
          <div className="space-y-2">
            <h4 className="text-xs font-bold text-accent uppercase tracking-widest">Confidence Score</h4>
            <p className="text-xs text-dark-muted leading-relaxed">
              Measured by the degree of consensus among 50 individual decision trees. A score &gt; 80% suggests strong model agreement on the projected path.
            </p>
          </div>
          <div className="space-y-2">
            <h4 className="text-xs font-bold text-accent uppercase tracking-widest">Random Forest</h4>
            <p className="text-xs text-dark-muted leading-relaxed">
              An ensemble machine learning method that operates by constructing a multitude of decision trees at training time and outputting the average prediction.
            </p>
          </div>
          <div className="space-y-2">
            <h4 className="text-xs font-bold text-accent uppercase tracking-widest">Features (sma_20 / vol)</h4>
            <p className="text-xs text-dark-muted leading-relaxed">
              The data inputs for the model. <b>SMA Dist</b> identifies mean-reversion potential, while <b>Volatility</b> helps the model price-in recent risk clusters.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

const RefreshCw = ({ size, className }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={className}>
    <path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8" />
    <path d="M21 3v5h-5" />
    <path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16" />
    <path d="M3 21v-5h5" />
  </svg>
);
