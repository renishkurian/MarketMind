import React, { useEffect, useState } from 'react';
import { useStockStore } from '../store/stockStore';
import Loader from '../components/Loader';
import { ShieldAlert, TrendingUp, Shield, BarChart2, Zap, ArrowRight, Target, Info, Sparkles, Trophy, Radio, Search, Activity, Globe, MessageSquare, RefreshCcw, Database } from 'lucide-react';
import axios from 'axios';
import toast from 'react-hot-toast';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export default function WarRoom() {
  const { stocks, fetchPortfolio } = useStockStore();
  const portfolioArray = Object.values(stocks);
  const [selectedSymbol, setSelectedSymbol] = useState('');
  const [loading, setLoading] = useState(false);
  const [research, setResearch] = useState(null);
  const [logs, setLogs] = useState([]);

  useEffect(() => {
    fetchPortfolio();
  }, [fetchPortfolio]);

  const addLog = (msg) => {
    setLogs(prev => [...prev, { time: new Date().toLocaleTimeString(), msg }]);
  };

  const runResearch = async (symbol, rebuild = false) => {
    if (!symbol) return;
    setLoading(true);
    setResearch(null);
    setLogs([]);
    setSelectedSymbol(symbol);

    if (rebuild) {
        addLog(`FORCING REBUILD OF INTELLIGENCE...`);
    } else {
        addLog(`INITIATING DEEP RESEARCH FOR ${symbol}...`);
    }
    
    addLog("FETCHING 10Y PRICE HISTORY...");
    addLog("CALCULATING XGBOOST CONVICTION...");
    
    try {
      const token = localStorage.getItem('mm_token') || localStorage.getItem('token');
      
      // Artificial delay to show the 'Thinking' process for Pro feel
      await new Promise(r => setTimeout(r, 1000));
      addLog("CONNECTING TO MARKET NEWS FEED...");
      addLog("EXTRACTING SENTIMENTAL DRIVERS...");
      
      const res = await axios.get(`${API_URL}/api/war-room/deep-research/${symbol}?rebuild=${rebuild}`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      
      if (res.data.from_cache) {
          addLog("RESTORED RESEARCH FROM SECURE ARCHIVE.");
      } else {
          addLog("SYNTHESIZING PRO VERDICT...");
      }
      setResearch(res.data);
    } catch (err) {
      toast.error(err.response?.data?.detail || "War Room analysis failed");
      addLog("CRITICAL ERROR IN NEURAL SYNTHESIS.");
    } finally {
      setLoading(false);
    }
  };

  if (portfolioArray.length === 0) {
    return (
        <div className="h-full flex flex-col items-center justify-center bg-black gap-6">
            <Loader />
            <p className="text-dark-muted font-mono text-[10px] uppercase tracking-[0.4em] animate-pulse">Initializing War Room Neural Engine...</p>
        </div>
    );
  }

  return (
    <div className="flex h-full bg-black overflow-hidden flex-col md:flex-row">
      
      {/* ── Left Selection Pane ───────────────────────────────────── */}
      <div className="w-full md:w-72 border-r border-dark-border bg-dark-bg p-4 flex flex-col gap-4 overflow-y-auto custom-scrollbar">
        <div className="flex items-center gap-2 px-2 mb-2">
            <Radio className="text-signal-buy animate-pulse" size={16} />
            <h2 className="text-[10px] font-black text-dark-text uppercase tracking-[#0.4em] text-white">War Room Assets</h2>
        </div>
        {portfolioArray.map(stock => (
            <button 
                key={stock.symbol}
                onClick={() => runResearch(stock.symbol)}
                disabled={loading}
                className={`w-full text-left p-4 rounded-xl border transition-all ${
                    selectedSymbol === stock.symbol 
                    ? 'border-accent bg-accent/10' 
                    : 'border-dark-border hover:border-dark-border/60 hover:bg-white/[0.02]'
                }`}
            >
                <div className="flex items-center justify-between">
                    <span className="font-bold text-white">{stock.symbol}</span>
                    <ArrowRight size={14} className={selectedSymbol === stock.symbol ? 'text-accent' : 'text-dark-muted'} />
                </div>
                <div className="text-[10px] text-dark-muted mt-1 uppercase font-bold tracking-tighter">
                    Ready for Intel
                </div>
            </button>
        ))}
      </div>

      {/* ── Main Intel Center ───────────────────────────────────── */}
      <div className="flex-1 overflow-y-auto bg-black p-8 custom-scrollbar">
        {!selectedSymbol && (
            <div className="h-full flex flex-col items-center justify-center text-center space-y-4">
                <Globe size={80} className="text-dark-muted opacity-20 animate-spin-slow" />
                <h2 className="text-2xl font-black text-white uppercase tracking-tighter italic">Select Asset for Deep Research</h2>
                <p className="text-dark-muted text-sm max-w-sm">Combining ML convictions with real-time news synthesis for advanced institutional alpha detection.</p>
            </div>
        )}

        {loading && (
            <div className="h-full flex flex-col gap-8">
                <div className="space-y-2 border-l-2 border-accent/30 pl-4 py-2">
                    {logs.map((log, i) => (
                        <div key={i} className="flex gap-4 items-center animate-in fade-in slide-in-from-left duration-300">
                            <span className="text-[10px] font-mono text-accent">{log.time}</span>
                            <span className="text-xs font-bold text-white tracking-widest uppercase">{log.msg}</span>
                        </div>
                    ))}
                </div>
                <div className="flex-1 flex items-center justify-center">
                    <div className="relative">
                        <div className="w-32 h-32 border-2 border-accent/20 rounded-full animate-ping" />
                        <Activity className="absolute inset-0 m-auto text-accent" size={48} />
                    </div>
                </div>
            </div>
        )}

        {research && !loading && (
            <div className="space-y-10 animate-in zoom-in-95 duration-700">
                {/* Top Banner */}
                <div className="flex flex-col md:flex-row items-start md:items-center justify-between gap-6 bg-dark-card border border-dark-border p-8 rounded-3xl relative overflow-hidden">
                    <div className="absolute top-0 right-0 p-2 text-[10px] font-black text-accent opacity-20 uppercase tracking-[.5em]">Classified Intel</div>
                    <div className="flex items-center gap-6">
                        <div className="w-16 h-16 bg-accent rounded-2xl flex items-center justify-center text-3xl font-black text-white shadow-2xl shadow-accent/40">
                            {research.symbol[0]}
                        </div>
                        <div>
                            <h1 className="text-4xl font-black text-white tracking-tighter">{research.symbol}</h1>
                            <div className="flex items-center gap-3 mt-1">
                                <span className={`px-2 py-0.5 rounded text-[10px] font-black uppercase ${
                                    research.ai_intelligence.institutional_action === 'BUY_HEAVY' ? 'bg-signal-buy text-white' : 'bg-dark-bg text-dark-muted'
                                }`}>
                                    {research.ai_intelligence.institutional_action || 'DEEP_DIVE'}
                                </span>
                                {research.from_cache && (
                                    <span className="flex items-center gap-1 px-2 py-0.5 rounded bg-amber-500/10 text-amber-500 border border-amber-500/20 text-[9px] font-black uppercase">
                                        <Database size={10} /> Archived
                                    </span>
                                )}
                                <span className="text-[10px] text-dark-muted font-bold uppercase tracking-widest hidden md:inline">Last Rescan: {new Date(research.generated_at).toLocaleTimeString()}</span>
                            </div>
                        </div>
                    </div>
                    <div className="flex items-center gap-8">
                        <button 
                            onClick={() => runResearch(research.symbol, true)}
                            className="flex items-center gap-2 px-4 py-2 bg-white/5 hover:bg-white/10 border border-white/10 rounded-xl transition-all group"
                            title="Re-run AI Synthesis (Costs Tokens)"
                        >
                            <RefreshCcw size={14} className="text-dark-muted group-hover:text-accent group-hover:rotate-180 transition-all duration-500" />
                            <span className="text-[10px] font-black text-dark-muted group-hover:text-white uppercase tracking-wider">Rebuild</span>
                        </button>
                        <div className="text-right">
                            <p className="text-[10px] font-bold text-dark-muted uppercase mb-1">Final Intel Score</p>
                            <p className="text-5xl font-black text-accent">{research.ml_data.conviction_score}%</p>
                        </div>
                    </div>
                </div>

                {/* Verdict Grid */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                    {/* The Bull Case */}
                    <div className="p-8 bg-signal-buy/[0.03] border border-signal-buy/20 rounded-3xl space-y-4">
                        <h3 className="text-xl font-black text-white uppercase italic flex items-center gap-2">
                             <TrendingUp className="text-signal-buy" size={20} /> Bull Case
                        </h3>
                        <ul className="space-y-3">
                            {research.ai_intelligence.bull_case?.map((point, i) => (
                                <li key={i} className="flex gap-3 text-sm text-dark-muted leading-relaxed">
                                    <span className="text-signal-buy font-bold opacity-40">+{i+1}</span>
                                    {point}
                                </li>
                            ))}
                        </ul>
                    </div>

                    {/* The Bear Case */}
                    <div className="p-8 bg-signal-sell/[0.03] border border-signal-sell/20 rounded-3xl space-y-4">
                        <h3 className="text-xl font-black text-white uppercase italic flex items-center gap-2">
                             <ShieldAlert className="text-signal-sell" size={20} /> Bear Case
                        </h3>
                        <ul className="space-y-3">
                            {research.ai_intelligence.bear_case?.map((point, i) => (
                                <li key={i} className="flex gap-3 text-sm text-dark-muted leading-relaxed">
                                    <span className="text-signal-sell font-bold opacity-40">-{i+1}</span>
                                    {point}
                                </li>
                            ))}
                        </ul>
                    </div>
                </div>

                {/* News Intelligence */}
                <div className="bg-dark-card border border-dark-border p-8 rounded-3xl">
                     <h3 className="text-lg font-black text-white uppercase mb-6 flex items-center gap-3">
                        <Globe size={24} className="text-accent" />
                        Scanned Intelligence (Search-Driven)
                     </h3>
                     <div className="grid grid-cols-1 md:grid-cols-2 gap-x-12 gap-y-6">
                        {research.news_analyzed?.map((news, i) => (
                            <div key={i} className="flex gap-4 items-start group">
                                <div className="w-1.5 h-1.5 rounded-full bg-dark-border mt-1.5 group-hover:bg-accent group-hover:scale-125 transition-all" />
                                <p className="text-[13px] text-dark-muted leading-relaxed group-hover:text-dark-text transition-colors">{news}</p>
                            </div>
                        ))}
                     </div>
                </div>

                {/* Final Pro Verdict */}
                <div className="p-10 bg-accent text-white rounded-[40px] relative overflow-hidden group">
                    <div className="absolute top-0 right-0 p-8 opacity-20 -rotate-12 translate-x-4">
                        <MessageSquare size={120} />
                    </div>
                    <div className="relative z-10 space-y-4">
                        <div className="flex items-center gap-2">
                            <Sparkles size={16} />
                            <span className="text-[10px] font-black uppercase tracking-[.3em]">Institutional Pro Verdict</span>
                        </div>
                        <h2 className="text-3xl font-black tracking-tighter uppercase leading-none max-w-4xl">
                            {research.ai_intelligence.pro_verdict}
                        </h2>
                    </div>
                </div>
            </div>
        )}
      </div>
    </div>
  );
}
