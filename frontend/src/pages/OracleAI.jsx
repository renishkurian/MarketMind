import React, { useEffect, useState } from 'react';
import { useStockStore } from '../store/stockStore';
import Loader from '../components/Loader';
import { Brain, TrendingUp, Shield, BarChart2, Zap, ArrowRight, Target, Info, Sparkles, Trophy } from 'lucide-react';
import axios from 'axios';
import toast from 'react-hot-toast';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export default function OracleAI() {
  const [loading, setLoading] = useState(true);
  const [convictions, setConvictions] = useState([]);
  const theme = useStockStore(state => state.theme);

  const fetchConvictions = async () => {
    setLoading(true);
    try {
      const token = localStorage.getItem('mm_token') || localStorage.getItem('token');
      const res = await axios.get(`${API_URL}/api/oracle/portfolio-conviction`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      setConvictions(res.data);
    } catch (err) {
      toast.error(err.response?.data?.detail || "Deep analysis failed");
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchConvictions();
  }, []);

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-[70vh] gap-6 text-center">
        <Trophy size={64} className="text-accent animate-bounce" />
        <div>
            <h2 className="text-2xl font-bold text-dark-text">Summoning the Oracle</h2>
            <p className="text-dark-muted text-sm mt-1">Combining Fundamental Value with XGBoost Trend Analysis...</p>
        </div>
        <Loader size="lg" />
      </div>
    );
  }

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-10 animate-in fade-in duration-1000">
      
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-end justify-between gap-6 border-b border-dark-border pb-8">
        <div>
          <div className="flex items-center gap-2 mb-2">
             <span className="px-3 py-1 bg-accent/20 text-accent text-[10px] font-black uppercase tracking-widest rounded-full">Elite Tier AI</span>
             <span className="px-3 py-1 bg-yellow-500/10 text-yellow-500 text-[10px] font-black uppercase tracking-widest rounded-full flex items-center gap-1">
                <Sparkles size={10} /> Buffett Mode
             </span>
          </div>
          <h1 className="text-4xl font-black text-dark-text tracking-tighter flex items-center gap-4">
            <Shield className="text-accent" size={36} />
            The Buffett Oracle
          </h1>
          <p className="text-dark-muted mt-4 max-w-3xl leading-relaxed">
             Our most advanced engine. It acts as an <b>Elite Portfolio Manager</b> filtering your portfolio for institutional quality (High ROE, Low Debt) and combining it with <b>XGBoost gradient boosting</b> to predict 30-day alpha breakouts.
          </p>
        </div>
        <button 
           onClick={fetchConvictions}
           className="px-8 py-3 bg-white text-black hover:bg-gray-200 transition-all font-black uppercase tracking-widest text-xs rounded-none"
        >
           Deep Rescan
        </button>
      </div>

      {/* Conviction Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
        {convictions.map((item, idx) => (
          <div key={item.symbol} className="bg-dark-card border-l-4 border-accent p-8 rounded-r-2xl border-y border-r border-dark-border shadow-2xl relative overflow-hidden group hover:bg-accent/[0.02] transition-all">
            <div className="absolute top-0 right-0 p-4 opacity-10 group-hover:opacity-100 transition-opacity">
               <Trophy size={48} className="text-accent" />
            </div>
            
            <div className="flex items-start justify-between mb-8">
              <div>
                 <h3 className="text-3xl font-black text-dark-text">{item.symbol}</h3>
                 <div className="flex items-center gap-2 mt-1">
                    <div className="h-2 w-2 rounded-full bg-signal-buy animate-pulse" />
                    <span className="text-xs font-bold text-dark-muted uppercase tracking-wider">{item.quality_grade} Quality Grade</span>
                 </div>
              </div>
              <div className="text-right">
                 <p className="text-[10px] font-bold text-dark-muted uppercase tracking-widest mb-1">Oracle Conviction</p>
                 <p className="text-4xl font-black text-accent">{item.conviction_score}%</p>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-6 mb-8 border-y border-dark-border py-6">
              <div>
                 <p className="text-[10px] font-bold text-dark-muted uppercase tracking-widest mb-1">XGBoost 30D Target</p>
                 <p className={`text-xl font-bold ${item.projected_30d_return > 0 ? 'text-signal-buy' : 'text-signal-sell'}`}>
                    {item.projected_30d_return > 0 ? '+' : ''}{item.projected_30d_return}%
                 </p>
              </div>
              <div>
                 <p className="text-[10px] font-bold text-dark-muted uppercase tracking-widest mb-1">Buffett Checklist</p>
                 <p className="text-xl font-bold text-dark-text">Passed</p>
              </div>
            </div>

            <div className="space-y-2">
               <p className="text-[10px] font-black text-accent uppercase tracking-widest flex items-center gap-2">
                  <Brain size={12} /> Institutional Insights
               </p>
               <div className="flex flex-wrap gap-2">
                  {item.buffett_insights.map((msg, i) => (
                    <div key={i} className="bg-dark-bg/50 border border-dark-border px-3 py-1.5 rounded-lg text-xs text-dark-muted flex items-center gap-2">
                       <Zap size={10} className="text-yellow-500" /> {msg}
                    </div>
                  ))}
               </div>
            </div>
          </div>
        ))}
      </div>

      {/* Logic Explained */}
      <div className="bg-dark-card border border-dark-border p-10 rounded-3xl relative overflow-hidden">
        <div className="absolute top-0 left-0 w-1 h-full bg-gradient-to-b from-accent to-transparent" />
        <h3 className="text-xl font-bold text-dark-text mb-6 flex items-center gap-3">
           <Info size={24} className="text-accent" />
           The "Buffett Oracle" Methodology
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
           <div className="space-y-3">
              <h4 className="font-bold text-dark-text flex items-center gap-2 lowercase italic">#01 Quality Filter</h4>
              <p className="text-sm text-dark-muted leading-relaxed">
                 Like the Sage of Omaha, we start by filtering for businesses with high **Return on Equity (ROE)** and strong operational moats. We penalize high leverage and unproven margins.
              </p>
           </div>
           <div className="space-y-3">
              <h4 className="font-bold text-dark-text flex items-center gap-2 lowercase italic">#02 ML Conviction</h4>
              <p className="text-sm text-dark-muted leading-relaxed">
                 We feed 10 years of price action into a **XGBoost Regressor**. It detects non-linear relationships between volatility and moving average deviations to find momentum breakouts.
              </p>
           </div>
           <div className="space-y-3">
              <h4 className="font-bold text-dark-text flex items-center gap-2 lowercase italic">#03 Short & Long Term</h4>
              <p className="text-sm text-dark-muted leading-relaxed">
                 By combining technical "burst" signals with fundamental "anchor" metrics, we provide a conviction score that works for both active swing trading and long-term wealth compounding.
              </p>
           </div>
        </div>
      </div>
    </div>
  );
}
