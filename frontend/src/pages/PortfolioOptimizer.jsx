import React, { useEffect, useState } from 'react';
import { useStockStore } from '../store/stockStore';
import Loader from '../components/Loader';
import { PieChart, TrendingUp, Shield, BarChart2, Zap, ArrowRight, Target, Info, Sparkles, Brain } from 'lucide-react';
import axios from 'axios';
import toast from 'react-hot-toast';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export default function PortfolioOptimizer() {
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState(null);
  const theme = useStockStore(state => state.theme);

  const fetchOptimization = async () => {
    setLoading(true);
    try {
      const token = localStorage.getItem('mm_token') || localStorage.getItem('token');
      const res = await axios.get(`${API_URL}/api/portfolio-opt/optimize`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      setData(res.data);
    } catch (err) {
      toast.error(err.response?.data?.detail || "Optimization failed");
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchOptimization();
  }, []);

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-[70vh] gap-6">
        <Sparkles size={64} className="text-accent animate-pulse" />
        <div className="text-center">
            <h2 className="text-xl font-bold text-dark-text">Calculating Efficient Frontier</h2>
            <p className="text-dark-muted text-sm mt-1">Solving Quadratic Programming for Max Sharpe Ratio...</p>
        </div>
        <Loader size="lg" />
      </div>
    );
  }

  if (!data) return null;

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-8 animate-in fade-in duration-700">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-dark-text tracking-tight flex items-center gap-3">
            <PieChart className="text-accent" size={32} />
            Institutional Optimizer
          </h1>
          <p className="text-dark-text/70 mt-2 max-w-4xl leading-relaxed text-sm lg:text-base">
            MarketMind uses <b>Modern Portfolio Theory (MPT)</b> to solve the complex mathematical problem of how to distribute your money. 
            We analyze 10 years of historical relationships between your stocks to find the <i>"Optimal Allocation"</i> that gives you the highest profit with the lowest possible stress (volatility).
          </p>
          <div className="flex flex-wrap gap-4 mt-3">
             <div className="flex items-center gap-1.5 text-[10px] font-bold text-signal-buy uppercase bg-signal-buy/10 px-2 py-0.5 rounded border border-signal-buy/20">
                <Shield size={10} /> Fully Diversified
             </div>
             <div className="flex items-center gap-1.5 text-[10px] font-bold text-accent uppercase bg-accent/10 px-2 py-0.5 rounded border border-accent/20">
                <Brain size={10} /> CAPM Model
             </div>
             <div className="flex items-center gap-1.5 text-[10px] font-bold text-amber-500 uppercase bg-amber-500/10 px-2 py-0.5 rounded border border-amber-500/20">
                <Zap size={10} /> Max efficiency
             </div>
          </div>
        </div>
        <button 
          onClick={fetchOptimization}
          className="bg-accent text-white px-6 py-2.5 rounded-xl font-bold shadow-lg shadow-accent/20 hover:bg-blue-600 transition-all flex items-center gap-2"
        >
          <BarChart2 size={18} />
          Recalculate
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-dark-card border border-dark-border p-6 rounded-2xl">
            <p className="text-[10px] font-bold text-dark-muted uppercase tracking-widest mb-1">Max Sharpe Ratio</p>
            <p className="text-3xl font-black text-accent">{data.metrics.sharpe_ratio}</p>
        </div>
        <div className="bg-dark-card border border-dark-border p-6 rounded-2xl">
            <p className="text-[10px] font-bold text-dark-muted uppercase tracking-widest mb-1">Exp. Annual Return</p>
            <p className="text-3xl font-black text-signal-buy">{data.metrics.expected_annual_return}%</p>
        </div>
        <div className="bg-dark-card border border-dark-border p-6 rounded-2xl">
            <p className="text-[10px] font-bold text-dark-muted uppercase tracking-widest mb-1">Annual Volatility</p>
            <p className="text-3xl font-black text-signal-sell">{data.metrics.annual_volatility}%</p>
        </div>
      </div>

      <div className="p-5 bg-dark-card/50 border border-dark-border/40 rounded-2xl flex items-start gap-4">
          <div className="p-2 bg-accent/10 rounded-lg text-accent shrink-0">
             <Target size={20} />
          </div>
          <div>
            <h4 className="text-base font-bold text-dark-text uppercase tracking-wider">The Strategic Goal</h4>
            <p className="text-sm text-dark-muted mt-1 leading-relaxed opacity-80">
               This profile assumes we want to <b>Maximize the Sharpe Ratio</b>. This means we are hunting for the highest possible return for <i>every single unit of risk</i> we take. The weights displayed below are the mathematically "Perfect" recipe to achieve this balance across your {data.symbols_analyzed?.length} assets.
            </p>
          </div>
      </div>

      <div className="bg-dark-card border border-dark-border rounded-2xl overflow-hidden">
        <div className="px-6 py-4 border-b border-dark-border bg-dark-bg/50">
            <h3 className="font-bold text-dark-text flex items-center gap-2">
                <Target size={18} className="text-accent" />
                Optimal Allocation Weights
            </h3>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 p-6 gap-4">
            {Object.entries(data.weights).map(([symbol, weight]) => (
                <div key={symbol} className={`p-4 rounded-xl border flex items-center justify-between ${weight > 0 ? 'border-accent/30 bg-accent/5' : 'border-dark-border bg-dark-bg/30 grayscale'}`}>
                    <div>
                        <p className="font-black text-dark-text">{symbol}</p>
                        <p className="text-[10px] text-dark-muted uppercase font-bold">{weight > 0 ? 'Invest' : 'Exclude'}</p>
                    </div>
                    <div className="text-right">
                        <p className={`text-xl font-black ${weight > 0 ? 'text-dark-text' : 'text-dark-muted opacity-30'}`}>{(weight * 100).toFixed(1)}%</p>
                    </div>
                </div>
            ))}
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="p-6 bg-signal-buy/5 border border-signal-buy/20 rounded-2xl">
            <h4 className="font-bold text-dark-text mb-3 flex items-center gap-2">
                <Info size={18} className="text-signal-buy" />
                What is Sharpe Ratio?
            </h4>
            <p className="text-sm text-dark-muted leading-relaxed">
                The Sharpe ratio measures the performance of an investment compared to a risk-free asset, after adjusting for its risk. A higher Sharpe ratio (usually &gt; 2.0) indicates that the portfolio has high returns without excessive volatility.
            </p>
        </div>
        <div className="p-6 bg-accent/5 border border-accent/20 rounded-2xl">
            <h4 className="font-bold text-dark-text mb-3 flex items-center gap-2">
                <Brain size={18} className="text-accent" />
                Optimization Logic
            </h4>
            <p className="text-sm text-dark-muted leading-relaxed">
                We use <b>Mean-Variance Optimization</b>. We analyze how your stocks move together (Covariance) to find the weights that produce the lowest possible risk for the highest possible return.
            </p>
        </div>
      </div>

      {/* ── Terminologies & Meanings ───────────────────────────────────── */}
      <div className="pt-12 border-t border-dark-border/40">
        <h2 className="text-xl font-bold text-dark-text mb-6 flex items-center gap-2">
            <Info size={20} className="text-accent" />
            Optimizer Glossary
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            <div className="space-y-2">
                <h4 className="text-xs font-bold text-accent uppercase tracking-widest">Sharpe Ratio</h4>
                <p className="text-xs text-dark-muted leading-relaxed">
                    Measured as (Return - RiskFreeRate) / Volatility. It tells us if your returns are due to smart investment decisions or excessive risk.
                </p>
            </div>
            <div className="space-y-2">
                <h4 className="text-xs font-bold text-accent uppercase tracking-widest">Max Sharpe Weight</h4>
                <p className="text-xs text-dark-muted leading-relaxed">
                    The specific allocation percentage that historical data suggests will give you the most "bang for your buck" (Highest Sharpe).
                </p>
            </div>
            <div className="space-y-2">
                <h4 className="text-xs font-bold text-accent uppercase tracking-widest">Annual Volatility</h4>
                <p className="text-xs text-dark-muted leading-relaxed">
                    The expected fluctuation in your portfolio's value over a year. Lower volatility means a smoother, less stressful investment ride.
                </p>
            </div>
            <div className="space-y-2">
                <h4 className="text-xs font-bold text-accent uppercase tracking-widest">Efficient Frontier</h4>
                <p className="text-xs text-dark-muted leading-relaxed">
                    A mathematical curve representing portfolios that have the maximum possible return for every given level of risk.
                </p>
            </div>
        </div>
      </div>
    </div>
  );
}
