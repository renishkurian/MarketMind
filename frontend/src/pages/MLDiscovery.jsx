import React, { useEffect, useState } from 'react';
import { useStockStore } from '../store/stockStore';
import MetricCard from '../components/MetricCard';
import Loader from '../components/Loader';
import { Brain, TrendingUp, Shield, BarChart2, Zap, ArrowRight, Target } from 'lucide-react';
import axios from 'axios';
import toast from 'react-hot-toast';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export default function MLDiscovery() {
  const [loading, setLoading] = useState(true);
  const [predictions, setPredictions] = useState([]);
  const theme = useStockStore(state => state.theme);

  const fetchMLData = async () => {
    setLoading(true);
    try {
      const token = localStorage.getItem('mm_token') || localStorage.getItem('token');
      const res = await axios.get(`${API_URL}/api/ml/portfolio-alpha`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      setPredictions(res.data);
    } catch (err) {
      toast.error("Failed to fetch ML insights");
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchMLData();
  }, []);

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-[70vh] gap-6">
        <div className="relative">
             <Brain size={64} className="text-accent animate-pulse" />
             <Zap size={24} className="text-yellow-400 absolute -top-1 -right-1 animate-bounce" />
        </div>
        <div className="text-center">
            <h2 className="text-xl font-bold text-dark-text">Synthesizing Alpha Predictions</h2>
            <p className="text-dark-muted text-sm mt-1">Training Random Forest regressors on your portfolio history...</p>
        </div>
        <Loader size="lg" />
      </div>
    );
  }

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-8 animate-in fade-in duration-700">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-dark-text tracking-tight flex items-center gap-3">
            <Brain className="text-accent" size={32} />
            QuantDiscovery <span className="text-xs bg-accent/20 text-accent px-2 py-0.5 rounded-full uppercase tracking-widest font-black">AI V3.0</span>
          </h1>
          <p className="text-dark-muted mt-2 max-w-2xl">
            Proprietary Machine Learning models analyzing 500+ historical data points per stock to detect subtle momentum patterns and project short-term alpha.
          </p>
        </div>
        <button 
          onClick={fetchMLData}
          className="flex items-center gap-2 px-5 py-2.5 bg-dark-card border border-dark-border rounded-xl hover:bg-accent/10 hover:border-accent/50 transition-all text-sm font-semibold text-dark-text"
        >
          <BarChart2 size={18} />
          Retrain Models
        </button>
      </div>

      {/* Top Gainers predicted */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {predictions.map((pred, idx) => (
          <div 
            key={pred.symbol}
            className="bg-dark-card border border-dark-border p-6 rounded-2xl relative overflow-hidden group hover:border-accent/30 transition-all shadow-xl"
          >
            {/* Background Glow */}
            <div className="absolute -right-8 -top-8 w-24 h-24 bg-accent/5 rounded-full blur-2xl group-hover:bg-accent/10 transition-colors" />
            
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
                 <button className="p-2 hover:bg-accent/10 rounded-lg transition-colors text-dark-muted hover:text-accent">
                    <ArrowRight size={16} />
                 </button>
              </div>
            </div>
          </div>
        ))}
      </div>

      {!loading && predictions.length === 0 && (
        <div className="py-20 flex flex-col items-center justify-center text-center space-y-4 bg-dark-card rounded-3xl border border-dashed border-dark-border">
           <div className="p-4 bg-accent/5 rounded-full">
              <Shield className="text-accent/30" size={48} />
           </div>
           <div>
              <p className="text-dark-text font-bold text-lg">No ML Alpha Found</p>
              <p className="text-dark-muted max-w-xs mx-auto text-sm">Add more stocks to your portfolio with at least 100 days of history to enable Discovery V3.</p>
           </div>
        </div>
      )}
    </div>
  );
}
