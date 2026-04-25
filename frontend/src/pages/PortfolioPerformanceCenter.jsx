import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { 
  Trophy, TrendingUp, BarChart2, Calendar, Globe, Star, 
  ArrowUpRight, ArrowDownRight, Info, Shield, Zap, Flame
} from 'lucide-react';
import Loader from '../components/Loader';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const PerformancePage = () => {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    const getToken = () => localStorage.getItem('mm_token') || localStorage.getItem('token');

    useEffect(() => {
        const fetchData = async () => {
            try {
                const res = await axios.get(`${API_URL}/api/portfolio-performance/summary`, {
                    headers: { 'Authorization': `Bearer ${getToken()}` }
                });
                if (res.data.error) setError(res.data.error);
                else setData(res.data);
            } catch (err) {
                setError("Failed to fetch performance summary.");
            } finally {
                setLoading(false);
            }
        };
        fetchData();
    }, []);

    if (loading) return (
        <div className="flex flex-col items-center justify-center h-[70vh] gap-6">
            <Trophy size={64} className="text-accent animate-bounce" />
            <Loader size="lg" />
            <p className="text-dark-muted font-black uppercase tracking-widest text-[10px]">Assembling Performance Reports...</p>
        </div>
    );

    if (error) return (
        <div className="p-12 text-center bg-rose-500/5 border border-rose-500/20 rounded-3xl mx-6">
            <Info size={48} className="text-rose-500 mx-auto mb-4 opacity-50" />
            <h3 className="text-xl font-bold text-dark-text mb-2">Analysis Failed</h3>
            <p className="text-dark-muted">{error}</p>
        </div>
    );

    const PerformerCard = ({ title, symbols, icon: Icon, color }) => (
        <div className="bg-dark-card border border-dark-border rounded-3xl p-6 transition-all hover:border-accent/40 group">
            <div className="flex items-center justify-between mb-6">
                <div className="flex items-center gap-3">
                    <div className={`p-2.5 rounded-xl ${color} bg-opacity-10 text-opacity-90`}>
                        <Icon size={20} />
                    </div>
                    <h4 className="font-black text-dark-text text-sm uppercase tracking-wider">{title}</h4>
                </div>
            </div>
            <div className="space-y-4">
                {symbols && symbols.length > 0 ? symbols.map((s, i) => (
                    <div key={i} className="flex items-center justify-between group/row">
                        <div className="flex flex-col">
                            <span className="text-base font-black text-white group-hover/row:text-accent transition-colors tracking-tight">{s.symbol}</span>
                            {s.exchange && <span className="text-[10px] text-dark-muted font-bold uppercase">{s.exchange}</span>}
                        </div>
                        <div className={`flex items-center gap-1 font-black ${s.gain >= 0 ? 'text-signal-buy' : 'text-signal-sell'}`}>
                            <span className="text-lg">{s.gain > 0 ? '+' : ''}{s.gain}%</span>
                            {s.gain >= 0 ? <ArrowUpRight size={16}/> : <ArrowDownRight size={16}/>}
                        </div>
                    </div>
                )) : (
                    <p className="text-xs text-dark-muted italic">No qualifying data in period</p>
                )}
            </div>
        </div>
    );

    return (
        <div className="p-6 max-w-7xl mx-auto space-y-12 animate-in fade-in duration-700">
            {/* Header */}
            <div className="flex flex-col md:flex-row md:items-center justify-between gap-6">
                <div>
                   <h1 className="text-4xl font-black text-dark-text tracking-tighter flex items-center gap-4 italic uppercase">
                        <Trophy className="text-accent" size={40} />
                        Performance Center
                   </h1>
                   <p className="text-dark-muted text-sm font-bold mt-2 flex items-center gap-2">
                        <Calendar size={14} className="text-accent" />
                        Analysis as of {data.analysis_date}
                   </p>
                </div>
                <div className="flex gap-3">
                   <div className="px-5 py-2.5 bg-accent/10 border border-accent/20 rounded-2xl flex items-center gap-2">
                        <Shield size={16} className="text-accent" />
                        <span className="text-xs font-black text-dark-text uppercase tracking-widest">Portfolio Tracking</span>
                   </div>
                </div>
            </div>

            {/* YoY Growth Table */}
            <div className="bg-dark-card border border-dark-border rounded-[2.5rem] overflow-hidden shadow-2xl">
                <div className="px-8 py-6 border-b border-dark-border bg-dark-bg/50 flex items-center justify-between">
                    <h3 className="text-lg font-black text-white flex items-center gap-3">
                        <BarChart2 size={22} className="text-accent" />
                        ANNUAL PORTFOLIO GROWTH
                    </h3>
                    <span className="text-[10px] font-black text-accent uppercase tracking-[0.2em] bg-accent/10 px-3 py-1 rounded-full border border-accent/20">Unified Returns Since 2021</span>
                </div>
                <div className="p-8">
                    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-6">
                        {data.yoy_growth.map((y, i) => (
                            <div key={i} className="flex flex-col p-6 rounded-[2rem] bg-dark-bg/40 border border-dark-border/50 hover:border-accent/30 transition-all text-center">
                                <span className="text-[10px] font-black text-dark-muted uppercase tracking-widest mb-2">{y.year}</span>
                                <span className={`text-2xl font-black ${y.growth >= 0 ? 'text-signal-buy' : 'text-signal-sell'}`}>
                                    {y.growth > 0 ? '+' : ''}{y.growth}%
                                </span>
                                <div className={`w-8 h-1 mx-auto mt-3 rounded-full ${y.growth >= 0 ? 'bg-signal-buy/30' : 'bg-signal-sell/30'}`} />
                            </div>
                        ))}
                    </div>
                </div>
            </div>

            {/* Portfolio Best Performers */}
            <div className="space-y-6">
                <div className="flex items-center gap-3">
                    <Star className="text-accent" size={24} />
                    <h2 className="text-2xl font-black text-white italic uppercase tracking-tight">Portfolio Winners</h2>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                    <PerformerCard title="Top This Week" symbols={data.portfolio_performers.week} icon={Zap} color="text-amber-500" />
                    <PerformerCard title="Top This Month" symbols={data.portfolio_performers.month} icon={Flame} color="text-orange-500" />
                    <PerformerCard title="Top This Year" symbols={data.portfolio_performers.year} icon={Trophy} color="text-accent" />
                </div>
            </div>

            {/* Global Market Leaders */}
            <div className="space-y-6">
                <div className="flex items-center gap-3">
                    <Globe className="text-accent" size={24} />
                    <h2 className="text-2xl font-black text-white italic uppercase tracking-tight">Market Leaders (Global)</h2>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                    <PerformerCard title="Market Top (Week)" symbols={data.market_leaders.week} icon={TrendingUp} color="text-signal-buy" />
                    <PerformerCard title="Market Top (Month)" symbols={data.market_leaders.month} icon={Zap} color="text-amber-500" />
                    <PerformerCard title="Market Top (Year)" symbols={data.market_leaders.year} icon={Star} color="text-purple-500" />
                </div>
            </div>
        </div>
    );
};

export default PerformancePage;
