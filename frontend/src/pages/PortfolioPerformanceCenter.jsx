import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Trophy, TrendingUp, TrendingDown, BarChart2, Calendar, Globe, Star, 
  ArrowUpRight, ArrowDownRight, Info, Shield, Zap, Flame, AlertTriangle, Eye, EyeOff,
  Sparkles, X, Send, ChevronRight, Brain
} from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import Loader from '../components/Loader';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const PerformancePage = () => {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);
    const [error, setError] = useState(null);
    const [exchangeTab, setExchangeTab] = useState('nse'); // 'nse' or 'bse'
    const [showAmounts, setShowAmounts] = useState(false);
    const [aiPanel, setAiPanel] = useState({ open: false, symbol: '', companyName: '' });
    const [chatMessages, setChatMessages] = useState([]);
    const [chatLoading, setChatLoading] = useState(false);
    const [chatInput, setChatInput] = useState('');

    const [explanations, setExplanations] = useState({});   // key: `${symbol}_${period}`
    const [loadingExpl,  setLoadingExpl]  = useState({});
    const [explModal,    setExplModal]    = useState(null);  // holds full explanation object
    const navigate = useNavigate();

    const openAI = (symbol, companyName, cardTitle, gain) => {
        const question = `I am looking at ${companyName} (NSE: ${symbol}), which is showing ${gain >= 0 ? '+' : ''}${gain}% in the '${cardTitle}' period. Why has it moved this much? Is this a good entry or exit point right now? Are there any known news catalysts?`;
        const initMessages = [{ role: 'user', content: question }];
        setAiPanel({ open: true, symbol, companyName: companyName || symbol });
        setChatMessages(initMessages);
        setChatInput('');
        _sendChat(symbol, initMessages);
    };

    const _sendChat = async (symbol, messages) => {
        setChatLoading(true);
        try {
            const res = await axios.post(
                `${API_URL}/api/stock/${symbol}/chart_chat`,
                { messages },
                { headers: { 'Authorization': `Bearer ${getToken()}` } }
            );
            setChatMessages(prev => [...prev, { role: 'assistant', content: res.data.reply || 'No response.' }]);
        } catch {
            setChatMessages(prev => [...prev, { role: 'assistant', content: 'AI unavailable. Please try again.' }]);
        } finally {
            setChatLoading(false);
        }
    };

    const fetchMoveExplanation = async (symbol, period, gainPct) => {
      const key = `${symbol}_${period}`;
      if (loadingExpl[key] || explanations[key]) return;
      setLoadingExpl(prev => ({ ...prev, [key]: true }));
      try {
        const res = await axios.get(`${API_URL}/api/stock/${symbol}/move-explanation`, {
          params: { period, gain_pct: gainPct },
          headers: { 'Authorization': `Bearer ${getToken()}` }
        });
        setExplanations(prev => ({ ...prev, [key]: res.data }));
      } catch (e) {
        console.warn('Move explanation failed:', e);
      } finally {
        setLoadingExpl(prev => ({ ...prev, [key]: false }));
      }
    };

    const handleSend = (text) => {
        const msg = text || chatInput;
        if (!msg.trim() || chatLoading) return;
        const next = [...chatMessages, { role: 'user', content: msg }];
        setChatMessages(next);
        setChatInput('');
        _sendChat(aiPanel.symbol, next);
    };

    const getToken = () => localStorage.getItem('mm_token') || localStorage.getItem('token');

    const fetchData = async (refresh = false) => {
        if (refresh) setRefreshing(true);
        else setLoading(true);
        try {
            const res = await axios.get(`${API_URL}/api/portfolio-performance/summary?refresh=${refresh}`, {
                headers: { 'Authorization': `Bearer ${getToken()}` }
            });
            if (res.data.error) setError(res.data.error);
            else setData(res.data);
        } catch (err) {
            setError("Failed to fetch performance summary.");
        } finally {
            setLoading(false);
            setRefreshing(false);
        }
    };

    useEffect(() => {
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
                    <div 
                        key={i} 
                        onClick={() => navigate(`/stock/${s.symbol}`)}
                        className="flex items-center justify-between group/row cursor-pointer hover:bg-dark-bg/40 p-2 -mx-2 rounded-xl transition-all"
                    >
                        <div className="flex flex-col">
                            <span className="text-sm font-black text-white group-hover/row:text-accent transition-colors tracking-tight">
                                {s.name || s.symbol}
                            </span>
                            <span className="text-[10px] text-dark-muted font-bold uppercase">{s.symbol}</span>
                        </div>
                        <div className="flex items-center gap-1.5">
                            <div className={`flex items-center gap-1 font-black ${s.gain >= 0 ? 'text-signal-buy' : 'text-signal-sell'}`}>
                                <span className="text-lg">{s.gain > 0 ? '+' : ''}{s.gain}%</span>
                                {s.gain >= 0 ? <ArrowUpRight size={16}/> : <ArrowDownRight size={16}/>}
                            </div>
                            <button
                                onClick={(e) => { e.stopPropagation(); openAI(s.symbol, s.name || s.symbol, title, s.gain); }}
                                className="p-1 rounded-lg text-dark-muted hover:text-accent hover:bg-accent/10 transition-all opacity-0 group-hover/row:opacity-100"
                                title="Ask AI"
                            >
                                <Sparkles size={13}/>
                            </button>
                        </div>
                    </div>
                )) : (
                    <p className="text-xs text-dark-muted italic">No qualifying data in period</p>
                )}
            </div>
        </div>
    );

    // Red-themed card for worst performers
    const LoserCard = ({ title, symbols, icon: Icon }) => (
        <div className="bg-dark-card border border-signal-sell/20 rounded-3xl p-6 transition-all hover:border-signal-sell/40 group">
            <div className="flex items-center justify-between mb-6">
                <div className="flex items-center gap-3">
                    <div className="p-2.5 rounded-xl bg-signal-sell/10 text-signal-sell">
                        <Icon size={20} />
                    </div>
                    <h4 className="font-black text-dark-text text-sm uppercase tracking-wider">{title}</h4>
                </div>
            </div>
            <div className="space-y-4">
                {symbols && symbols.length > 0 ? symbols.map((s, i) => (
                    <div 
                        key={i} 
                        onClick={() => navigate(`/stock/${s.symbol}`)}
                        className="flex items-center justify-between group/row cursor-pointer hover:bg-signal-sell/5 p-2 -mx-2 rounded-xl transition-all"
                    >
                        <div className="flex flex-col">
                            <span className="text-sm font-black text-white group-hover/row:text-signal-sell transition-colors tracking-tight">
                                {s.name || s.symbol}
                            </span>
                            <span className="text-[10px] text-dark-muted font-bold uppercase">{s.symbol}</span>
                        </div>
                        <div className="flex items-center gap-1.5">
                            <div className="flex items-center gap-1 font-black text-signal-sell">
                                <span className="text-lg">{s.gain > 0 ? '+' : ''}{s.gain}%</span>
                                <ArrowDownRight size={16}/>
                            </div>
                            <button
                                onClick={(e) => { e.stopPropagation(); openAI(s.symbol, s.name || s.symbol, title, s.gain); }}
                                className="p-1 rounded-lg text-dark-muted hover:text-signal-sell hover:bg-signal-sell/10 transition-all opacity-0 group-hover/row:opacity-100"
                                title="Ask AI"
                            >
                                <Sparkles size={13}/>
                            </button>
                        </div>
                    </div>
                )) : (
                    <p className="text-xs text-dark-muted italic">No qualifying data in period</p>
                )}
            </div>
        </div>
    );

    return (
        <>
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
                <div className="flex flex-wrap gap-3">
                   <button 
                        onClick={() => fetchData(true)}
                        disabled={refreshing}
                        className="px-5 py-2.5 bg-dark-card border border-dark-border rounded-2xl flex items-center gap-2 hover:border-accent/40 transition-all disabled:opacity-50"
                   >
                        <Zap size={16} className={refreshing ? "text-accent animate-pulse" : "text-dark-muted"} />
                        <span className="text-xs font-black text-dark-text uppercase tracking-widest">
                            {refreshing ? 'Syncing...' : 'Regenerate Analysis'}
                        </span>
                   </button>
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
                    <div className="flex items-center gap-4">
                        <button
                            onClick={() => setShowAmounts(!showAmounts)}
                            className="flex items-center gap-2 px-3 py-1.5 bg-dark-bg/60 border border-dark-border rounded-xl text-dark-muted hover:text-white hover:border-accent/40 transition-all"
                            title={showAmounts ? "Hide Amounts" : "Show Amounts"}
                        >
                            {showAmounts ? <EyeOff size={16} /> : <Eye size={16} />}
                            <span className="text-[10px] font-black uppercase tracking-widest">{showAmounts ? 'Mask' : 'Reveal'}</span>
                        </button>
                        <span className="text-[10px] font-black text-accent uppercase tracking-[0.2em] bg-accent/10 px-3 py-1 rounded-full border border-accent/20 italic">Unified Returns Since 2021</span>
                    </div>
                </div>
                <div className="p-8">
                    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-7 gap-4">
                        {data.yoy_growth.map((y, i) => (
                            <div key={i} className="flex flex-col p-5 rounded-[2rem] bg-dark-bg/40 border border-dark-border/50 hover:border-accent/30 transition-all text-center">
                                <span className="text-[10px] font-black text-dark-muted uppercase tracking-widest mb-2">{y.year}</span>
                                <span className={`text-xl font-black ${y.growth >= 0 ? 'text-signal-buy' : 'text-signal-sell'}`}>
                                    {y.growth > 0 ? '+' : ''}{y.growth}%
                                    <span className="text-[8px] font-bold block text-dark-muted mt-0.5 uppercase tracking-widest italic opacity-60">Annual Delta</span>
                                </span>
                                <div className="mt-4 flex flex-col gap-1.5 p-3 rounded-2xl bg-dark-bg/60 border border-dark-border/30">
                                    <div className="flex items-center justify-between">
                                        <span className="text-[8px] font-black text-dark-muted uppercase font-mono">Gain</span>
                                        <span className={`text-[10px] font-black ${y.profit >=0 ? 'text-signal-buy' : 'text-signal-sell'}`}>
                                            {showAmounts ? `₹${Math.round(y.profit).toLocaleString()}` : "••••••"}
                                        </span>
                                    </div>
                                    <div className="flex items-center justify-between border-t border-dark-border/40 pt-1.5">
                                        <span className="text-[8px] font-black text-accent uppercase italic">Total ROI %</span>
                                        <span className="text-[10px] font-black text-white">
                                            {y.cumulative_roi > 0 ? '+' : ''}{y.cumulative_roi}%
                                        </span>
                                    </div>
                                </div>
                                <div className={`w-8 h-1 mx-auto mt-3 rounded-full ${y.growth >= 0 ? 'bg-signal-buy/30' : 'bg-signal-sell/30'}`} />
                            </div>
                        ))}
                        
                        {/* Total Profit Column */}
                        <div className="flex flex-col p-5 rounded-[2rem] bg-accent/10 border border-accent/30 shadow-lg shadow-accent/5 transition-all text-center min-w-[140px]">
                            <span className="text-[9px] font-black text-accent uppercase tracking-widest mb-2">Unrealized P&L</span>
                            <span className="text-xl font-black text-white italic truncate">
                                {showAmounts ? `₹${Math.round(data.grand_total_profit).toLocaleString()}` : "••••••••"}
                            </span>
                            <span className={`text-[10px] font-black mt-1 ${data.grand_total_roi >= 0 ? 'text-signal-buy' : 'text-signal-sell'}`}>
                                {data.grand_total_roi > 0 ? '+' : ''}{data.grand_total_roi}% Total
                            </span>
                            <div className="flex items-center justify-center gap-1 mt-3">
                                <TrendingUp size={12} className="text-accent" />
                                <span className="text-[9px] font-black text-accent uppercase tracking-widest">Lifetime Pulse</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* ── Portfolio Winners ──────────────────────────────── */}
            <div className="space-y-6">
                <div className="flex items-center gap-3">
                    <Star className="text-accent" size={24} />
                    <h2 className="text-2xl font-black text-white italic uppercase tracking-tight">Portfolio Winners</h2>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-6">
                    <PerformerCard title="Top This Week"  symbols={data.portfolio_performers?.week}  icon={Zap}    color="text-amber-500" />
                    <PerformerCard title="Top This Month" symbols={data.portfolio_performers?.month} icon={Flame}  color="text-orange-500" />
                    <PerformerCard title="Top 52 Weeks"   symbols={data.portfolio_performers?.year}  icon={Trophy} color="text-accent" />
                    <PerformerCard title="Top YTD"        symbols={data.portfolio_performers?.ytd}   icon={TrendingUp} color="text-emerald-500" />
                </div>
            </div>

            {/* ── Portfolio Losers ───────────────────────────────── */}
            <div className="space-y-6">
                <div className="flex items-center gap-3">
                    <AlertTriangle className="text-signal-sell" size={24} />
                    <h2 className="text-2xl font-black text-white italic uppercase tracking-tight">Portfolio Worst Performers</h2>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-6">
                    <LoserCard title="Worst This Week"  symbols={data.portfolio_losers?.week}  icon={TrendingDown} />
                    <LoserCard title="Worst This Month" symbols={data.portfolio_losers?.month} icon={TrendingDown} />
                    <LoserCard title="Worst 52 Weeks"   symbols={data.portfolio_losers?.year}  icon={TrendingDown} />
                    <LoserCard title="Worst YTD"        symbols={data.portfolio_losers?.ytd}   icon={TrendingDown} />
                </div>
            </div>

            {/* ── Global Market Leaders ──────────────────────────── */}
            <div className="space-y-6">
                <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                    <div className="flex items-center gap-3">
                        <Globe className="text-accent" size={24} />
                        <h2 className="text-2xl font-black text-white italic uppercase tracking-tight">Market Leaders (Global)</h2>
                    </div>
                    <div className="flex bg-dark-bg/60 p-1 rounded-xl border border-dark-border">
                        {['nse', 'bse'].map(ex => (
                            <button
                                key={ex}
                                onClick={() => setExchangeTab(ex)}
                                className={`px-4 py-1.5 rounded-lg text-[10px] font-black uppercase tracking-widest transition-all ${
                                    exchangeTab === ex ? 'bg-accent text-white shadow-lg' : 'text-dark-muted hover:text-white'
                                }`}
                            >
                                {ex}
                            </button>
                        ))}
                    </div>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-6">
                    <PerformerCard title={`Top ${exchangeTab.toUpperCase()} (Week)`}  symbols={data.market_leaders?.week?.[exchangeTab]}  icon={TrendingUp} color="text-signal-buy" />
                    <PerformerCard title={`Top ${exchangeTab.toUpperCase()} (Month)`} symbols={data.market_leaders?.month?.[exchangeTab]} icon={Zap}        color="text-amber-500" />
                    <PerformerCard title={`Top ${exchangeTab.toUpperCase()} (52W)`}   symbols={data.market_leaders?.year?.[exchangeTab]}  icon={Star}       color="text-purple-500" />
                    <PerformerCard title={`Top ${exchangeTab.toUpperCase()} (YTD)`}   symbols={data.market_leaders?.ytd?.[exchangeTab]}   icon={Trophy}     color="text-accent" />
                </div>
            </div>

            {/* ── Market Laggards ────────────────────────────────── */}
            <div className="space-y-6">
                <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                    <div className="flex items-center gap-3">
                        <AlertTriangle className="text-signal-sell" size={24} />
                        <h2 className="text-2xl font-black text-white italic uppercase tracking-tight">Market Laggards (Global)</h2>
                    </div>
                    <div className="flex bg-dark-bg/60 p-1 rounded-xl border border-dark-border">
                        {['nse', 'bse'].map(ex => (
                            <button
                                key={ex}
                                onClick={() => setExchangeTab(ex)}
                                className={`px-4 py-1.5 rounded-lg text-[10px] font-black uppercase tracking-widest transition-all ${
                                    exchangeTab === ex ? 'bg-signal-sell text-white shadow-lg' : 'text-dark-muted hover:text-white'
                                }`}
                            >
                                {ex}
                            </button>
                        ))}
                    </div>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-6">
                    <LoserCard title={`Worst ${exchangeTab.toUpperCase()} (Week)`}  symbols={data.market_laggards?.week?.[exchangeTab]}  icon={TrendingDown} />
                    <LoserCard title={`Worst ${exchangeTab.toUpperCase()} (Month)`} symbols={data.market_laggards?.month?.[exchangeTab]} icon={TrendingDown} />
                    <LoserCard title={`Worst ${exchangeTab.toUpperCase()} (52W)`}   symbols={data.market_laggards?.year?.[exchangeTab]}  icon={TrendingDown} />
                    <LoserCard title={`Worst ${exchangeTab.toUpperCase()} (YTD)`}   symbols={data.market_laggards?.ytd?.[exchangeTab]}   icon={TrendingDown} />
                </div>
            </div>
        </div>

            {/* ── Floating AI Chat Panel ────────────────────────── */}
            {aiPanel.open && (
                <div className="fixed bottom-6 right-6 z-50 w-[380px] h-[520px] bg-dark-card border border-accent/30 rounded-3xl shadow-2xl shadow-accent/10 flex flex-col overflow-hidden animate-in slide-in-from-bottom-4 duration-300">
                    {/* Panel Header */}
                    <div className="flex items-center justify-between px-5 py-4 border-b border-dark-border bg-gradient-to-r from-accent/10 to-transparent flex-shrink-0">
                        <div className="flex items-center gap-3">
                            <div className="p-1.5 bg-accent/20 rounded-xl">
                                <Sparkles size={16} className="text-accent" />
                            </div>
                            <div>
                                <p className="text-sm font-black text-white tracking-tight">{aiPanel.companyName || aiPanel.symbol}</p>
                                <p className="text-[10px] text-dark-muted font-bold uppercase tracking-widest font-mono">{aiPanel.symbol} · AI Research Assistant</p>
                            </div>
                        </div>
                        <button
                            onClick={() => setAiPanel({ open: false, symbol: '' })}
                            className="p-1.5 rounded-xl text-dark-muted hover:text-white hover:bg-dark-border/60 transition-all"
                        >
                            <X size={16} />
                        </button>
                    </div>

                    {/* Messages */}
                    <div className="flex-1 overflow-y-auto px-4 py-3 space-y-3 scroll-smooth">
                        {chatMessages.map((m, i) => (
                            <div key={i} className={`flex ${m.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                                <div className={`max-w-[85%] px-4 py-2.5 rounded-2xl text-[12px] leading-relaxed font-medium ${
                                    m.role === 'user'
                                        ? 'bg-accent text-white rounded-br-sm'
                                        : 'bg-dark-bg/80 text-dark-text border border-dark-border rounded-bl-sm'
                                }`}>
                                    {m.content}
                                </div>
                            </div>
                        ))}
                        {chatLoading && (
                            <div className="flex justify-start">
                                <div className="bg-dark-bg/80 border border-dark-border rounded-2xl rounded-bl-sm px-4 py-3">
                                    <div className="flex gap-1.5">
                                        <span className="w-1.5 h-1.5 bg-accent rounded-full animate-bounce" style={{animationDelay:'0ms'}}/>
                                        <span className="w-1.5 h-1.5 bg-accent rounded-full animate-bounce" style={{animationDelay:'150ms'}}/>
                                        <span className="w-1.5 h-1.5 bg-accent rounded-full animate-bounce" style={{animationDelay:'300ms'}}/>
                                    </div>
                                </div>
                            </div>
                        )}
                    </div>

                    {/* Quick Chips */}
                    {!chatLoading && (
                        <div className="px-4 py-2 flex flex-wrap gap-1.5 border-t border-dark-border/40 flex-shrink-0">
                            {['Should I take profit?', 'Is this overbought?', 'Key support/resistance?', 'Any news catalyst?'].map(chip => (
                                <button
                                    key={chip}
                                    onClick={() => handleSend(chip)}
                                    className="flex items-center gap-1 px-2.5 py-1 bg-dark-bg/60 border border-dark-border hover:border-accent/50 hover:text-accent rounded-full text-[10px] font-bold text-dark-muted transition-all"
                                >
                                    <ChevronRight size={10}/>{chip}
                                </button>
                            ))}
                        </div>
                    )}

                    {/* Input */}
                    <div className="px-4 py-3 border-t border-dark-border flex-shrink-0">
                        <div className="flex gap-2">
                            <input
                                value={chatInput}
                                onChange={e => setChatInput(e.target.value)}
                                onKeyDown={e => e.key === 'Enter' && handleSend()}
                                placeholder="Ask anything about this stock..."
                                className="flex-1 bg-dark-bg/60 border border-dark-border rounded-2xl px-4 py-2 text-[12px] text-dark-text placeholder:text-dark-muted/50 focus:outline-none focus:border-accent/50 transition-all"
                                disabled={chatLoading}
                            />
                            <button
                                onClick={() => handleSend()}
                                disabled={chatLoading || !chatInput.trim()}
                                className="p-2.5 bg-accent rounded-2xl text-white hover:bg-accent/80 transition-all disabled:opacity-40 disabled:cursor-not-allowed flex-shrink-0"
                            >
                                <Send size={14}/>
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </>
    );
};

export default PerformancePage;
