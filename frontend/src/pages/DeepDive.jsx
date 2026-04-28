import React, { useRef,useEffect, useState, useCallback,useMemo } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useStockStore } from '../store/stockStore';
import {
  ArrowLeft, ArrowUpRight, ArrowDownRight, ArrowRight,
  Activity, Brain, TrendingUp, TrendingDown, Minus,
  RefreshCw, AlertTriangle, CheckCircle, BarChart2,
  Clock, Shield, Sun, Moon, Layers, BookOpen, Briefcase, Sparkles,
  Edit2, X, ShieldCheck,
  Flame,
  Zap,
  Star,
  Database,
  Bell, BellRing, PieChart, Users, TrendingUp as TrendUp
} from 'lucide-react';
import HistoricalPricesTable from '../components/HistoricalPricesTable';

const FiftyTwoWeekRange = ({ current, low, high }) => {
  if (!current || !low || !high) return null;
  const range = high - low;
  const progress = ((current - low) / range) * 100;
  const clampProgress = Math.min(100, Math.max(0, progress));

  return (
    <div className="space-y-1.5">
      <div className="flex justify-between text-[10px] font-bold text-dark-muted uppercase tracking-tighter">
        <span>52W Low (₹{low.toFixed(1)})</span>
        <span>52W High (₹{high.toFixed(1)})</span>
      </div>
      <div className="h-1.5 w-full bg-dark-bg rounded-full border border-dark-border relative overflow-hidden">
        <div 
          className="h-full bg-gradient-to-r from-signal-sell via-signal-hold to-signal-buy transition-all duration-1000"
          style={{ width: `${clampProgress}%` }}
        />
        <div 
          className="absolute top-0 bottom-0 w-1 bg-white shadow-[0_0_8px_rgba(255,255,255,0.8)] z-10"
          style={{ left: `calc(${clampProgress}% - 2px)` }}
        />
      </div>
      <p className="text-[10px] text-center text-dark-muted italic">
        Currently {progress.toFixed(1)}% above 52w low
      </p>
    </div>
  );
};

import toast from 'react-hot-toast';

import SignalBadge from '../components/SignalBadge';
import MetricCard from '../components/MetricCard';
import CandlestickChart from '../components/charts/CandlestickChart';
import VolumeChart from '../components/charts/VolumeChart';
import RSIChart from '../components/charts/RSIChart';
import MACDChart from '../components/charts/MACDChart';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const SKILLS = [
  { id: 'sebi_forensic', name: 'SEBI Forensic', icon: '🔍', desc: 'Accounting red flags & governance risks' },
  { id: 'warren_buffett_quality', name: 'Warren Buffett', icon: '🦅', desc: 'High ROE, low debt & sustainable moats' },
  { id: 'rj_india_growth', name: 'RJ India Cycle', icon: '🐂', desc: 'Macro cycle & sector tailwinds' },
  { id: 'sequoia_moat', name: 'Sequoia Moat', icon: '🌲', desc: 'Pricing power & scalability analysis' },
  { id: 'ark_disruptive', name: 'ARK Disruptive', icon: '🚀', desc: 'Exponential growth & disruptive tech' },
  { id: 'goldman_screener', name: 'Goldman Screener', icon: '📊', desc: 'Institutional screening & targets' },
  { id: 'peter_lynch_simple', name: 'Lynch Main St', icon: '🏠', desc: 'Consumer logic & retail insight' }
];

// ── AI Insight Panel ──────────────────────────────────────────────────────
const AIInsightPanel = ({ insight, loading, error }) => {
  if (loading) return (
    <div className="flex items-center justify-center py-16 gap-3 text-dark-muted">
      <RefreshCw size={18} className="animate-spin" />
      <span className="text-sm">Loading AI analysis…</span>
    </div>
  );
  if (error) return (
    <div className="flex flex-col items-center gap-4 py-16 justify-center text-dark-muted border-2 border-dashed border-dark-border rounded-2xl">
      <div className="p-3 bg-accent/5 rounded-full">
        <Brain size={24} className="text-accent/40" />
      </div>
      <div className="text-center">
        <p className="text-sm font-medium text-dark-text">No Intelligence Report Found</p>
        <p className="text-xs text-dark-muted mt-1 max-w-[200px] mx-auto">Click generate to start AI analysis for this symbol.</p>
      </div>
      <button
        onClick={() => window.dispatchEvent(new CustomEvent('generate-insight'))}
        className="px-4 py-2 bg-accent hover:bg-accent/80 text-white rounded-lg text-xs font-semibold transition-all shadow-lg shadow-accent/20"
      >
        Generate Report
      </button>
    </div>
  );
  if (!insight) return null;

  const sentimentColor = insight.sentiment_score > 0.5
    ? 'text-signal-buy' : insight.sentiment_score < 0
    ? 'text-signal-sell' : 'text-signal-hold';

  return (
    <div className="space-y-5">
      {/* Meta */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-xs text-dark-muted">
          <Clock size={13} />
          <span>Generated {new Date(insight.generated_at).toLocaleString()}</span>
          {insight.trigger_reason && (
            <span className="px-2 py-0.5 bg-accent/10 text-accent border border-accent/20 rounded text-[10px] font-mono uppercase ml-2">
              {insight.trigger_reason}
            </span>
          )}
        </div>
        {insight.sentiment_score != null && (
          <span className={`font-mono text-sm font-bold ${sentimentColor}`}>
            Sentiment: {(insight.sentiment_score > 0 ? '+' : '')}{insight.sentiment_score?.toFixed(2)}
          </span>
        )}
      </div>

      {/* Summary */}
      {insight.short_summary && (
        <div className="bg-accent/5 border border-accent/20 rounded-xl p-4">
          <p className="text-sm font-medium text-dark-text leading-relaxed">{insight.short_summary}</p>
        </div>
      )}
      {insight.long_summary && (
        <pre className="text-sm text-dark-muted leading-relaxed whitespace-pre-wrap font-sans">{insight.long_summary}</pre>
      )}

      {/* Opportunities & Risks */}
      <div className="grid grid-cols-2 gap-4">
        {insight.key_opportunities?.length > 0 && (
          <div className="bg-signal-buy/5 border border-signal-buy/20 rounded-xl p-4">
            <div className="flex items-center gap-2 mb-3">
              <CheckCircle size={14} className="text-signal-buy" />
              <span className="text-xs font-semibold text-signal-buy uppercase tracking-wide">Opportunities</span>
            </div>
            <ul className="space-y-2">
              {insight.key_opportunities.slice(0, 4).map((o, i) => (
                <li key={i} className="text-xs text-dark-text flex items-start gap-2">
                  <span className="text-signal-buy mt-0.5">↑</span>
                  <span>{o}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
        {insight.key_risks?.length > 0 && (
          <div className="bg-signal-sell/5 border border-signal-sell/20 rounded-xl p-4">
            <div className="flex items-center gap-2 mb-3">
              <AlertTriangle size={14} className="text-signal-sell" />
              <span className="text-xs font-semibold text-signal-sell uppercase tracking-wide">Risks</span>
            </div>
            <ul className="space-y-2">
              {insight.key_risks.slice(0, 4).map((r, i) => (
                <li key={i} className="text-xs text-dark-text flex items-start gap-2">
                  <span className="text-signal-sell mt-0.5">↓</span>
                  <span>{r}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </div>
  );
};


// ── Main Page ─────────────────────────────────────────────────────────────
export default function DeepDive() {
  const { symbol } = useParams();
  const navigate = useNavigate();
  const { stocks, marketStatus, theme } = useStockStore();

  const [history, setHistory] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(true);
  const [insight, setInsight] = useState(null);
  const [insightLoading, setInsightLoading] = useState(true);
  const [insightError, setInsightError] = useState(false);
  const [fundResearchLoading, setFundResearchLoading] = useState(false);
  const [signalRegening, setSignalRegening] = useState(false);
  const [insightHistory, setInsightHistory] = useState([]);
  const [selectedHistoryId, setSelectedHistoryId] = useState(null);
  const [isEditModalOpen, setIsEditModalOpen] = useState(false);
  const [fundSyncLoading, setFundSyncLoading] = useState(false);
  const [screenerSyncLoading, setScreenerSyncLoading] = useState(false);
  const [editData, setEditData] = useState({});
  const [activeTab, setActiveTab] = useState('chart');
  const [screenerData, setScreenerData] = useState(null);
  const [selectedSkill, setSelectedSkill] = useState(SKILLS[0].id);
  const [signals, setSignals] = useState(null);
  const [fundamentals, setFundamentals] = useState(null);
  const [lots, setLots] = useState([]);
  const [analysis, setAnalysis] = useState(null);
  const [analysisLoading, setAnalysisLoading] = useState(false);

  const stock = stocks[symbol];
  const sig = stock?.signal || {};

  // Price change
  const priceChange = sig.change_pct;
  const priceColor = priceChange > 0 ? 'text-signal-buy' : priceChange < 0 ? 'text-signal-sell' : 'text-dark-muted';
  const PriceIcon = priceChange > 0 ? TrendingUp : priceChange < 0 ? TrendingDown : Minus;

  const portfolioSummary = useMemo(() => {
    if (!lots || lots.length === 0) return null;
    // The backend already filters for OPEN lots, but we double-check just in case
    const openLots = lots.filter(l => !l.status || l.status === 'OPEN');
    if (openLots.length === 0) return null;

    const quantity = openLots.reduce((sum, l) => sum + parseFloat(l.quantity), 0);
    const invested = openLots.reduce((sum, l) => sum + (parseFloat(l.quantity) * parseFloat(l.buy_price)), 0);
    const avgPrice = invested / quantity;
    const currentPrice = sig.current_price || 0;
    const currentValue = quantity * currentPrice;
    const pnl = currentValue - invested;
    const pnlPct = (pnl / invested) * 100;

    return { quantity, invested, avgPrice, currentValue, pnl, pnlPct };
  }, [lots, sig.current_price]);

  const fetchData = useCallback(async () => {
    setHistoryLoading(true);
    setInsightLoading(true);
    setInsightError(false);

    const token = localStorage.getItem('mm_token') || localStorage.getItem('token');
    const headers = { 'Authorization': `Bearer ${token}` };

    // Parallel fetches
    const [histRes, insightRes, signalsRes, fundRes, lotsRes, historyRes, screenerRes] = await Promise.allSettled([
      fetch(`${API_URL}/api/stock/${symbol}/history`, { headers }),
      fetch(`${API_URL}/api/stock/${symbol}/insight`, { headers }),
      fetch(`${API_URL}/api/stock/${symbol}/signals`, { headers }),
      fetch(`${API_URL}/api/stock/${symbol}/fundamentals`, { headers }),
      fetch(`${API_URL}/api/stock/${symbol}/lots`, { headers }),
      fetch(`${API_URL}/api/ai-logs?symbol=${symbol}&limit=50`, { headers }),
      fetch(`${API_URL}/api/stock/${symbol}/screener`, { headers }),
    ]);

    if (histRes.status === 'fulfilled' && histRes.value.ok) {
      setHistory(await histRes.value.json());
    } else { setHistory([]); }
    setHistoryLoading(false);

    if (insightRes.status === 'fulfilled') {
      if (insightRes.value.ok) {
        setInsight(await insightRes.value.json());
        setInsightError(false);
      } else if (insightRes.value.status === 404) {
        setInsight(null);
        setInsightError(true);
      }
    } else { setInsight(null); setInsightError(true); }
    setInsightLoading(false);

    if (signalsRes.status === 'fulfilled' && signalsRes.value.ok) {
      setSignals(await signalsRes.value.json());
    }
    if (fundRes.status === 'fulfilled' && fundRes.value.ok) {
      setFundamentals(await fundRes.value.json());
    }
    if (lotsRes.status === 'fulfilled' && lotsRes.value.ok) {
      setLots(await lotsRes.value.json());
    } else {
      setLots([]);
    }

    if (historyRes.status === 'fulfilled' && historyRes.value.ok) {
      setInsightHistory(await historyRes.value.json());
    } else {
      setInsightHistory([]);
    }

    if (screenerRes.status === 'fulfilled' && screenerRes.value.ok) {
      const sd = await screenerRes.value.json();
      if (sd.available) setScreenerData(sd);
    }

    // Fetch Full Consensus Analysis if ISIN exists
    if (stock?.isin) {
      setAnalysisLoading(true);
      try {
        const token = localStorage.getItem('mm_token') || localStorage.getItem('token');
        const aRes = await fetch(`${API_URL}/api/analysis/${stock.isin}/full`, {
          headers: { 'Authorization': `Bearer ${token}` }
        });
        if (aRes.ok) {
          setAnalysis(await aRes.json());
        }
      } catch (e) {
        console.error("Analysis fetch failed:", e);
      }
      setAnalysisLoading(false);
    }
  }, [symbol, stock?.isin]);

  const handleGenerateInsight = async () => {
    try {
      setInsightLoading(true);
      const token = localStorage.getItem('mm_token');
      const res = await fetch(`${API_URL}/api/stock/${symbol}/insight/generate`, { 
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}` 
        },
        body: JSON.stringify({ skill_id: selectedSkill })
      });
      if (res.ok) {
        toast.success('AI generation started. Refreshing in 30s...');
        // Poll for result — Pi can be slow, try at 30s then 60s
        setTimeout(fetchData, 30000);
        setTimeout(fetchData, 60000);
      } else {
        const err = await res.json();
        toast.error(err.detail || 'Failed to generate insight');
        setInsightLoading(false);
      }
    } catch (e) {
      setInsightLoading(false);
    }
  };

  const handleManualUpdate = async (e) => {
    e.preventDefault();
    try {
      const token = localStorage.getItem('mm_token');
      const res = await fetch(`${API_URL}/api/stock/${symbol}/fundamentals`, {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify(editData)
      });
      if (res.ok) {
        toast.success('Fundamentals updated! Signals recomputing...');
        setIsEditModalOpen(false);
        fetchData(); // Refresh UI
      } else {
        const err = await res.json();
        toast.error(err.detail || 'Failed to update');
      }
    } catch (e) {
      toast.error('Network error updating fundamentals');
    }
  };


  const handleFundSync = async () => {
    try {
      setFundSyncLoading(true);
      const token = localStorage.getItem('mm_token');
      const res = await fetch(`${API_URL}/api/stock/${symbol}/fundamentals/sync`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (res.ok) {
        const data = await res.json();
        toast.success(`Synced! Source data quality: ${data.status}`);
        // Automate signal refresh to pick up new fundamentals
        await handleRegenerateSignals();
      } else {
        toast.error('Failed to sync from Yahoo');
      }
    } catch (e) {
      toast.error('Sync error');
    } finally {
      setFundSyncLoading(false);
    }
  };

  const handleScreenerSync = async () => {
    try {
      setScreenerSyncLoading(true);
      const token = localStorage.getItem('mm_token');
      const res = await fetch(`${API_URL}/api/stock/${symbol}/fundamentals/sync-screener`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (res.ok) {
        const data = await res.json();
        if (data.filled > 0) {
          toast.success(`Screener.in filled ${data.filled} field(s): ${data.fields?.join(', ')}`);
          await handleRegenerateSignals();
        } else {
          toast.success('No missing fields — all data already present.');
        }
      } else {
        const err = await res.json().catch(() => ({}));
        toast.error(err.detail || 'Screener.in sync failed');
      }
    } catch (e) {
      toast.error('Screener sync error');
    } finally {
      setScreenerSyncLoading(false);
    }
  };

  const handleRegenerateSignals = async () => {
    try {
      setSignalRegening(true);
      const token = localStorage.getItem('mm_token');
      const res = await fetch(`${API_URL}/api/stock/${symbol}/signals/recompute`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (res.ok) {
        toast.success('Signals recomputed successfully!');
        fetchData();
      } else {
        toast.error('Failed to recompute signals');
      }
    } catch (e) {
      toast.error('Recompute error');
    } finally {
      setSignalRegening(false);
    }
  };

  const fetchPatterns = useCallback(async () => {
    if (!symbol) return;
    setPatternLoading(true);
    try {
      const token = localStorage.getItem('mm_token');
      const res = await fetch(`${API_URL}/api/stock/${symbol}/patterns`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (res.ok) {
        const data = await res.json();
        setPatterns(data.patterns || []);
        setPatternSummary(data.summary || '');
      }
    } catch (e) {
      console.warn('Pattern fetch failed:', e);
    } finally {
      setPatternLoading(false);
    }
  }, [symbol]);

  const handleSyncAndResearch = async () => {
    try {
      setFundSyncLoading(true);
      const token = localStorage.getItem('mm_token');
      // 1. Sync from Yahoo
      const syncRes = await fetch(`${API_URL}/api/stock/${symbol}/fundamentals/sync`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` }
      });
      
      if (!syncRes.ok) {
        toast.error('Sync failed, using existing data for research');
      } else {
        toast.success('Yahoo sync complete!');
      }

      // 2. Generate Insight
      setInsightLoading(true);
      const insRes = await fetch(`${API_URL}/api/stock/${symbol}/insight/generate`, { 
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}` 
        },
        body: JSON.stringify({ skill_id: selectedSkill })
      });
      
      if (insRes.ok) {
        toast.success('Sync-Research started. Refreshing in 15s...');
        setTimeout(fetchData, 15000);
      } else {
        const err = await insRes.json();
        toast.error(err.detail || 'Research failed');
        setInsightLoading(false);
      }
    } catch (e) {
      toast.error('Sync-Research error');
      setInsightLoading(false);
    } finally {
      setFundSyncLoading(false);
    }
  };

  const handleAIResearch = async () => {
    try {
      setFundResearchLoading(true);
      const token = localStorage.getItem('mm_token');
      const res = await fetch(`${API_URL}/api/stock/${symbol}/fundamentals/research`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`
        }
      });
      if (res.ok) {
        toast.success('AI research started. Check logs or refresh in 15s.');
        setTimeout(fetchData, 15000);
      } else {
        const err = await res.json();
        toast.error(err.detail || 'Failed to start AI research');
      }
    } catch (e) {
      toast.error('Connection error during AI research');
    } finally {
      setFundResearchLoading(false);
    }
  };

  const [stockAlerts, setStockAlerts]   = useState([]);
  const [alertsLoading, setAlertsLoading] = useState(false);

  const fetchStockAlerts = useCallback(async () => {
    try {
      const token = localStorage.getItem('mm_token');
      const res = await fetch(`${API_URL}/api/alerts`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      if (res.ok) {
        const all = await res.json();
        setStockAlerts(all.filter(a => a.symbol === symbol && a.is_active && !a.is_triggered));
      }
    } catch (e) { console.warn('Alerts fetch failed:', e); }
  }, [symbol]);

  useEffect(() => {
    fetchData();
    fetchPatterns();
    fetchStockAlerts();
    window.addEventListener('generate-insight', handleGenerateInsight);
    return () => window.removeEventListener('generate-insight', handleGenerateInsight);
  }, [fetchData, fetchPatterns, fetchStockAlerts, symbol]);

  // Chart range filtering
  const [range, setRange] = useState('3M');
  const rangeMap = { '1W': 7, '1M': 21, '3M': 63, '6M': 126, '1Y': 252, 'ALL': 9999 };
  const filteredHistory = history.slice(-(rangeMap[range] ?? 63));
  const fullHistoryForIndicators = history; // always full dataset for RSI/MACD accuracy

  // Volume max for bar scaling
  const maxVol = Math.max(...filteredHistory.map(d => d.volume || 0), 1);

  // ── Chart Chat (Multi-Session, Persistent) ─────────────────────────────
  const CHAT_STORAGE_KEY = `mm_chart_chats_${symbol}`;

  const [showChartChat, setShowChartChat] = useState(false);
  const [showSessionList, setShowSessionList] = useState(false);
  const [chatSessions, setChatSessions] = useState([]);
  const [activeChatSessionId, setActiveChatSessionId] = useState(null);
  const [chatInput, setChatInput] = useState('');
  const [chatLoading, setChatLoading] = useState(false);
  const [activeSkill, setActiveSkill]     = useState(null); // null = default chart AI
  const [showSkillPicker, setShowSkillPicker] = useState(false);
  const [activeTrendLines, setActiveTrendLines] = useState([]);
  const [activePriceTarget, setActivePriceTarget] = useState(null);
  const [patterns, setPatterns]               = useState([]);
  const [patternSummary, setPatternSummary]   = useState('');
  const [patternLoading, setPatternLoading]   = useState(false);
  const [patternModal, setPatternModal]       = useState(null); // holds one pattern object
  const chatScrollRef = useRef(null);

  // Derived: active session messages
  const activeSession = chatSessions.find(s => s.id === activeChatSessionId);
  const chatMessages = activeSession?.messages || [];

  // Load sessions from localStorage when symbol changes
  useEffect(() => {
    const stored = localStorage.getItem(CHAT_STORAGE_KEY);
    if (stored) {
      try {
        const sessions = JSON.parse(stored);
        setChatSessions(sessions);
        // Default to latest session
        if (sessions.length > 0) {
          setActiveChatSessionId(sessions[sessions.length - 1].id);
          setActiveTrendLines(sessions[sessions.length - 1].trendLines || []);
        }
      } catch { /* ignore corrupt data */ }
    } else {
      setChatSessions([]);
      setActiveChatSessionId(null);
    }
  }, [symbol]);

  // Persist sessions to localStorage whenever they change
  const saveSessions = (sessions) => {
    setChatSessions(sessions);
    localStorage.setItem(CHAT_STORAGE_KEY, JSON.stringify(sessions));
  };

  const createNewSession = (autoStart = false) => {
    const newSession = {
      id: Date.now().toString(),
      title: 'New Analysis',
      createdAt: new Date().toISOString(),
      messages: [],
      trendLines: [],
    };
    const updated = [...chatSessions, newSession];
    saveSessions(updated);
    setActiveChatSessionId(newSession.id);
    setActiveTrendLines([]);
    setShowSessionList(false);
    return newSession.id;
  };

  const updateSession = (sessionId, newMessages, newTrendLines = null) => {
    setChatSessions(prev => {
      const updated = prev.map(s => {
        if (s.id !== sessionId) return s;
        // Use first user message as title
        const firstUser = newMessages.find(m => m.role === 'user');
        const title = firstUser
          ? firstUser.content.slice(0, 40) + (firstUser.content.length > 40 ? '…' : '')
          : s.title;
        return { ...s, messages: newMessages, trendLines: newTrendLines ?? s.trendLines, title };
      });
      localStorage.setItem(CHAT_STORAGE_KEY, JSON.stringify(updated));
      return updated;
    });
  };

  const deleteSession = (sessionId) => {
    const updated = chatSessions.filter(s => s.id !== sessionId);
    saveSessions(updated);
    if (activeChatSessionId === sessionId) {
      const next = updated[updated.length - 1];
      setActiveChatSessionId(next?.id || null);
      setActiveTrendLines(next?.trendLines || []);
    }
  };

  const switchSession = (sessionId) => {
    const s = chatSessions.find(s => s.id === sessionId);
    setActiveChatSessionId(sessionId);
    setActiveTrendLines(s?.trendLines || []);
    setShowSessionList(false);
  };

  // Auto-scroll when messages change
  useEffect(() => {
    if (chatScrollRef.current) {
      chatScrollRef.current.scrollTop = chatScrollRef.current.scrollHeight;
    }
  }, [chatMessages]);

  const sendChatMessage = async (msgOverride = null, sessionIdOverride = null) => {
    const text = msgOverride || chatInput;
    if (!text.trim() && !msgOverride) return;

    // Intercept alert-intent messages — route to alert creation instead of chat
    const alertKeywords = ['set an alert', 'set alert', 'alert me', 'notify me', 'set a stop', 'alert at'];
    const isAlertRequest = alertKeywords.some(kw => text.toLowerCase().includes(kw));
    if (isAlertRequest) {
      if (!msgOverride) setChatInput('');
      await createAIAlert(text);
      return;
    }

    const targetSessionId = sessionIdOverride || activeChatSessionId;
    if (!targetSessionId) return;

    const currentSession = chatSessions.find(s => s.id === targetSessionId);
    const prevMessages = currentSession?.messages || [];

    const newMsg = { role: 'user', content: text };
    const updatedMessages = [...prevMessages, newMsg];
    updateSession(targetSessionId, updatedMessages, currentSession?.trendLines);
    if (!msgOverride) setChatInput('');
    setChatLoading(true);

    try {
      const token = localStorage.getItem('mm_token');
      const endpoint = activeSkill
        ? `${API_URL}/api/stock/${symbol}/skill_chat`
        : `${API_URL}/api/stock/${symbol}/chart_chat`;

      const body = activeSkill
        ? JSON.stringify({ skill_id: activeSkill.id, messages: updatedMessages })
        : JSON.stringify({ messages: updatedMessages });

      const res = await fetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body
      });
      if (res.ok) {
        const data = await res.json();
        const finalMessages = [...updatedMessages, { role: 'assistant', content: data.reply || '' }];
        const newTrendLines = data.trend_lines && data.trend_lines.length > 0 ? data.trend_lines : (currentSession?.trendLines || []);
        updateSession(targetSessionId, finalMessages, newTrendLines);
        setActiveTrendLines(newTrendLines);
        if (data.trend_lines?.length > 0) toast.success(`AI plotted ${data.trend_lines.length} trendlines`);
        if (data.price_target) {
          setActivePriceTarget(data.price_target);
          toast.success(`AI target: ₹${data.price_target.confidence_low?.toFixed(0)}–₹${data.price_target.confidence_high?.toFixed(0)}`);
        }
      } else {
        toast.error('AI chat failed');
        updateSession(targetSessionId, prevMessages);
      }
    } catch (e) {
      toast.error('Connection error');
      updateSession(targetSessionId, prevMessages);
    } finally {
      setChatLoading(false);
    }
  };

  const deleteAlert = async (alertId) => {
    try {
      const token = localStorage.getItem('mm_token');
      await fetch(`${API_URL}/api/alerts/${alertId}`, {
        method: 'DELETE',
        headers: { 'Authorization': `Bearer ${token}` }
      });
      setStockAlerts(prev => prev.filter(a => a.id !== alertId));
      toast.success('Alert removed');
    } catch (e) { toast.error('Failed to remove alert'); }
  };

  const createAIAlert = async (message) => {
    if (!message.trim()) return;
    setAlertsLoading(true);
    try {
      const token = localStorage.getItem('mm_token');
      const currentSession = chatSessions.find(s => s.id === activeChatSessionId);
      const contextData = {
        composite_score: sig?.composite_score,
        current_st_signal: sig?.st_signal,
        current_lt_signal: sig?.lt_signal,
        today: { close: sig?.current_price },
        price_summary: {
          sma_20: sig?.ta_breakdown?.sma20,
          sma_50: sig?.ta_breakdown?.sma50,
          "90d_high": Math.max(...(filteredHistory.map(d => d.high) || [0])),
          "90d_low":  Math.min(...(filteredHistory.map(d => d.low)  || [0])),
          recent_5_bars: filteredHistory.slice(-5),
        }
      };
      const res = await fetch(`${API_URL}/api/stock/${symbol}/alerts/ai`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({ message, context_data: contextData })
      });
      if (res.ok) {
        const data = await res.json();
        toast.success(`${data.count} alert(s) set for ${symbol}`);
        fetchStockAlerts();
        // Echo the AI reply into the chat
        if (data.reply && activeChatSessionId) {
          const currentMsgs = currentSession?.messages || [];
          updateSession(activeChatSessionId, [
            ...currentMsgs,
            { role: 'assistant', content: `🔔 ${data.reply}\n\n${data.alerts?.map(a => `• **${a.label}** — ₹${a.price_level?.toFixed(2)} (${a.direction})`).join('\n')}` }
          ]);
        }
      } else {
        toast.error('Failed to set alert');
      }
    } catch (e) {
      toast.error('Alert creation failed');
    } finally {
      setAlertsLoading(false);
    }
  };

  const toggleChat = () => {
    const willOpen = !showChartChat;
    setShowChartChat(willOpen);
    if (willOpen) {
      const stored = localStorage.getItem(CHAT_STORAGE_KEY);
      const existingSessions = (stored && stored !== 'undefined') ? JSON.parse(stored) : [];
      if (existingSessions.length === 0) {
        // Auto-start the very first session
        const newSession = {
          id: Date.now().toString(),
          title: 'Initial Analysis',
          createdAt: new Date().toISOString(),
          messages: [],
          trendLines: [],
        };
        const updated = [newSession];
        saveSessions(updated);
        setActiveChatSessionId(newSession.id);
        const autoMsg = sig
          ? `Analyze ${symbol} chart. Current price ₹${Number(sig.current_price || 0).toFixed(2)}, ` +
            `ST signal ${sig.st_signal}, LT signal ${sig.lt_signal}. ` +
            `Is this a good long-term entry? Plot key support/resistance levels.`
          : `Please analyze this ${symbol} chart. State if this is a good buy setup and plot key support/resistance.`;
        
        // Instead of auto-sending, we just prepare the input for the user
        setChatInput(autoMsg);
      }
    }
  };

  const tabs = [
    { id: 'chart', label: 'Price Chart', Icon: BarChart2 },
    { id: 'positions', label: 'Tax Lots', Icon: Briefcase },
    { id: 'signals', label: 'Signals', Icon: Layers },
    { id: 'fundamentals', label: 'Fundamentals', Icon: BookOpen },
    { id: 'ai', label: 'AI Insight', Icon: Brain },
    { id: 'historical', label: 'Historical Data', Icon: Database },
    { id: 'screener', label: 'Screener', Icon: PieChart },
  ];

  return (
    <div className="p-6 space-y-6">
      {/* Portfolio Status Bar (if held) */}
      {portfolioSummary && (
        <div className="mb-0 p-1 bg-gradient-to-r from-accent/20 to-transparent rounded-2xl border border-accent/20">
          <div className="bg-dark-bg/80 backdrop-blur-xl p-4 rounded-xl flex flex-wrap items-center gap-8 shadow-2xl">
             <div className="flex items-center gap-3 pr-8 border-r border-dark-border">
                <div className="p-2 bg-accent/10 rounded-lg text-accent">
                   <Shield size={18} />
                </div>
                <div>
                   <p className="text-[10px] text-dark-muted font-bold uppercase tracking-widest leading-none mb-1">Your Holding</p>
                   <p className="text-sm font-black text-dark-text leading-none">{portfolioSummary.quantity.toLocaleString()} Shares</p>
                </div>
             </div>

             <div className="flex-1 flex flex-wrap gap-8">
                <div>
                   <p className="text-[10px] text-dark-muted font-bold uppercase tracking-tight leading-none mb-2">Total Invested</p>
                   <p className="text-base font-mono font-bold text-dark-text leading-none">₹{portfolioSummary.invested.toLocaleString(undefined, {minimumFractionDigits: 2})}</p>
                </div>
                <div>
                   <p className="text-[10px] text-dark-muted font-bold uppercase tracking-tight leading-none mb-2">Avg. Cost</p>
                   <p className="text-base font-mono font-bold text-dark-text leading-none">₹{portfolioSummary.avgPrice.toFixed(2)}</p>
                </div>
                <div>
                   <p className="text-[10px] text-dark-muted font-bold uppercase tracking-tight leading-none mb-2">Market Value</p>
                   <p className="text-base font-mono font-bold text-dark-text leading-none">₹{portfolioSummary.currentValue.toLocaleString(undefined, {minimumFractionDigits: 2})}</p>
                </div>
                <div className="ml-auto flex items-center gap-6">
                   <div className="text-right">
                      <p className="text-[10px] text-dark-muted font-bold uppercase tracking-tight leading-none mb-2">Total Return</p>
                      <div className={`flex items-center gap-2 justify-end ${portfolioSummary.pnl >= 0 ? 'text-signal-buy' : 'text-signal-sell'}`}>
                         <span className="text-lg font-black font-mono">
                            {portfolioSummary.pnl >= 0 ? '+' : ''}₹{portfolioSummary.pnl.toLocaleString(undefined, {minimumFractionDigits: 2})}
                         </span>
                         <div className={`px-2 py-0.5 rounded text-[10px] font-black ${portfolioSummary.pnl >= 0 ? 'bg-signal-buy/20' : 'bg-signal-sell/20'}`}>
                            {portfolioSummary.pnlPct.toFixed(2)}%
                         </div>
                      </div>
                   </div>
                </div>
             </div>
          </div>
        </div>
      )}
      {/* Page Title & Back */}
      <div className="flex items-center gap-4">
        <button
          onClick={() => navigate(-1)}
          className="p-2 rounded-xl hover:bg-dark-border transition-all text-dark-muted hover:text-dark-text bg-dark-card border border-dark-border"
        >
          <ArrowLeft size={18} />
        </button>
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-accent flex items-center gap-3">
            {symbol}
            {stock?.company_name && (
              <span className="text-sm font-normal text-dark-muted">/ {stock.company_name}</span>
            )}
            {stockAlerts.length > 0 && (
              <div className="flex items-center gap-1.5 px-2.5 py-1 bg-accent/10 border border-accent/30 rounded-full">
                <BellRing size={12} className="text-accent animate-pulse" />
                <span className="text-[11px] font-black text-accent">{stockAlerts.length} alert{stockAlerts.length > 1 ? 's' : ''} active</span>
              </div>
            )}
          </h1>
          {stock?.sector && (
            <p className="text-xs text-dark-muted font-mono uppercase tracking-widest mt-0.5">{stock.sector}</p>
          )}
        </div>
      </div>

      {/* Price Hero */}
      <div className="bg-dark-card border border-dark-border rounded-2xl p-6 mb-6 shadow-2xl">
        <div className="flex flex-wrap items-end justify-between gap-6">
          <div>
            <div className="flex items-baseline gap-3 mb-2">
              <span className="text-5xl font-bold font-mono">
                ₹{sig.current_price?.toFixed(2) ?? '—'}
              </span>
              {priceChange != null && (
                <span className={`flex items-center gap-1 text-lg font-semibold font-mono ${priceColor}`}>
                  <PriceIcon size={20} />
                  {priceChange > 0 ? '+' : ''}{priceChange.toFixed(2)}%
                </span>
              )}
            </div>
            <p className="text-dark-muted text-sm">
              {marketStatus === 'OPEN' ? 'Live NSE Price' : 'Last Traded Price'}
            </p>
          </div>

          {/* Signals */}
          <div className="flex gap-6 flex-wrap">
            <div className="text-center">
              <p className="text-xs text-dark-muted mb-2 uppercase tracking-wide font-medium">Short Term</p>
              <SignalBadge signal={sig.st_signal} score={sig.st_score} size="lg" />
            </div>
            <div className="text-center">
              <p className="text-xs text-dark-muted mb-2 uppercase tracking-wide font-medium">Long Term</p>
              <SignalBadge signal={sig.lt_signal} score={sig.lt_score} size="lg" />
            </div>
            <div className="text-center min-w-[100px] relative group/score">
              <p className="text-xs text-dark-muted mb-2 uppercase tracking-wide font-medium">Composite Score</p>
              <div className="flex items-center gap-3">
                <div className="text-3xl font-bold font-mono text-accent">
                  {analysis?.score?.composite_score ?? sig.lt_score?.toFixed(1) ?? '—'}
                </div>
                <div className="text-[10px] text-dark-muted leading-tight">
                  / 100<br/>V2 ENGINE
                </div>
                <button
                  onClick={handleRegenerateSignals}
                  disabled={signalRegening}
                  className="p-1.5 rounded-lg hover:bg-accent/10 text-dark-muted hover:text-accent transition-all opacity-0 group-hover/score:opacity-100 disabled:opacity-50"
                  title="Force recompute signals"
                >
                  <RefreshCw size={14} className={signalRegening ? 'animate-spin' : ''} />
                </button>
              </div>
            </div>
          </div>
        </div>

        {/* Forensic/Governance Warning Strip */}
        {analysis?.score?.promoter_pledge_warning && (
          <div className="mt-4 p-3 bg-red-500/10 border border-red-500/20 rounded-xl flex items-center gap-3">
            <AlertTriangle size={16} className="text-red-500" />
            <span className="text-xs font-bold text-red-500 uppercase tracking-wide">
              Critical Warning: High Promoter Pledge Detected (&gt;20%)
            </span>
            <span className="text-[10px] text-red-400/70 ml-auto">
              Scoring engine has applied a -15pt governance penalty.
            </span>
          </div>
        )}
      </div>

      {/* Metrics Row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <MetricCard
          label="ST Score"
          value={sig.st_score?.toFixed(1) ?? '—'}
          sub="Short-term technical"
          color={(sig.st_score ?? 0) > 60 ? 'text-signal-buy' : (sig.st_score ?? 0) < 40 ? 'text-signal-sell' : 'text-signal-hold'}
        />
        <MetricCard
          label="LT Score"
          value={sig.lt_score?.toFixed(1) ?? '—'}
          sub="Long-term fundamental"
          color={(sig.lt_score ?? 0) > 60 ? 'text-signal-buy' : (sig.lt_score ?? 0) < 40 ? 'text-signal-sell' : 'text-signal-hold'}
        />
        <MetricCard
          label="Data Points"
          value={history.length > 0 ? `${history.length}d` : '—'}
          sub="Historical bars loaded"
        />
        <MetricCard
          label="Quality"
          value={sig.data_quality ?? '—'}
          sub="Signal data quality"
          color={
            sig.data_quality === 'FULL' ? 'text-signal-buy' :
            sig.data_quality === 'PARTIAL' ? 'text-signal-hold' : 'text-dark-muted'
          }
        />
      </div>

      {/* Tab Panel */}
      <div className="bg-dark-card border border-dark-border rounded-2xl overflow-hidden shadow-xl">
        {/* Tabs */}
        <div className="flex border-b border-dark-border">
          {tabs.map(({ id, label, Icon }) => (
            <button
              key={id}
              onClick={() => setActiveTab(id)}
              className={`flex items-center gap-2 px-6 py-4 text-sm font-medium transition-colors border-b-2 ${
                activeTab === id
                  ? 'border-accent text-accent bg-accent/5'
                  : 'border-transparent text-dark-muted hover:text-dark-text '
              }`}
            >
              <Icon size={16} />
              {label}
            </button>
          ))}
        </div>

        <div className="p-6">
          {/* Chart Tab */}
          {activeTab === 'chart' && (
            <div className="relative">
              {/* Range buttons + Action Button */}
              <div className="flex items-center justify-between mb-5">
                <div className="flex gap-2">
                  {Object.keys(rangeMap).map(r => (
                    <button
                      key={r}
                      onClick={() => setRange(r)}
                      className={`px-3 py-1 rounded text-xs font-mono font-medium transition-colors ${
                        range === r
                          ? 'bg-accent text-white'
                          : 'text-dark-muted bg-gray-800 hover:bg-gray-700'
                      }`}
                    >
                      {r}
                    </button>
                  ))}
                </div>
                {/* Pattern badges */}
                <div className="flex items-center gap-2 flex-wrap">
                  {patternLoading && (
                    <div className="flex items-center gap-1.5 px-3 py-1 bg-dark-card border border-dark-border rounded-full">
                      <RefreshCw size={11} className="animate-spin text-dark-muted" />
                      <span className="text-[10px] text-dark-muted font-bold">Scanning patterns…</span>
                    </div>
                  )}
                  {!patternLoading && patterns.map((p, i) => (
                    <button
                      key={i}
                      onClick={() => setPatternModal(p)}
                      className={`flex items-center gap-1.5 px-3 py-1 rounded-full border text-[10px] font-black transition-all hover:scale-105 ${
                        p.implication === 'Bullish'
                          ? 'bg-signal-buy/10 border-signal-buy/40 text-signal-buy hover:bg-signal-buy/20'
                          : p.implication === 'Bearish'
                          ? 'bg-signal-sell/10 border-signal-sell/40 text-signal-sell hover:bg-signal-sell/20'
                          : 'bg-signal-hold/10 border-signal-hold/40 text-signal-hold hover:bg-signal-hold/20'
                      }`}
                    >
                      <span>{p.implication === 'Bullish' ? '📐' : p.implication === 'Bearish' ? '📉' : '◈'}</span>
                      {p.name}
                      <span className="opacity-60">{Math.round(p.confidence * 100)}%</span>
                    </button>
                  ))}
                  {!patternLoading && patterns.length === 0 && (
                    <div className="flex items-center gap-1.5">
                      <span className="text-[10px] text-dark-muted font-mono">No active patterns</span>
                      <button
                        onClick={async () => {
                          const token = localStorage.getItem('mm_token');
                          await fetch(`${API_URL}/api/stock/${symbol}/patterns/cache`, {
                            method: 'DELETE',
                            headers: { 'Authorization': `Bearer ${token}` }
                          });
                          fetchPatterns();
                        }}
                        title="Force rescan"
                        className="flex items-center gap-1 px-2 py-0.5 rounded text-[10px] text-dark-muted bg-gray-800 hover:bg-gray-700 hover:text-white transition-colors"
                      >
                        <RefreshCw size={9} />
                        Rescan
                      </button>
                    </div>
                  )}
                </div>

                <button 
                  onClick={toggleChat}
                  className={`flex items-center gap-2 px-4 py-1.5 rounded-lg text-sm font-bold transition-colors border ${
                    showChartChat ? 'bg-signal-buy text-white border-signal-buy' : 'bg-dark-card text-accent border-accent hover:bg-accent/10'
                  }`}
                >
                  <Sparkles size={16} />
                  Ask AI
                </button>
              </div>

              {historyLoading ? (
                <div className="flex items-center justify-center py-20 gap-2 text-dark-muted">
                  <RefreshCw size={18} className="animate-spin" />
                  <span>Loading price data…</span>
                </div>
              ) : filteredHistory.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-20 gap-3 text-dark-muted">
                  <Activity size={36} className="opacity-30" />
                  <p className="text-sm">No historical data available.</p>
                  <p className="text-xs text-dark-muted/70">Run the historical data loader to populate price data.</p>
                </div>
              ) : (
                <div className="flex gap-4 items-stretch">
                  {/* Left Chart Area */}
                  <div className={`flex-1 overflow-hidden transition-all duration-300 ${showChartChat ? 'w-2/3' : 'w-full'}`}>
                    <div className="space-y-4">
                      <CandlestickChart data={filteredHistory} theme={theme} trendLines={activeTrendLines} priceTarget={activePriceTarget} />
                      <VolumeChart data={filteredHistory} theme={theme} />
                      <RSIChart data={fullHistoryForIndicators} visibleRange={rangeMap[range]} theme={theme} />
                      <MACDChart data={fullHistoryForIndicators} visibleRange={rangeMap[range]} theme={theme} />
                    </div>

                    {/* OHLC last bar summary */}
                    {filteredHistory.length > 0 && (() => {
                      const last = filteredHistory[filteredHistory.length - 1];
                      return (
                        <div className="flex flex-wrap gap-5 mt-4 pt-4 border-t border-dark-border text-xs font-mono text-dark-muted">
                          <span>O: <span className="text-dark-text">₹{last.open?.toFixed(2)}</span></span>
                          <span>H: <span className="text-signal-buy">₹{last.high?.toFixed(2)}</span></span>
                          <span>L: <span className="text-signal-sell">₹{last.low?.toFixed(2)}</span></span>
                          <span>C: <span className="text-dark-text font-bold">₹{last.close?.toFixed(2)}</span></span>
                          <span>Vol: <span className="text-dark-text">{last.volume?.toLocaleString()}</span></span>
                          <span className="ml-auto text-dark-muted">{last.date}</span>
                        </div>
                      );
                    })()}

                    {/* Pattern Detail Modal */}
                    {patternModal && (
                      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm" onClick={() => setPatternModal(null)}>
                        <div className="bg-dark-card border border-dark-border rounded-2xl p-6 max-w-md w-full mx-4 shadow-2xl" onClick={e => e.stopPropagation()}>
                          <div className="flex items-start justify-between mb-4">
                            <div>
                              <div className="flex items-center gap-2 mb-1">
                                <span className={`text-xs font-black px-2 py-0.5 rounded-full ${
                                  patternModal.implication === 'Bullish' ? 'bg-signal-buy/20 text-signal-buy' :
                                  patternModal.implication === 'Bearish' ? 'bg-signal-sell/20 text-signal-sell' :
                                  'bg-signal-hold/20 text-signal-hold'
                                }`}>{patternModal.implication}</span>
                                <span className="text-xs text-dark-muted font-mono">{Math.round(patternModal.confidence * 100)}% confidence</span>
                              </div>
                              <h3 className="text-lg font-black text-white">📐 {patternModal.name}</h3>
                            </div>
                            <button onClick={() => setPatternModal(null)} className="text-dark-muted hover:text-white p-1"><X size={18}/></button>
                          </div>

                          <p className="text-sm text-dark-text leading-relaxed mb-4">{patternModal.description}</p>

                          <div className="grid grid-cols-2 gap-3 mb-4">
                            {patternModal.target_price && (
                              <div className="bg-signal-buy/5 border border-signal-buy/20 rounded-xl p-3">
                                <p className="text-[10px] text-dark-muted uppercase font-bold mb-1">Target</p>
                                <p className="text-signal-buy font-black font-mono">₹{patternModal.target_price?.toFixed(2)}</p>
                              </div>
                            )}
                            {patternModal.stop_loss && (
                              <div className="bg-signal-sell/5 border border-signal-sell/20 rounded-xl p-3">
                                <p className="text-[10px] text-dark-muted uppercase font-bold mb-1">Stop Loss</p>
                                <p className="text-signal-sell font-black font-mono">₹{patternModal.stop_loss?.toFixed(2)}</p>
                              </div>
                            )}
                          </div>

                          <button
                            onClick={() => {
                              setPatternModal(null);
                              const trendLines = patternModal.trend_lines || [];
                              if (trendLines.length > 0) setActiveTrendLines(trendLines);
                              if (!showChartChat) toggleChat();
                              setTimeout(() => {
                                setChatInput(`Explain the ${patternModal.name} pattern on this chart and tell me if I should act on it.`);
                                sendChatMessage(`Explain the ${patternModal.name} pattern on this chart and tell me if I should act on it.`);
                              }, 300);
                            }}
                            className="w-full py-2.5 bg-accent hover:bg-accent/80 text-white rounded-xl text-sm font-black transition-all"
                          >
                            <Sparkles size={14} className="inline mr-2" />
                            Ask AI to Analyse This Pattern
                          </button>
                        </div>
                      </div>
                    )}
                  </div>

                  {/* Right Chat Panel */}
                  {showChartChat && (
                    <div className="w-1/3 min-w-[320px] border border-dark-border bg-[#0d1117] rounded-2xl flex flex-col shadow-2xl h-[800px]">
                      
                      {/* Header */}
                      <div className="p-3 border-b border-dark-border bg-dark-bg flex items-center gap-2 shrink-0">
                        <span className="text-sm">{activeSkill ? activeSkill.icon : '🧠'}</span>
                        <span className="font-bold text-accent text-sm flex-1 truncate">
                          {activeSkill ? activeSkill.name : (activeSession?.title || 'Chart Intelligence')}
                        </span>
                        <div className="flex items-center gap-1">
                          {/* Skill Picker Toggle */}
                          <button
                            onClick={() => setShowSkillPicker(v => !v)}
                            title="Switch Lens"
                            className={`text-dark-muted hover:text-accent transition-colors p-1 rounded text-[11px] font-black ${showSkillPicker ? 'text-accent bg-accent/10' : ''}`}
                          >
                            {activeSkill ? activeSkill.icon : '🎭'}
                          </button>
                          {/* New Chat */}
                          <button
                            onClick={() => createNewSession()}
                            title="New Chat"
                            className="text-dark-muted hover:text-signal-buy transition-colors p-1 rounded"
                          >
                            <Edit2 size={14}/>
                          </button>
                          {/* Session History */}
                          <button
                            onClick={() => setShowSessionList(v => !v)}
                            title="All chats"
                            className={`text-dark-muted hover:text-white transition-colors p-1 rounded ${showSessionList ? 'text-white bg-dark-border' : ''}`}
                          >
                            <Clock size={14}/>
                          </button>
                          <button onClick={() => setShowChartChat(false)} className="text-dark-muted hover:text-white p-1">
                            <X size={14}/>
                          </button>
                        </div>
                      </div>

                      {/* Skill Picker Dropdown */}
                      {showSkillPicker && (
                        <div className="border-b border-dark-border bg-gray-950 shrink-0">
                          <div className="p-2">
                            <p className="text-[9px] font-black text-dark-muted uppercase tracking-widest px-2 mb-2">Analyse Through Lens Of</p>
                            {/* Default option */}
                            <button
                              onClick={() => { setActiveSkill(null); setShowSkillPicker(false); }}
                              className={`w-full flex items-center gap-2.5 px-2 py-2 rounded-lg text-left hover:bg-dark-card transition-colors mb-1 ${!activeSkill ? 'bg-accent/10 border border-accent/20' : ''}`}
                            >
                              <span className="text-base">🧠</span>
                              <div>
                                <p className="text-[11px] font-black text-white">Default Chart AI</p>
                                <p className="text-[9px] text-dark-muted">Technical analysis & signals</p>
                              </div>
                              {!activeSkill && <span className="ml-auto text-accent text-[9px] font-black">ACTIVE</span>}
                            </button>
                            {SKILLS.map(skill => (
                              <button
                                key={skill.id}
                                onClick={() => { setActiveSkill(skill); setShowSkillPicker(false); createNewSession(); }}
                                className={`w-full flex items-center gap-2.5 px-2 py-2 rounded-lg text-left hover:bg-dark-card transition-colors ${activeSkill?.id === skill.id ? 'bg-accent/10 border border-accent/20' : ''}`}
                              >
                                <span className="text-base">{skill.icon}</span>
                                <div className="flex-1 min-w-0">
                                  <p className="text-[11px] font-black text-white truncate">{skill.name}</p>
                                  <p className="text-[9px] text-dark-muted truncate">{skill.desc}</p>
                                </div>
                                {activeSkill?.id === skill.id && <span className="ml-auto text-accent text-[9px] font-black shrink-0">ACTIVE</span>}
                              </button>
                            ))}
                          </div>
                        </div>
                      )}

                      {/* Session List Dropdown */}
                      {showSessionList && (
                        <div className="border-b border-dark-border bg-gray-950 shrink-0 max-h-48 overflow-y-auto">
                          {chatSessions.length === 0 ? (
                            <p className="p-3 text-xs text-dark-muted text-center">No chats yet</p>
                          ) : (
                            [...chatSessions].reverse().map(s => (
                              <div key={s.id} className={`flex items-center gap-2 px-3 py-2 hover:bg-dark-card cursor-pointer border-b border-dark-border/50 last:border-0 ${activeChatSessionId === s.id ? 'bg-accent/10' : ''}`}>
                                <div className="flex-1 min-w-0" onClick={() => switchSession(s.id)}>
                                  <p className="text-xs font-medium text-dark-text truncate">
                                    {s.title || 'Untitled'}
                                  </p>
                                  <p className="text-[10px] text-dark-muted">
                                    {new Date(s.createdAt).toLocaleDateString()} · {s.messages.length} messages
                                  </p>
                                </div>
                                <button
                                  onClick={(e) => { e.stopPropagation(); deleteSession(s.id); }}
                                  className="text-dark-muted hover:text-signal-sell shrink-0 p-1"
                                >
                                  <X size={12}/>
                                </button>
                              </div>
                            ))
                          )}
                        </div>
                      )}

                      {/* Messages */}
                      <div
                        className="flex-1 overflow-y-auto p-4 space-y-4 scroll-smooth custom-scrollbar"
                        ref={chatScrollRef}
                      >
                        {chatMessages.length === 0 && !chatLoading && (
                          <div className="flex flex-col items-center justify-center h-full gap-3 text-dark-muted">
                            {activeSkill ? (
                              <>
                                <span className="text-4xl">{activeSkill.icon}</span>
                                <div className="text-center">
                                  <p className="text-sm font-black text-white">{activeSkill.name}</p>
                                  <p className="text-xs text-dark-muted mt-1 max-w-[220px] mx-auto">{activeSkill.desc}</p>
                                  <p className="text-[10px] text-accent mt-2 font-bold">Ask anything — responses are in this persona</p>
                                </div>
                              </>
                            ) : (
                              <>
                                <Brain size={28} className="opacity-30"/>
                                <p className="text-sm text-center">Analyzing chart…<br/><span className="text-xs opacity-60">This may take a few seconds</span></p>
                              </>
                            )}
                          </div>
                        )}
                        {chatMessages.map((msg, i) => (
                          <div key={i} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                            <div className={`max-w-[88%] p-3 rounded-2xl text-sm leading-relaxed whitespace-pre-wrap ${
                              msg.role === 'user'
                                ? 'bg-accent text-white rounded-br-sm'
                                : 'bg-dark-card border border-dark-border text-dark-text rounded-bl-sm'
                            }`}>
                              {msg.content}
                            </div>
                          </div>
                        ))}
                        {chatLoading && (
                          <div className="flex justify-start">
                            <div className="p-3 rounded-2xl bg-dark-card border border-dark-border rounded-bl-sm flex items-center gap-1.5">
                              <div className="w-2 h-2 bg-accent/70 rounded-full animate-bounce"/>
                              <div className="w-2 h-2 bg-accent/70 rounded-full animate-bounce" style={{animationDelay: '150ms'}}/>
                              <div className="w-2 h-2 bg-accent/70 rounded-full animate-bounce" style={{animationDelay: '300ms'}}/>
                            </div>
                          </div>
                        )}
                      </div>

                      {/* Quick Prompt Chips */}
                      {!chatLoading && activeChatSessionId && (
                        <div className="px-3 pb-3 flex flex-wrap gap-2">
                          {[
                            "Is this a good entry right now?",
                            "Identify key support and resistance",
                            "Explain the recent price move",
                            `What's the risk/reward for next 3 months?`,
                            "Set an alert at key support level",
                            "Set a stop loss alert for me",
                            "Alert me at the breakout level",
                          ].map(prompt => (
                            <button
                              key={prompt}
                              onClick={() => {
                                setChatInput(prompt);
                                setTimeout(() => sendChatMessage(prompt), 10);
                              }}
                              className="shrink-0 px-3 py-1.5 bg-dark-card border border-dark-border hover:border-accent hover:bg-accent/5 rounded-full text-[10px] font-bold text-dark-muted hover:text-accent transition-all whitespace-nowrap"
                            >
                              {prompt}
                            </button>
                          ))}
                        </div>
                      )}

                      {/* Skill-specific quick prompts */}
                      {activeSkill && !chatLoading && activeChatSessionId && (
                        <div className="px-3 pb-3 flex flex-wrap gap-1.5">
                          {(activeSkill.id === 'warren_buffett_quality'
                            ? ["Is this a wonderful business at a fair price?", "What is the moat here?", "Would you hold this for 10 years?"]
                            : activeSkill.id === 'sebi_forensic'
                            ? ["Any accounting red flags?", "Is promoter pledge a concern?", "Cash flow vs profit — any divergence?"]
                            : activeSkill.id === 'goldman_screener'
                            ? ["Give me a 12-month price target", "Bull / base / bear scenarios", "Would this pass your institutional screen?"]
                            : activeSkill.id === 'ark_disruptive'
                            ? ["What's the disruption thesis here?", "Where is this on the S-curve?", "5-year TAM estimate?"]
                            : activeSkill.id === 'peter_lynch_simple'
                            ? ["Is this a fast grower or stalwart?", "What's the PEG ratio?", "Would you buy this at current price?"]
                            : activeSkill.id === 'rj_india_growth'
                            ? ["Is this riding India's growth story?", "Any structural tailwind here?", "10-year wealth creator or trap?"]
                            : activeSkill.id === 'sequoia_moat'
                            ? ["Describe the moat in one sentence", "Can margins scale with revenue?", "Pricing power — yes or no?"]
                            : []
                          ).map(prompt => (
                            <button
                              key={prompt}
                              onClick={() => { setChatInput(prompt); setTimeout(() => sendChatMessage(prompt), 10); }}
                              className="shrink-0 px-2.5 py-1 bg-accent/5 border border-accent/20 hover:border-accent hover:bg-accent/10 rounded-full text-[10px] font-bold text-accent transition-all whitespace-nowrap"
                            >
                              {prompt}
                            </button>
                          ))}
                        </div>
                      )}

                      {/* Active Alerts for this stock */}
                      {stockAlerts.length > 0 && (
                        <div className="px-3 pb-2 shrink-0">
                          <p className="text-[9px] font-black text-dark-muted uppercase tracking-widest mb-1.5">Active Alerts</p>
                          <div className="space-y-1">
                            {stockAlerts.map(a => (
                              <div key={a.id} className="flex items-center justify-between bg-dark-bg border border-dark-border rounded-lg px-2.5 py-1.5">
                                <div className="flex items-center gap-1.5 min-w-0">
                                  <Bell size={10} className={a.direction === 'ABOVE' ? 'text-signal-buy' : 'text-signal-sell'} />
                                  <span className="text-[10px] font-bold text-dark-text truncate">{a.label || a.alert_type}</span>
                                  <span className={`text-[10px] font-mono font-black ${a.direction === 'ABOVE' ? 'text-signal-buy' : 'text-signal-sell'}`}>
                                    {a.direction === 'ABOVE' ? '▲' : '▼'} ₹{a.price_level?.toFixed(2)}
                                  </span>
                                </div>
                                <button onClick={() => deleteAlert(a.id)} className="text-dark-muted hover:text-signal-sell p-0.5 shrink-0">
                                  <X size={10}/>
                                </button>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {/* Input Bar */}
                      <div className="p-3 border-t border-dark-border bg-dark-bg flex gap-2 shrink-0">
                        <input
                          type="text"
                          value={chatInput}
                          onChange={(e) => setChatInput(e.target.value)}
                          onKeyDown={(e) => e.key === 'Enter' && !chatLoading && sendChatMessage()}
                          placeholder={activeChatSessionId ? "Ask about this chart…" : "Start a new chat first"}
                          disabled={chatLoading || !activeChatSessionId}
                          className="flex-1 bg-dark-card border border-dark-border rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-accent text-white disabled:opacity-50 placeholder:text-gray-600"
                        />
                        <button
                          onClick={() => sendChatMessage()}
                          disabled={chatLoading || !activeChatSessionId}
                          className="bg-accent text-white p-2 rounded-lg hover:bg-accent-hover disabled:opacity-50 transition-colors shrink-0"
                        >
                          <ArrowUpRight size={18}/>
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          {/* Positions Tab */}
          {activeTab === 'positions' && (
            <div className="space-y-4">
              {lots.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-20 gap-3 text-dark-muted">
                  <Briefcase size={36} className="opacity-30" />
                  <p className="text-sm">No tax lots or positions found.</p>
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-left text-sm whitespace-nowrap">
                    <thead>
                      <tr className="border-b border-dark-border text-dark-muted font-mono uppercase text-xs">
                        <th className="pb-3 font-medium px-4">Buy Date</th>
                        <th className="pb-3 font-medium px-4 text-right">Quantity</th>
                        <th className="pb-3 font-medium px-4 text-right">Buy Price</th>
                        <th className="pb-3 font-medium px-4 text-right">Current Price</th>
                        <th className="pb-3 font-medium px-4 text-right">Unrealised P&L</th>
                      </tr>
                    </thead>
                    <tbody>
                      {lots.map((lot, idx) => {
                        const currentVal = sig.current_price ?? lot.buy_price;
                        const profit = (currentVal - lot.buy_price) * lot.quantity;
                        const profitPct = ((currentVal - lot.buy_price) / lot.buy_price) * 100;
                        const isProfit = profit >= 0;
                        return (
                          <tr key={idx} className="border-b border-dark-border/50 hover:bg-dark-border/20 transition-colors">
                            <td className="py-3 px-4 font-mono">{lot.buy_date}</td>
                            <td className="py-3 px-4 text-right">{lot.quantity.toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
                            <td className="py-3 px-4 text-right font-mono">₹{lot.buy_price.toFixed(2)}</td>
                            <td className="py-3 px-4 text-right font-mono">₹{currentVal.toFixed(2)}</td>
                            <td className={`py-3 px-4 text-right font-mono font-bold ${isProfit ? 'text-signal-buy' : 'text-signal-sell'}`}>
                              {isProfit ? '+' : ''}₹{profit.toFixed(2)} ({isProfit ? '+' : ''}{profitPct.toFixed(2)}%)
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}

          {/* Signals Breakdown Tab */}
          {activeTab === 'signals' && (
            <div className="space-y-4">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-xs font-black text-dark-muted uppercase tracking-widest">Signal Logic & Indicators</h3>
                <button
                  onClick={handleRegenerateSignals}
                  disabled={signalRegening}
                  className="flex items-center gap-1.5 px-3 py-1.5 bg-dark-bg border border-dark-border rounded-lg text-[10px] font-bold text-dark-muted hover:border-dark-text hover:text-dark-text transition-all disabled:opacity-50"
                >
                  <RefreshCw size={12} className={signalRegening ? 'animate-spin' : ''} />
                  Regenerate Signals
                </button>
              </div>
              {!signals ? (
                <div className="flex flex-col items-center justify-center py-20 gap-3 text-dark-muted">
                  <Layers size={36} className="opacity-30" />
                  <p className="text-sm">No signal breakdown available yet. Run EOD consolidation first.</p>
                </div>
              ) : (
                <>
                  <div className="grid grid-cols-2 gap-4 mb-2">
                    {[['Market Session', signals.market_session], ['Computed At', signals.computed_at ? new Date(signals.computed_at).toLocaleString() : '—']].map(([k,v]) => (
                      <div key={k} className="bg-gray-900/50 border border-dark-border rounded-xl p-3">
                        <p className="text-xs text-dark-muted mb-1">{k}</p>
                        <p className="font-mono text-sm">{v}</p>
                      </div>
                    ))}
                  </div>

                  {/* V2 Institutional Pillars */}
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                    {[
                      { label: 'Fundamental', val: signals.fundamental_score, icon: '🏛️' },
                      { label: 'Technical', val: signals.technical_score, icon: '📈' },
                      { label: 'Momentum', val: signals.momentum_score, icon: '🚀' },
                      { label: 'Sector Rank', val: signals.sector_rank_score, icon: '📊' },
                    ].map(p => (
                      <div key={p.label} className="bg-gradient-to-b from-dark-card to-dark-bg border border-dark-border rounded-xl p-4 flex flex-col items-center text-center">
                        <span className="text-xl mb-2">{p.icon}</span>
                        <p className="text-[10px] font-bold text-dark-muted uppercase tracking-widest mb-1">{p.label}</p>
                        <p className={`text-2xl font-black font-mono ${
                          (p.val ?? 0) >= 70 ? 'text-signal-buy' : 
                          (p.val ?? 0) <= 40 ? 'text-signal-sell' : 'text-signal-hold'
                        }`}>
                          {p.val?.toFixed(1) ?? '—'}
                        </p>
                      </div>
                    ))}
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                    <div className="bg-accent/5 border border-accent/20 rounded-xl p-4 flex items-center justify-between">
                       <div>
                          <p className="text-[10px] font-black text-accent uppercase tracking-widest mb-1">Sector Percentile</p>
                          <p className="text-sm text-dark-text">Top {100 - (signals.sector_percentile ?? 0).toFixed(1)}% of peers in {stock?.sector}</p>
                       </div>
                       <div className="text-2xl font-black text-accent font-mono">
                          {signals.sector_percentile?.toFixed(1)}%
                       </div>
                    </div>
                    <div className="bg-dark-border/20 border border-dark-border rounded-xl p-4 flex items-center justify-between">
                       <div>
                          <p className="text-[10px] font-black text-dark-muted uppercase tracking-widest mb-1">Data Confidence</p>
                          <p className="text-sm text-dark-text">{signals.data_quality} / {((signals.data_confidence ?? 0) * 100).toFixed(0)}% completeness</p>
                       </div>
                       <div className={`text-2xl font-black font-mono ${
                          (signals.data_confidence ?? 0) >= 0.8 ? 'text-signal-buy' : 'text-signal-hold'
                       }`}>
                          {((signals.data_confidence ?? 0) * 100).toFixed(0)}%
                       </div>
                    </div>
                  </div>
                  {signals.indicator_breakdown && (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      {Object.entries(signals.indicator_breakdown).map(([group, breakdown]) => (
                        <div key={group} className="bg-gray-900/40 border border-dark-border rounded-xl p-4">
                          <p className="text-xs text-dark-muted font-semibold uppercase tracking-wide mb-3">
                            {group.replace('_', ' ')}
                          </p>
                          <div className="space-y-2">
                            {Object.entries(breakdown).map(([name, data]) => {
                              const LABEL_MAP = {
                                rsi: "RSI", macd: "MACD", price_vs_sma200: "SMA 200", price_vs_sma50: "SMA 50", adx: "ADX Trend", bb_position: "Bollinger", trade_activity: "Vol Shock",
                                roc_1yr: "1Y ROC", roc_60d: "60d ROC", roc_20d: "20d ROC", volume_trend: "Vol Trend", "52w_rank": "52W Rank", rs_vs_nifty: "RS vs Nifty", "52w_high": "52W High", "52w_low": "52W Low", beta: "Beta",
                                pe_vs_5yr: "PE vs 5Y", roe_quality: "ROE", debt_equity: "D/E", revenue_growth_3yr: "Rev Growth", pat_growth_3yr: "PAT Growth", operating_margin: "Margin", pledge_penalty_on_roe: "Pledge Pnlty",
                                institutional_hold: "Institutional", analyst_sentiment: "Analyst", balance_sheet_health: "Bal Sheet", peg_ratio: "PEG Ratio"
                              };
                                const displayName = LABEL_MAP[name] || name.replace(/_/g, ' ');
                                const isLabel = data.label !== undefined;
                                const isMissing = !isLabel && data.score === null;
                                const pct = (!isLabel && !isMissing && data.max > 0) ? (data.score / data.max) * 100 : 0;
                                const barColor = isLabel ? 'bg-indigo-500/50' : (isMissing ? 'bg-gray-800' : (pct >= 66 ? 'bg-signal-buy' : pct >= 33 ? 'bg-signal-hold' : 'bg-signal-sell'));
                                
                                // Format value display
                                let valDisplay = '—';
                                if (isLabel) {
                                  if (name.includes('cross')) {
                                    valDisplay = data.label === 1 ? 'Bull 📈' : data.label === -1 ? 'Bear 📉' : 'None';
                                  } else {
                                    valDisplay = data.label || '—';
                                  }
                                } else if (!isMissing) {
                                  valDisplay = `${Math.round(data.score)}/${Math.round(data.max)}`;
                                }

                                return (
                                  <div key={name} className="flex items-center gap-3">
                                    <span className="text-[10px] font-mono text-dark-muted w-24 shrink-0 truncate uppercase tracking-wider" title={displayName}>{displayName}</span>
                                    <div className="flex-1 h-1.5 bg-gray-700/30 rounded-full overflow-hidden">
                                      <div className={`h-full rounded-full transition-all duration-500 ${barColor}`} style={{ width: `${isMissing ? 0 : (isLabel ? 100 : pct)}%` }} />
                                    </div>
                                    <span className={`font-mono text-[10px] text-right w-16 truncate ${isLabel ? 'text-indigo-400 font-bold' : 'text-dark-muted'}`}>
                                      {isMissing ? 'N/A' : valDisplay}
                                    </span>
                                  </div>
                                );
                            })}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </>
              )}
            </div>
          )}

          {/* Fundamentals Tab */}
          {activeTab === 'fundamentals' && (
            <div>
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-xs font-black text-dark-muted uppercase tracking-widest">Financial Profile</h3>
                <div className="flex items-center gap-2">
                  <button 
                    onClick={handleFundSync}
                    disabled={fundSyncLoading}
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-dark-bg border border-dark-border rounded-lg text-[10px] font-bold text-dark-muted hover:border-dark-text hover:text-dark-text transition-all disabled:opacity-50"
                  >
                    <RefreshCw size={12} className={fundSyncLoading ? 'animate-spin' : ''} />
                    Sync Yahoo
                  </button>
                  <button
                    onClick={handleScreenerSync}
                    disabled={screenerSyncLoading}
                    title="Fill missing fields from Screener.in"
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-dark-bg border border-dark-border rounded-lg text-[10px] font-bold text-dark-muted hover:border-accent/60 hover:text-accent transition-all disabled:opacity-50"
                  >
                    <RefreshCw size={12} className={screenerSyncLoading ? 'animate-spin' : ''} />
                    Sync Screener
                  </button>
                  <button 
                    onClick={() => {
                      setEditData({
                        pe_ratio: fundamentals?.pe_ratio,
                        eps: fundamentals?.eps,
                        roe: fundamentals?.roe,
                        debt_equity: fundamentals?.debt_equity,
                        revenue_growth: fundamentals?.revenue_growth,
                        market_cap: fundamentals?.market_cap,
                        yahoo_symbol: fundamentals?.yahoo_symbol
                      });
                      setIsEditModalOpen(true);
                    }}
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-dark-bg border border-dark-border rounded-lg text-[10px] font-bold text-dark-muted hover:border-dark-text hover:text-dark-text transition-all"
                  >
                    <Edit2 size={12} />
                    Edit Data
                  </button>
                </div>
              </div>

              {!fundamentals ? (
                <div className="flex flex-col items-center justify-center py-20 gap-4 text-dark-muted border-2 border-dashed border-dark-border rounded-2xl">
                  <div className="p-3 bg-dark-bg rounded-xl border border-dark-border">
                    <BookOpen size={24} className="opacity-40" />
                  </div>
                  <div className="text-center p-6">
                    <p className="text-sm font-bold text-dark-text">No Fundamental Profile</p>
                    <p className="text-xs text-dark-muted mt-1 mb-4">Official data for this symbol has not been cached yet.</p>
                    <button
                      onClick={handleAIResearch}
                      disabled={fundResearchLoading}
                      className="flex items-center gap-2 px-4 py-2 bg-accent/20 border border-accent/40 text-accent rounded-xl text-xs font-bold hover:bg-accent/30 transition-all disabled:opacity-50 mx-auto"
                    >
                      {fundResearchLoading ? <RefreshCw size={14} className="animate-spin" /> : <Sparkles size={14} />}
                      {fundResearchLoading ? 'Reseach in progress...' : 'Research via AI'}
                    </button>
                  </div>
                </div>
              ) : (
                <div className="space-y-4">
                  {fundamentals.data_quality === 'AI_RESEARCHED' && (
                    <div className="flex items-center gap-2 p-3 bg-yellow-400/10 border border-yellow-400/20 rounded-xl mb-2">
                       <AlertTriangle size={14} className="text-yellow-400" />
                       <p className="text-[10px] font-bold text-yellow-500/80 uppercase tracking-wider">Note: Displaying AI-estimated fundamentals (Unverified)</p>
                       <button 
                        onClick={handleAIResearch}
                        disabled={fundResearchLoading}
                        className="ml-auto flex items-center gap-1.5 px-2 py-0.5 bg-yellow-400/20 rounded text-[9px] font-black text-yellow-400 hover:bg-yellow-400/30 transition-all"
                       >
                         {fundResearchLoading ? <RefreshCw size={10} className="animate-spin" /> : <RefreshCw size={10} />}
                         Re-Research
                       </button>
                    </div>
                  )}
                  {fundamentals.data_quality === 'VERIFIED' && (
                    <div className="flex items-center gap-2 p-3 bg-signal-buy/10 border border-signal-buy/20 rounded-xl mb-2">
                       <ShieldCheck size={14} className="text-signal-buy" />
                       <p className="text-[10px] font-bold text-signal-buy/80 uppercase tracking-wider">GROUND TRUTH: This data was manually verified and is used for signal scoring.</p>
                    </div>
                  )}
                  <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                    {[
                      { label: 'P/E Ratio', value: fundamentals.pe_ratio?.toFixed(2) ?? '—', sub: 'Trailing' },
                      { label: 'PEG Ratio', value: fundamentals.peg_ratio?.toFixed(2) ?? '—', sub: 'Price/Earnings-to-Growth', highlight: fundamentals.peg_ratio && fundamentals.peg_ratio < 1.0 },
                      { label: 'P/B Ratio', value: fundamentals.pb_ratio?.toFixed(2) ?? '—', sub: 'Price to Book' },
                      { label: 'P/S Ratio', value: fundamentals.ps_ratio?.toFixed(2) ?? '—', sub: 'Price to Sales' },
                      { label: 'ROE', value: fundamentals.roe != null ? `${(fundamentals.roe * 100).toFixed(1)}%` : '—', sub: 'Return on Equity' },
                      { label: 'Debt / Equity', value: fundamentals.debt_equity?.toFixed(2) ?? '—', sub: 'Leverage ratio' },
                      { label: 'Current Ratio', value: fundamentals.current_ratio?.toFixed(2) ?? '—', sub: 'Liquidity index' },
                      { label: 'Analyst Rating', value: fundamentals.analyst_rating?.toFixed(2) ?? '—', sub: fundamentals.recommendation_key || 'No Consensus', highlight: fundamentals.recommendation_key === 'strong_buy' },
                      { label: 'Beta (5Y)', value: fundamentals.beta?.toFixed(2) ?? sig?.beta?.toFixed(2) ?? '—', sub: 'Market Volatility' },
                      { label: 'EV / EBITDA', value: fundamentals.ev_ebitda?.toFixed(1) ?? '—', sub: 'Value Multiple' },
                      { label: 'Institutional Hold', value: fundamentals.held_percent_institutions != null ? `${fundamentals.held_percent_institutions.toFixed(1)}%` : '—', sub: 'Held by Big Money' },
                      { label: 'Total Cash', value: fundamentals.total_cash ? `₹${(fundamentals.total_cash / 1e7).toFixed(1)}Cr` : '—', sub: 'Cash Balance' },
                      { label: 'Total Debt', value: fundamentals.total_debt ? `₹${(fundamentals.total_debt / 1e7).toFixed(1)}Cr` : '—', sub: 'Liabilities' },
                      { label: 'Revenue Growth', value: fundamentals.revenue_growth != null ? `${(fundamentals.revenue_growth * 100).toFixed(1)}%` : '—', sub: 'YoY' },
                      { label: 'Market Cap', value: fundamentals.market_cap ? `₹${(fundamentals.market_cap / 1e9).toFixed(1)}B` : '—', sub: 'Valuation' },
                    ].map(({ label, value, sub, highlight }) => (
                      <div key={label} className={`bg-gray-900/50 border rounded-xl p-4 group transition-colors ${highlight ? 'border-accent/50 bg-accent/5' : 'border-dark-border hover:border-accent/30'}`}>
                        <p className="text-xs text-dark-muted mb-1">{label}</p>
                        <p className={`text-2xl font-bold font-mono transition-colors ${highlight ? 'text-accent' : 'group-hover:text-accent'}`}>{value}</p>
                        <p className="text-xs text-dark-muted/70 mt-1 uppercase text-[10px] font-bold tracking-tight">{sub}</p>
                      </div>
                    ))}
                    <div className="col-span-full mt-4 p-5 bg-dark-bg border border-dark-border rounded-2xl">
                      <h4 className="text-[10px] font-black text-dark-muted uppercase tracking-widest mb-4 flex items-center gap-2">
                        <Zap size={10} className="text-accent" />
                        Price Action Momentum (52-Week Statistics)
                      </h4>
                      <FiftyTwoWeekRange 
                        current={sig?.current_price} 
                        low={fundamentals.fifty_two_week_low || sig?.fifty_two_week_low} 
                        high={fundamentals.fifty_two_week_high || sig?.fifty_two_week_high} 
                      />
                    </div>
                    <div className="col-span-full pt-2">
                      <p className="text-xs text-dark-muted/60 text-right">
                        Last fetched: {new Date(fundamentals.fetched_at).toLocaleString()} · Quality: <span className={
                          fundamentals.data_quality === 'FULL' ? 'text-signal-buy' : 
                          fundamentals.data_quality === 'AI_RESEARCHED' ? 'text-yellow-400 font-bold' :
                          fundamentals.data_quality === 'VERIFIED' ? 'text-accent font-black' :
                          'text-signal-hold'
                        }>{fundamentals.data_quality}</span>
                      </p>
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* AI Insight Tab */}
          {activeTab === 'ai' && (
            <div className="space-y-6">
              <div className="flex flex-col md:flex-row items-start md:items-center justify-between gap-4 p-4 bg-gray-900/50 border border-dark-border rounded-xl">
                <div className="flex-1">
                  <h4 className="text-xs font-bold text-dark-muted uppercase tracking-wider mb-2">Analysis Skill / Persona</h4>
                  <div className="relative group">
                    <select
                      value={selectedSkill}
                      onChange={(e) => setSelectedSkill(e.target.value)}
                      className="w-full bg-dark-bg border border-dark-border text-dark-text text-sm rounded-xl px-4 py-3 appearance-none focus:outline-none focus:border-accent transition-all cursor-pointer hover:border-dark-muted"
                    >
                      {SKILLS.map(s => (
                        <option key={s.id} value={s.id} className="bg-dark-bg">
                          {s.icon} {s.name} — {s.desc}
                        </option>
                      ))}
                    </select>
                    <div className="absolute right-4 top-1/2 -translate-y-1/2 pointer-events-none text-dark-muted">
                      <ArrowRight size={14} className="rotate-90" />
                    </div>
                  </div>
                </div>
                <div className="flex flex-col gap-2">
                  <button
                    onClick={handleGenerateInsight}
                    disabled={insightLoading}
                    className="shrink-0 flex items-center gap-2 px-6 py-3 bg-accent hover:bg-accent/80 disabled:opacity-50 text-white rounded-xl text-xs font-bold transition-all shadow-lg shadow-accent/20"
                  >
                    <RefreshCw size={14} className={insightLoading ? 'animate-spin' : ''} />
                    {insightLoading ? 'Analyzing...' : (insight ? 'Regenerate Insight' : 'Generate Insight')}
                  </button>
                  <button
                    onClick={handleSyncAndResearch}
                    disabled={insightLoading || fundSyncLoading}
                    className="shrink-0 flex items-center gap-2 px-6 py-2 bg-dark-bg border border-dark-border hover:border-accent hover:text-accent disabled:opacity-50 text-dark-muted rounded-xl text-[10px] font-black uppercase tracking-tighter transition-all"
                  >
                    <RefreshCw size={12} className={fundSyncLoading ? 'animate-spin' : ''} />
                    {fundSyncLoading ? 'Syncing...' : 'Sync Yahoo & Research'}
                  </button>
                </div>
              </div>

              <div className="flex flex-col lg:flex-row gap-6">
                {/* History Sidebar */}
                {insightHistory.length > 0 && (
                  <div className="w-full lg:w-64 shrink-0 space-y-3">
                    <div className="flex items-center gap-2 mb-2 px-1">
                      <Clock size={14} className="text-dark-muted" />
                      <h4 className="text-[10px] font-black text-dark-muted uppercase tracking-widest">Analysis History</h4>
                    </div>
                    <div className="flex lg:flex-col gap-2 overflow-x-auto lg:overflow-x-visible pb-2 lg:pb-0 scroll-hide">
                      <button
                        onClick={() => setSelectedHistoryId(null)}
                        className={`flex-1 shrink-0 p-3 rounded-xl border text-left transition-all ${
                          selectedHistoryId === null 
                          ? 'bg-accent/10 border-accent/40 ring-1 ring-accent/20' 
                          : 'bg-dark-card border-dark-border hover:border-dark-muted'
                        }`}
                      >
                        <p className="text-[10px] font-black text-dark-text flex items-center justify-between">
                          <span>LATEST REPORT</span>
                          {selectedHistoryId === null && <span className="w-1.5 h-1.5 rounded-full bg-accent animate-pulse" />}
                        </p>
                        <p className="text-[9px] text-dark-muted mt-1">Current Active Insight</p>
                      </button>
                      
                      {insightHistory.slice(selectedHistoryId === null ? 1 : 0).map(h => {
                        // Skip if it is the current active one already shown at top
                        if (selectedHistoryId === null && h.id === insight?.id) return null;
                        
                        return (
                          <button
                            key={h.id}
                            onClick={() => setSelectedHistoryId(h.id)}
                            className={`flex-1 shrink-0 p-3 rounded-xl border text-left transition-all ${
                              selectedHistoryId === h.id 
                              ? 'bg-accent/10 border-accent/40 ring-1 ring-accent/20' 
                              : 'bg-dark-card border-dark-border hover:border-dark-muted'
                            }`}
                          >
                            <p className="text-[10px] font-bold text-dark-text truncate">
                              {h.skill_id ? h.skill_id.replace(/_/g, ' ').toUpperCase() : 'GENERAL'}
                            </p>
                            <p className="text-[9px] text-dark-muted mt-0.5">
                              {new Date(h.generated_at).toLocaleDateString()} at {new Date(h.generated_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                            </p>
                          </button>
                        );
                      })}
                    </div>
                  </div>
                )}

                {/* Display Panel */}
                <div className="flex-1 min-w-0">
                  {selectedHistoryId !== null && (
                    <div className="mb-4 flex items-center justify-between p-3 bg-accent/5 border border-accent/20 rounded-xl">
                      <div className="flex items-center gap-2">
                        <Clock size={14} className="text-accent" />
                        <span className="text-xs font-bold text-accent">Viewing Historical Report</span>
                      </div>
                      <button 
                        onClick={() => setSelectedHistoryId(null)}
                        className="text-[10px] font-black text-dark-text hover:text-accent transition-colors"
                      >
                        RETURN TO LATEST
                      </button>
                    </div>
                  )}

                  <AIInsightPanel 
                    insight={selectedHistoryId ? insightHistory.find(h => h.id === selectedHistoryId) : insight} 
                    loading={insightLoading} 
                    error={insightError} 
                  />
                  
                  {/* Consensus section moved inside display panel */}
                  <div className="mt-8">
                    {analysis?.consensus && (
                      <div className="p-5 bg-accent/5 border border-accent/20 rounded-2xl">
                      <div className="flex items-center justify-between mb-4">
                        <div className="flex items-center gap-3">
                          <Brain size={20} className="text-accent" />
                          <h3 className="text-sm font-bold uppercase tracking-widest text-dark-text">AI Multi-Skill Consensus</h3>
                        </div>
                        <div className={`px-4 py-1.5 rounded-full text-xs font-black uppercase tracking-tighter shadow-lg shadow-accent/20 ${
                          analysis.consensus.consensus_verdict.includes('BUY') ? 'bg-signal-buy text-white' : 
                          analysis.consensus.consensus_verdict.includes('AVOID') ? 'bg-signal-sell text-white' : 'bg-signal-hold text-white'
                        }`}>
                          {analysis.consensus.consensus_verdict}
                        </div>
                      </div>
                      
                      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                        <div className="md:col-span-2">
                          <p className="text-sm leading-relaxed text-dark-text opacity-90 italic">
                            "{analysis.consensus.executive_summary}"
                          </p>
                          <div className="flex gap-4 mt-4">
                             <div className="flex items-center gap-1.5 text-[10px] font-bold text-signal-buy bg-signal-buy/10 px-2 py-1 rounded">
                                <ArrowUpRight size={12} /> {analysis.consensus.bull_count} BULLS
                             </div>
                             <div className="flex items-center gap-1.5 text-[10px] font-bold text-signal-sell bg-signal-sell/10 px-2 py-1 rounded">
                                <ArrowDownRight size={12} /> {analysis.consensus.bear_count} BEARS
                             </div>
                          </div>
                        </div>

                        {analysis.backtest && (
                          <div className="bg-dark-bg/50 p-4 rounded-xl border border-dark-border">
                            <p className="text-[10px] font-bold text-dark-muted uppercase tracking-widest mb-3 flex items-center gap-1.5">
                              <Shield size={10} /> Backtest Context
                            </p>
                            <div className="space-y-3">
                               <div>
                                  <p className="text-[10px] text-dark-muted leading-none mb-1">9-YR CAGR</p>
                                  <p className="text-lg font-black font-mono text-signal-buy">+{analysis.backtest.cagr}%</p>
                               </div>
                               <div className="flex gap-4">
                                  <div>
                                     <p className="text-[8px] text-dark-muted leading-none mb-1 uppercase">Win Rate</p>
                                     <p className="text-xs font-bold font-mono">{analysis.backtest.win_rate}%</p>
                                  </div>
                                  <div>
                                     <p className="text-[8px] text-dark-muted leading-none mb-1 uppercase">Avg Rtn</p>
                                     <p className="text-xs font-bold font-mono">{analysis.backtest.avg_return}%</p>
                                  </div>
                               </div>
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                </div>
                </div>
              </div>
            </div>
          )}

          {/* ── Historical Data Tab ───────────────────────────────────── */}
          {activeTab === 'historical' && (
            <div className="space-y-2">
              <div className="flex items-center justify-between mb-4">
                <div>
                  <h3 className="text-base font-black text-dark-text uppercase tracking-widest flex items-center gap-2">
                    <Database size={16} className="text-accent" />
                    Historical Price Data
                  </h3>
                  <p className="text-xs text-dark-muted mt-1">
                    {history.length.toLocaleString()} trading sessions · Daily / Monthly / Yearly aggregations
                  </p>
                </div>
              </div>
              <HistoricalPricesTable history={history} symbol={symbol} />
            </div>
          )}

          {activeTab === 'screener' && (
            <div className="space-y-6">
              {/* Header */}
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-base font-black text-dark-text uppercase tracking-widest flex items-center gap-2">
                    <PieChart size={16} className="text-accent" />
                    Screener.in Financial Data
                  </h3>
                  <p className="text-xs text-dark-muted mt-1">
                    {screenerData ? `Last synced: ${new Date(screenerData.fetched_at).toLocaleString()}` : 'No data yet — click Sync Screener in Fundamentals tab'}
                  </p>
                </div>
                <button
                  onClick={async () => {
                    const token = localStorage.getItem('mm_token') || localStorage.getItem('token');
                    toast.loading('Syncing Screener.in...');
                    const r = await fetch(`${API_URL}/api/stock/${symbol}/fundamentals/sync-screener`, {
                      method: 'POST', headers: { 'Authorization': `Bearer ${token}` }
                    });
                    toast.dismiss();
                    if (r.ok) { toast.success('Synced!'); setTimeout(fetchData, 1000); }
                    else toast.error('Sync failed');
                  }}
                  className="flex items-center gap-1.5 px-3 py-1.5 bg-accent/10 hover:bg-accent/20 border border-accent/30 rounded-lg text-accent text-xs font-semibold transition-colors"
                >
                  <RefreshCw size={12} /> Sync Screener
                </button>
              </div>

              {!screenerData ? (
                <div className="flex flex-col items-center justify-center py-20 text-dark-muted">
                  <PieChart size={40} className="opacity-20 mb-3" />
                  <p className="text-sm">No Screener data available.</p>
                  <p className="text-xs mt-1">Click "Sync Screener" to fetch from Screener.in</p>
                </div>
              ) : (
                <>
                  {/* Pros & Cons */}
                  {((screenerData.screener_pros?.length > 0) || (screenerData.screener_cons?.length > 0)) && (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      {screenerData.screener_pros?.length > 0 && (
                        <div className="bg-signal-buy/5 border border-signal-buy/20 rounded-xl p-4">
                          <h4 className="text-xs font-bold text-signal-buy uppercase tracking-widest mb-3 flex items-center gap-1.5">
                            <CheckCircle size={12} /> Pros
                          </h4>
                          <ul className="space-y-1.5">
                            {screenerData.screener_pros.map((p, i) => (
                              <li key={i} className="text-xs text-dark-muted flex items-start gap-1.5">
                                <span className="text-signal-buy mt-0.5">•</span>{p}
                              </li>
                            ))}
                          </ul>
                        </div>
                      )}
                      {screenerData.screener_cons?.length > 0 && (
                        <div className="bg-signal-sell/5 border border-signal-sell/20 rounded-xl p-4">
                          <h4 className="text-xs font-bold text-signal-sell uppercase tracking-widest mb-3 flex items-center gap-1.5">
                            <AlertTriangle size={12} /> Cons
                          </h4>
                          <ul className="space-y-1.5">
                            {screenerData.screener_cons.map((c, i) => (
                              <li key={i} className="text-xs text-dark-muted flex items-start gap-1.5">
                                <span className="text-signal-sell mt-0.5">•</span>{c}
                              </li>
                            ))}
                          </ul>
                        </div>
                      )}
                    </div>
                  )}

                  {/* CAGR Tables */}
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                    {[
                      { label: 'Revenue Growth', fields: ['revenue_cagr_3yr','revenue_cagr_5yr','revenue_cagr_10yr'] },
                      { label: 'Profit Growth', fields: ['profit_cagr_3yr','profit_cagr_5yr','profit_cagr_10yr'] },
                      { label: 'Stock Price CAGR', fields: ['price_cagr_1yr','price_cagr_3yr','price_cagr_5yr','price_cagr_10yr'] },
                      { label: 'Return on Equity', fields: ['roe_avg_3yr','roe_avg_5yr','roe_avg_10yr'] },
                    ].map(({ label, fields }) => (
                      <div key={label} className="bg-dark-card border border-dark-border rounded-xl p-4">
                        <h4 className="text-xs font-bold text-dark-muted uppercase tracking-widest mb-3">{label}</h4>
                        <div className="space-y-2">
                          {fields.map(f => {
                            const periods = { revenue_cagr_3yr:'3Y', revenue_cagr_5yr:'5Y', revenue_cagr_10yr:'10Y', profit_cagr_3yr:'3Y', profit_cagr_5yr:'5Y', profit_cagr_10yr:'10Y', price_cagr_1yr:'1Y', price_cagr_3yr:'3Y', price_cagr_5yr:'5Y', price_cagr_10yr:'10Y', roe_avg_3yr:'3Y Avg', roe_avg_5yr:'5Y Avg', roe_avg_10yr:'10Y Avg' };
                            const v = screenerData[f];
                            const color = v === null || v === undefined ? 'text-dark-muted' : v >= 0 ? 'text-signal-buy' : 'text-signal-sell';
                            return (
                              <div key={f} className="flex justify-between items-center">
                                <span className="text-xs text-dark-muted">{periods[f]}</span>
                                <span className={`text-xs font-bold font-mono ${color}`}>
                                  {v !== null && v !== undefined ? `${v > 0 ? '+' : ''}${v}%` : '—'}
                                </span>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    ))}
                  </div>

                  {/* Key Metrics Row */}
                  <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3">
                    {[
                      { label: 'ROCE', value: screenerData.roce != null ? `${screenerData.roce}%` : '—' },
                      { label: 'Div Yield', value: screenerData.dividend_yield != null ? `${screenerData.dividend_yield}%` : '—' },
                      { label: 'Div Payout', value: screenerData.dividend_payout_pct != null ? `${screenerData.dividend_payout_pct}%` : '—' },
                      { label: 'Face Value', value: screenerData.face_value != null ? `₹${screenerData.face_value}` : '—' },
                      { label: 'Debtor Days', value: screenerData.debtor_days != null ? `${screenerData.debtor_days}d` : '—' },
                      { label: 'WC Days', value: screenerData.working_capital_days != null ? `${screenerData.working_capital_days}d` : '—' },
                    ].map(({ label, value }) => (
                      <div key={label} className="bg-dark-card border border-dark-border rounded-xl p-3 text-center">
                        <div className="text-xs text-dark-muted mb-1">{label}</div>
                        <div className="text-sm font-bold text-dark-text font-mono">{value}</div>
                      </div>
                    ))}
                  </div>

                  {/* Shareholding Pattern */}
                  {(screenerData.promoter_holding != null || screenerData.fii_holding != null) && (
                    <div className="bg-dark-card border border-dark-border rounded-xl p-4">
                      <h4 className="text-xs font-bold text-dark-muted uppercase tracking-widest mb-4 flex items-center gap-1.5">
                        <Users size={12} /> Shareholding Pattern (Latest Quarter)
                      </h4>
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                        {[
                          { label: 'Promoters', value: screenerData.promoter_holding, color: 'text-accent', bar: 'bg-accent' },
                          { label: 'FII', value: screenerData.fii_holding, color: 'text-blue-400', bar: 'bg-blue-400' },
                          { label: 'DII', value: screenerData.dii_holding, color: 'text-purple-400', bar: 'bg-purple-400' },
                          { label: 'Public', value: screenerData.public_holding, color: 'text-yellow-400', bar: 'bg-yellow-400' },
                        ].map(({ label, value, color, bar }) => (
                          <div key={label}>
                            <div className="flex justify-between mb-1">
                              <span className="text-xs text-dark-muted">{label}</span>
                              <span className={`text-xs font-bold font-mono ${color}`}>{value != null ? `${value}%` : '—'}</span>
                            </div>
                            <div className="h-1.5 bg-dark-border rounded-full overflow-hidden">
                              {value != null && <div className={`h-full rounded-full ${bar}`} style={{ width: `${Math.min(value, 100)}%` }} />}
                            </div>
                          </div>
                        ))}
                      </div>
                      {screenerData.promoter_pledge_pct != null && (
                        <div className="mt-3 pt-3 border-t border-dark-border flex items-center justify-between">
                          <span className="text-xs text-dark-muted">Promoter Pledge %</span>
                          <span className={`text-xs font-bold font-mono ${screenerData.promoter_pledge_pct > 20 ? 'text-signal-sell' : screenerData.promoter_pledge_pct > 5 ? 'text-signal-hold' : 'text-signal-buy'}`}>
                            {screenerData.promoter_pledge_pct}%
                          </span>
                        </div>
                      )}
                    </div>
                  )}

                  {/* Shareholding History Table */}
                  {screenerData.shareholding_history?.length > 0 && (() => {
                    const rows = screenerData.shareholding_history;
                    const headers = rows.length > 0 ? Object.keys(rows[0]) : [];
                    const visibleHeaders = headers.slice(0, 9); // max 9 cols
                    return (
                      <div className="bg-dark-card border border-dark-border rounded-xl overflow-hidden">
                        <div className="px-4 py-3 border-b border-dark-border">
                          <h4 className="text-xs font-bold text-dark-muted uppercase tracking-widest flex items-center gap-1.5">
                            <Users size={12} /> Shareholding History
                          </h4>
                        </div>
                        <div className="overflow-x-auto">
                          <table className="w-full text-xs">
                            <thead>
                              <tr className="border-b border-dark-border">
                                {visibleHeaders.map(h => <th key={h} className="px-3 py-2 text-left text-dark-muted font-semibold whitespace-nowrap">{h}</th>)}
                              </tr>
                            </thead>
                            <tbody>
                              {rows.map((row, i) => (
                                <tr key={i} className="border-b border-dark-border/50 hover:bg-dark-border/20">
                                  {visibleHeaders.map(h => (
                                    <td key={h} className="px-3 py-2 text-dark-text font-mono whitespace-nowrap">{row[h] || '—'}</td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    );
                  })()}

                  {/* Quarterly Results Table */}
                  {screenerData.quarterly_results?.length > 0 && (() => {
                    const rows = screenerData.quarterly_results;
                    const headers = rows.length > 0 ? Object.keys(rows[0]) : [];
                    const visibleHeaders = headers.slice(0, 9);
                    return (
                      <div className="bg-dark-card border border-dark-border rounded-xl overflow-hidden">
                        <div className="px-4 py-3 border-b border-dark-border">
                          <h4 className="text-xs font-bold text-dark-muted uppercase tracking-widest">Quarterly Results (Rs. Cr)</h4>
                        </div>
                        <div className="overflow-x-auto">
                          <table className="w-full text-xs">
                            <thead>
                              <tr className="border-b border-dark-border">
                                {visibleHeaders.map(h => <th key={h} className="px-3 py-2 text-left text-dark-muted font-semibold whitespace-nowrap">{h}</th>)}
                              </tr>
                            </thead>
                            <tbody>
                              {rows.map((row, i) => (
                                <tr key={i} className={`border-b border-dark-border/50 hover:bg-dark-border/20 ${i % 2 === 0 ? '' : 'bg-dark-border/10'}`}>
                                  {visibleHeaders.map(h => (
                                    <td key={h} className="px-3 py-2 text-dark-text font-mono whitespace-nowrap">{row[h] || '—'}</td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    );
                  })()}

                  {/* Annual P&L Table */}
                  {screenerData.annual_pnl?.length > 0 && (() => {
                    const rows = screenerData.annual_pnl;
                    const headers = rows.length > 0 ? Object.keys(rows[0]) : [];
                    const visibleHeaders = headers.slice(0, 9);
                    return (
                      <div className="bg-dark-card border border-dark-border rounded-xl overflow-hidden">
                        <div className="px-4 py-3 border-b border-dark-border">
                          <h4 className="text-xs font-bold text-dark-muted uppercase tracking-widest">Annual Profit & Loss (Rs. Cr)</h4>
                        </div>
                        <div className="overflow-x-auto">
                          <table className="w-full text-xs">
                            <thead>
                              <tr className="border-b border-dark-border">
                                {visibleHeaders.map(h => <th key={h} className="px-3 py-2 text-left text-dark-muted font-semibold whitespace-nowrap">{h}</th>)}
                              </tr>
                            </thead>
                            <tbody>
                              {rows.map((row, i) => (
                                <tr key={i} className={`border-b border-dark-border/50 hover:bg-dark-border/20 ${i % 2 === 0 ? '' : 'bg-dark-border/10'}`}>
                                  {visibleHeaders.map(h => (
                                    <td key={h} className="px-3 py-2 text-dark-text font-mono whitespace-nowrap">{row[h] || '—'}</td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    );
                  })()}

                  {/* Balance Sheet Table */}
                  {screenerData.annual_balance_sheet?.length > 0 && (() => {
                    const rows = screenerData.annual_balance_sheet;
                    const headers = rows.length > 0 ? Object.keys(rows[0]) : [];
                    const visibleHeaders = headers.slice(0, 9);
                    return (
                      <div className="bg-dark-card border border-dark-border rounded-xl overflow-hidden">
                        <div className="px-4 py-3 border-b border-dark-border">
                          <h4 className="text-xs font-bold text-dark-muted uppercase tracking-widest">Balance Sheet (Rs. Cr)</h4>
                        </div>
                        <div className="overflow-x-auto">
                          <table className="w-full text-xs">
                            <thead>
                              <tr className="border-b border-dark-border">
                                {visibleHeaders.map(h => <th key={h} className="px-3 py-2 text-left text-dark-muted font-semibold whitespace-nowrap">{h}</th>)}
                              </tr>
                            </thead>
                            <tbody>
                              {rows.map((row, i) => (
                                <tr key={i} className={`border-b border-dark-border/50 hover:bg-dark-border/20 ${i % 2 === 0 ? '' : 'bg-dark-border/10'}`}>
                                  {visibleHeaders.map(h => (
                                    <td key={h} className="px-3 py-2 text-dark-text font-mono whitespace-nowrap">{row[h] || '—'}</td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    );
                  })()}

                  {/* Cash Flows Table */}
                  {screenerData.annual_cashflows?.length > 0 && (() => {
                    const rows = screenerData.annual_cashflows;
                    const headers = rows.length > 0 ? Object.keys(rows[0]) : [];
                    const visibleHeaders = headers.slice(0, 9);
                    return (
                      <div className="bg-dark-card border border-dark-border rounded-xl overflow-hidden">
                        <div className="px-4 py-3 border-b border-dark-border">
                          <h4 className="text-xs font-bold text-dark-muted uppercase tracking-widest">Cash Flows (Rs. Cr)</h4>
                        </div>
                        <div className="overflow-x-auto">
                          <table className="w-full text-xs">
                            <thead>
                              <tr className="border-b border-dark-border">
                                {visibleHeaders.map(h => <th key={h} className="px-3 py-2 text-left text-dark-muted font-semibold whitespace-nowrap">{h}</th>)}
                              </tr>
                            </thead>
                            <tbody>
                              {rows.map((row, i) => (
                                <tr key={i} className={`border-b border-dark-border/50 hover:bg-dark-border/20 ${i % 2 === 0 ? '' : 'bg-dark-border/10'}`}>
                                  {visibleHeaders.map(h => (
                                    <td key={h} className="px-3 py-2 text-dark-text font-mono whitespace-nowrap">{row[h] || '—'}</td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    );
                  })()}

                  {/* Annual Ratios Table */}
                  {screenerData.annual_ratios?.length > 0 && (() => {
                    const rows = screenerData.annual_ratios;
                    const headers = rows.length > 0 ? Object.keys(rows[0]) : [];
                    const visibleHeaders = headers.slice(0, 9);
                    return (
                      <div className="bg-dark-card border border-dark-border rounded-xl overflow-hidden">
                        <div className="px-4 py-3 border-b border-dark-border">
                          <h4 className="text-xs font-bold text-dark-muted uppercase tracking-widest">Annual Ratios</h4>
                        </div>
                        <div className="overflow-x-auto">
                          <table className="w-full text-xs">
                            <thead>
                              <tr className="border-b border-dark-border">
                                {visibleHeaders.map(h => <th key={h} className="px-3 py-2 text-left text-dark-muted font-semibold whitespace-nowrap">{h}</th>)}
                              </tr>
                            </thead>
                            <tbody>
                              {rows.map((row, i) => (
                                <tr key={i} className={`border-b border-dark-border/50 hover:bg-dark-border/20 ${i % 2 === 0 ? '' : 'bg-dark-border/10'}`}>
                                  {visibleHeaders.map(h => (
                                    <td key={h} className="px-3 py-2 text-dark-text font-mono whitespace-nowrap">{row[h] || '—'}</td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    );
                  })()}
                </>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Manual Edit Modal */}
      {isEditModalOpen && (
        <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm">
          <div className="bg-dark-card border border-dark-border w-full max-w-lg rounded-2xl overflow-hidden shadow-2xl animate-in zoom-in-95 duration-200">
            <div className="p-6 border-b border-dark-border flex items-center justify-between">
              <div>
                <h3 className="text-lg font-bold text-dark-text">Edit Fundamentals</h3>
                <p className="text-xs text-dark-muted mt-1">Provide ground truth for {symbol} to improve scoring accuracy.</p>
              </div>
              <button 
                onClick={() => setIsEditModalOpen(false)}
                className="p-2 text-dark-muted hover:text-dark-text transition-colors"
              >
                <X size={20} />
              </button>
            </div>
            
            <form onSubmit={handleManualUpdate} className="p-6 space-y-4">
              <div className="col-span-full">
                <label className="block text-[10px] font-bold text-accent uppercase mb-1.5 flex items-center gap-1.5">
                  <RefreshCw size={10} /> Yahoo Finance Symbol
                </label>
                <input 
                  type="text"
                  placeholder="e.g. RELIANCE.NS"
                  defaultValue={editData.yahoo_symbol}
                  onChange={(e) => setEditData({ ...editData, yahoo_symbol: e.target.value.toUpperCase() })}
                  className="w-full bg-dark-bg border border-accent/20 rounded-xl px-4 py-3 text-sm font-mono focus:border-accent focus:ring-1 focus:ring-accent outline-none transition-all placeholder:text-dark-muted/30"
                />
                <p className="text-[9px] text-dark-muted mt-1.5">When you click "Sync Yahoo", this symbol will be used for the fetch.</p>
              </div>

              <div className="grid grid-cols-2 gap-4">
                {[
                  { id: 'pe_ratio', label: 'P/E Ratio', placeholder: 'e.g. 25.5' },
                  { id: 'eps', label: 'EPS', placeholder: 'e.g. 12.0' },
                  { id: 'roe', label: 'ROE (%)', placeholder: 'e.g. 15.0', transform: (v) => v / 100 },
                  { id: 'debt_equity', label: 'Debt to Equity', placeholder: 'e.g. 0.5' },
                  { id: 'revenue_growth', label: 'Rev Growth (%)', placeholder: 'e.g. 20.0', transform: (v) => v / 100 },
                  { id: 'market_cap', label: 'Market Cap (Amt)', placeholder: 'Full amount in ₹' },
                ].map(field => (
                  <div key={field.id}>
                    <label className="block text-[10px] font-bold text-dark-muted uppercase mb-1.5">{field.label}</label>
                    <input 
                      type="number"
                      step="any"
                      placeholder={field.placeholder}
                      defaultValue={field.id === 'roe' || field.id === 'revenue_growth' ? (editData[field.id] * 100).toFixed(2) : editData[field.id]}
                      onChange={(e) => {
                        let val = parseFloat(e.target.value);
                        if (field.transform) val = field.transform(val);
                        setEditData({ ...editData, [field.id]: val });
                      }}
                      className="w-full bg-dark-bg border border-dark-border rounded-xl px-4 py-2.5 text-sm font-mono focus:border-accent focus:ring-1 focus:ring-accent outline-none transition-all placeholder:text-dark-muted/30"
                    />
                  </div>
                ))}
              </div>
              
              <div className="pt-4 flex gap-3">
                <button 
                  type="button"
                  onClick={() => setIsEditModalOpen(false)}
                  className="flex-1 px-4 py-3 bg-dark-bg border border-dark-border text-dark-text rounded-xl text-xs font-bold hover:bg-gray-800 transition-all"
                >
                  Cancel
                </button>
                <button 
                  type="submit"
                  className="flex-3 px-8 py-3 bg-accent text-white rounded-xl text-xs font-bold hover:bg-accent/80 transition-all shadow-lg shadow-accent/20"
                >
                  Update & Recompute Signals
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Footer disclaimer */}
      <p className="text-center text-xs text-dark-muted/50 mt-6 flex items-center justify-center gap-2">
        <Shield size={12} />
        For informational purposes only. Not financial advice.
      </p>
    </div>
  );
}
