import React, { useEffect, useState, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useStockStore } from '../store/stockStore';
import {
  ArrowLeft, ArrowUpRight, ArrowDownRight, ArrowRight,
  Activity, Brain, TrendingUp, TrendingDown, Minus,
  RefreshCw, AlertTriangle, CheckCircle, BarChart2,
  Clock, Layers, BookOpen, Briefcase
} from 'lucide-react';

import SignalBadge from '../components/SignalBadge';
import MetricCard from '../components/MetricCard';
import CandlestickChart from '../components/charts/CandlestickChart';
import VolumeChart from '../components/charts/VolumeChart';
import RSIChart from '../components/charts/RSIChart';
import MACDChart from '../components/charts/MACDChart';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const SKILLS = [
  { id: 'goldman_screener', name: 'Goldman Screener', icon: '📊', desc: 'Institutional institutional-grade screening & targets' },
  { id: 'mckinsey_macro', name: 'McKinsey Macro', icon: '🌍', desc: 'Global & domestic macro impact analysis' },
  { id: 'renaissance_patterns', name: 'Renaissance Quant', icon: '🧬', desc: 'Statistical trends & seasonal anomalies' },
  { id: 'bain_strategy', name: 'Bain Strategy', icon: '📉', desc: 'Competitive moat & market share trajectory' },
  { id: 'harvard_dividends', name: 'Harvard Income', icon: '🎓', desc: 'Sustainable yield & compounding projections' },
  { id: 'hindenburg_forensic', name: 'Hindenburg Audit', icon: '🔍', desc: 'Accounting red flags & governance risks' },
  { id: 'ark_disruptive_tech', name: 'ARK Disruptive', icon: '🚀', desc: 'Exponential growth & disruptive tech scout' },
  { id: 'peter_lynch_simple', name: 'Lynch Main St', icon: '🏠', desc: 'Consumer logic & "Invest in what you know"' },
  { id: 'policy_lobbyist', name: 'Policy Lobbyist', icon: '🏛️', desc: 'PLI schemes & regulatory catalyst tracking' },
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
        <p className="text-sm text-dark-muted leading-relaxed">{insight.long_summary}</p>
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
  const [activeTab, setActiveTab] = useState('chart');
  const [selectedSkill, setSelectedSkill] = useState(SKILLS[0].id);
  const [signals, setSignals] = useState(null);
  const [fundamentals, setFundamentals] = useState(null);
  const [lots, setLots] = useState([]);

  const stock = stocks[symbol];
  const sig = stock?.signal || {};

  // Price change
  const priceChange = sig.change_pct;
  const priceColor = priceChange > 0 ? 'text-signal-buy' : priceChange < 0 ? 'text-signal-sell' : 'text-dark-muted';
  const PriceIcon = priceChange > 0 ? TrendingUp : priceChange < 0 ? TrendingDown : Minus;

  const fetchData = useCallback(async () => {
    setHistoryLoading(true);
    setInsightLoading(true);
    setInsightError(false);

    // Parallel fetches
    const [histRes, insightRes, signalsRes, fundRes, lotsRes] = await Promise.allSettled([
      fetch(`${API_URL}/api/stock/${symbol}/history`),
      fetch(`${API_URL}/api/stock/${symbol}/insight`),
      fetch(`${API_URL}/api/stock/${symbol}/signals`),
      fetch(`${API_URL}/api/stock/${symbol}/fundamentals`),
      fetch(`${API_URL}/api/stock/${symbol}/lots`),
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
  }, [symbol]);

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
        body: json.stringify({ skill_id: selectedSkill })
      });
      if (res.ok) {
        // Poll for result after a short delay
        setTimeout(fetchData, 5000);
      } else {
        const err = await res.json();
        alert(err.detail || 'Failed to generate insight');
        setInsightLoading(false);
      }
    } catch (e) {
      setInsightLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    window.addEventListener('generate-insight', handleGenerateInsight);
    return () => window.removeEventListener('generate-insight', handleGenerateInsight);
  }, [fetchData, symbol]);

  // Chart range filtering
  const [range, setRange] = useState('3M');
  const rangeMap = { '1W': 7, '1M': 30, '3M': 90, '6M': 180, '1Y': 365, 'ALL': 9999 };
  const filteredHistory = history.slice(-(rangeMap[range] ?? 90));

  // Volume max for bar scaling
  const maxVol = Math.max(...filteredHistory.map(d => d.volume || 0), 1);

  const tabs = [
    { id: 'chart', label: 'Price Chart', Icon: BarChart2 },
    { id: 'positions', label: 'Tax Lots', Icon: Briefcase },
    { id: 'signals', label: 'Signals', Icon: Layers },
    { id: 'fundamentals', label: 'Fundamentals', Icon: BookOpen },
    { id: 'ai', label: 'AI Insight', Icon: Brain },
  ];

  return (
    <div className="p-6 space-y-6">
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
            <div className="text-center min-w-[100px]">
              <p className="text-xs text-dark-muted mb-2 uppercase tracking-wide font-medium">Confidence</p>
              <div className="flex items-center gap-2">
                <div className="w-20 h-2 bg-gray-700 rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full transition-all ${
                      (sig.confidence_pct ?? 0) >= 70 ? 'bg-signal-buy' :
                      (sig.confidence_pct ?? 0) >= 45 ? 'bg-signal-hold' : 'bg-signal-sell'
                    }`}
                    style={{ width: `${sig.confidence_pct ?? 0}%` }}
                  />
                </div>
                <span className="font-mono font-bold text-sm">{sig.confidence_pct?.toFixed(0) ?? 0}%</span>
              </div>
            </div>
          </div>
        </div>
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
            sig.data_quality === 'HIGH' ? 'text-signal-buy' :
            sig.data_quality === 'MEDIUM' ? 'text-signal-hold' : 'text-dark-muted'
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
            <div>
              {/* Range buttons */}
              <div className="flex gap-2 mb-5">
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
                <>
                  <div className="space-y-4">
                    <CandlestickChart data={filteredHistory} theme={theme} />
                    <VolumeChart data={filteredHistory} theme={theme} />
                    <RSIChart data={filteredHistory} theme={theme} />
                    <MACDChart data={filteredHistory} theme={theme} />
                  </div>

                  {/* OHLC last bar summary */}
                  {filteredHistory.length > 0 && (() => {
                    const last = filteredHistory[filteredHistory.length - 1];
                    return (
                      <div className="flex gap-5 mt-4 pt-4 border-t border-dark-border text-xs font-mono text-dark-muted">
                        <span>O: <span className="text-dark-text">₹{last.open?.toFixed(2)}</span></span>
                        <span>H: <span className="text-signal-buy">₹{last.high?.toFixed(2)}</span></span>
                        <span>L: <span className="text-signal-sell">₹{last.low?.toFixed(2)}</span></span>
                        <span>C: <span className="text-dark-text font-bold">₹{last.close?.toFixed(2)}</span></span>
                        <span>Vol: <span className="text-dark-text">{last.volume?.toLocaleString()}</span></span>
                        <span className="ml-auto text-dark-muted">{last.date}</span>
                      </div>
                    );
                  })()}
                </>
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
                  {signals.indicator_breakdown && (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      {Object.entries(signals.indicator_breakdown).map(([group, breakdown]) => (
                        <div key={group} className="bg-gray-900/40 border border-dark-border rounded-xl p-4">
                          <p className="text-xs text-dark-muted font-semibold uppercase tracking-wide mb-3">
                            {group === 'short_term' ? '📈 Short-Term Indicators' : '📊 Long-Term Indicators'}
                          </p>
                          <div className="space-y-2">
                            {Object.entries(breakdown).map(([name, data]) => {
                              const pct = data.max > 0 ? (data.score / data.max) * 100 : 0;
                              const barColor = pct >= 66 ? 'bg-signal-buy' : pct >= 33 ? 'bg-signal-hold' : 'bg-signal-sell';
                              return (
                                <div key={name} className="flex items-center gap-3">
                                  <span className="text-xs font-mono text-dark-muted w-20 shrink-0">{name}</span>
                                  <div className="flex-1 h-1.5 bg-gray-700 rounded-full overflow-hidden">
                                    <div className={`h-full rounded-full ${barColor}`} style={{ width: `${pct}%` }} />
                                  </div>
                                  <span className="font-mono text-xs text-right w-12">{data.score}/{data.max}</span>
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
              {!fundamentals ? (
                <div className="flex flex-col items-center justify-center py-20 gap-4 text-dark-muted border-2 border-dashed border-dark-border rounded-2xl">
                  <div className="p-3 bg-dark-bg rounded-xl border border-dark-border">
                    <BookOpen size={24} className="opacity-40" />
                  </div>
                  <div className="text-center">
                    <p className="text-sm font-medium text-dark-text">No Fundamental Profile</p>
                    <p className="text-xs text-dark-muted mt-1">Data for this symbol has not been cached yet.</p>
                  </div>
                </div>
              ) : (
                <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                  {[
                    { label: 'P/E Ratio', value: fundamentals.pe_ratio?.toFixed(2) ?? '—', sub: 'Trailing' },
                    { label: 'EPS', value: fundamentals.eps != null ? `₹${fundamentals.eps.toFixed(2)}` : '—', sub: 'Trailing 12m' },
                    { label: 'ROE', value: fundamentals.roe != null ? `${(fundamentals.roe * 100).toFixed(1)}%` : '—', sub: 'Return on Equity' },
                    { label: 'Debt / Equity', value: fundamentals.debt_equity?.toFixed(2) ?? '—', sub: 'Leverage ratio' },
                    { label: 'Revenue Growth', value: fundamentals.revenue_growth != null ? `${(fundamentals.revenue_growth * 100).toFixed(1)}%` : '—', sub: 'YoY' },
                    { label: 'Market Cap', value: fundamentals.market_cap ? `₹${(fundamentals.market_cap / 1e9).toFixed(1)}B` : '—', sub: 'In billions' },
                  ].map(({ label, value, sub }) => (
                    <div key={label} className="bg-gray-900/50 border border-dark-border rounded-xl p-4">
                      <p className="text-xs text-dark-muted mb-1">{label}</p>
                      <p className="text-2xl font-bold font-mono">{value}</p>
                      <p className="text-xs text-dark-muted/70 mt-1">{sub}</p>
                    </div>
                  ))}
                  <div className="col-span-full">
                    <p className="text-xs text-dark-muted/60 text-right">
                      Last fetched: {new Date(fundamentals.fetched_at).toLocaleString()} · Quality: <span className={fundamentals.data_quality === 'FULL' ? 'text-signal-buy' : 'text-signal-hold'}>{fundamentals.data_quality}</span>
                    </p>
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
                  <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
                    {SKILLS.map(s => (
                      <button
                        key={s.id}
                        onClick={() => setSelectedSkill(s.id)}
                        className={`flex items-center gap-2 p-2 rounded-lg text-left transition-all border ${
                          selectedSkill === s.id 
                          ? 'bg-accent/10 border-accent/40 text-accent' 
                          : 'bg-dark-bg border-dark-border text-dark-muted hover:border-dark-muted'
                        }`}
                      >
                        <span className="text-base">{s.icon}</span>
                        <div className="min-w-0">
                          <p className="text-[10px] font-bold truncate leading-none">{s.name}</p>
                          <p className="text-[8px] opacity-60 truncate mt-0.5">{s.desc}</p>
                        </div>
                      </button>
                    ))}
                  </div>
                </div>
                <button
                  onClick={handleGenerateInsight}
                  disabled={insightLoading}
                  className="shrink-0 flex items-center gap-2 px-6 py-3 bg-accent hover:bg-accent/80 disabled:opacity-50 text-white rounded-xl text-xs font-bold transition-all shadow-lg shadow-accent/20"
                >
                  <RefreshCw size={14} className={insightLoading ? 'animate-spin' : ''} />
                  {insightLoading ? 'Analyzing...' : (insight ? 'Regenerate Analysis' : 'Generate Intelligence')}
                </button>
              </div>

              <div className="pt-2">
                <AIInsightPanel
                  insight={insight}
                  loading={insightLoading}
                  error={insightError && !insightLoading}
                />
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Footer disclaimer */}
      <p className="text-center text-xs text-dark-muted/50 mt-6 flex items-center justify-center gap-2">
        <Shield size={12} />
        For informational purposes only. Not financial advice.
      </p>
    </div>
  );
}
