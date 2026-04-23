import React, { useState, useEffect, useCallback } from 'react';
import {
  Brain, RefreshCw, ChevronDown, ChevronUp, AlertCircle, CheckCircle2,
  Clock, TrendingUp, TrendingDown, Minus, Zap, Cpu, DollarSign, Code2,
  FileText, Timer, Hash
} from 'lucide-react';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const TRIGGER_COLORS = {
  MANUAL: 'bg-blue-500/20 text-blue-400 border border-blue-500/30',
  WEEKLY: 'bg-purple-500/20 text-purple-400 border border-purple-500/30',
  PRICE_SPIKE: 'bg-orange-500/20 text-orange-400 border border-orange-500/30',
};

const SENTIMENT_ICON = (score) => {
  if (score === null || score === undefined) return <Minus size={14} className="text-dark-muted" />;
  if (score >= 0.3) return <TrendingUp size={14} className="text-signal-buy" />;
  if (score <= -0.3) return <TrendingDown size={14} className="text-signal-sell" />;
  return <Minus size={14} className="text-signal-hold" />;
};

/* ── Insight Log Card ─────────────────────────────────────────────────── */
function InsightCard({ log }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="bg-dark-card border border-dark-border rounded-2xl overflow-hidden hover:border-accent/30 transition-all duration-200">
      <button className="w-full text-left flex items-center gap-4 p-4 hover:bg-white/5 transition-colors" onClick={() => setExpanded(p => !p)}>
        <div className="p-2 bg-accent/10 rounded-xl shrink-0"><Brain size={16} className="text-accent" /></div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-sm font-black font-mono text-accent">{log.symbol}</span>
            {log.skill_id && <span className="text-[10px] font-bold text-dark-muted bg-dark-bg border border-dark-border px-2 py-0.5 rounded-full">{log.skill_id.replace(/_/g, ' ').toUpperCase()}</span>}
            <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full ${TRIGGER_COLORS[log.trigger_reason] || 'bg-gray-700 text-gray-400'}`}>{log.trigger_reason}</span>
            {log.verdict && <span className={`text-[10px] font-black px-2 py-0.5 rounded-full border ${log.verdict.includes('BUY') ? 'text-signal-buy border-signal-buy/30 bg-signal-buy/10' : log.verdict.includes('AVOID') || log.verdict.includes('SELL') ? 'text-signal-sell border-signal-sell/30 bg-signal-sell/10' : 'text-signal-hold border-signal-hold/20 bg-signal-hold/10'}`}>{log.verdict}</span>}
          </div>
          <p className="text-xs text-dark-muted mt-1 truncate">{log.short_summary || 'No summary available.'}</p>
        </div>
        <div className="flex items-center gap-4 shrink-0">
          <div className="hidden md:flex items-center gap-1.5">
            {SENTIMENT_ICON(log.sentiment_score)}
            {log.sentiment_score !== null && log.sentiment_score !== undefined && (
              <span className={`text-xs font-mono font-bold ${log.sentiment_score >= 0.5 ? 'text-signal-buy' : 'text-signal-sell'}`}>{(log.sentiment_score * 100).toFixed(0)}</span>
            )}
          </div>
          <div className="text-right">
            <p className="text-[10px] text-dark-muted font-mono">{new Date(log.generated_at).toLocaleDateString('en-IN')}</p>
            <p className="text-[10px] text-dark-muted font-mono">{new Date(log.generated_at).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' })}</p>
          </div>
          {expanded ? <ChevronUp size={16} className="text-dark-muted" /> : <ChevronDown size={16} className="text-dark-muted" />}
        </div>
      </button>
      {expanded && (
        <div className="border-t border-dark-border bg-dark-bg/50 p-5 space-y-4 animate-in fade-in slide-in-from-top-2 duration-200">
          {log.long_summary && <div><p className="text-[10px] font-bold text-dark-muted uppercase tracking-widest mb-2 flex items-center gap-1.5"><Brain size={10} /> Full Analysis</p><p className="text-sm text-dark-text/90 leading-relaxed whitespace-pre-line">{log.long_summary}</p></div>}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {log.key_opportunities?.length > 0 && <div><p className="text-[10px] font-bold text-signal-buy uppercase tracking-widest mb-2 flex items-center gap-1.5"><TrendingUp size={10} /> Opportunities</p><ul className="space-y-1.5">{log.key_opportunities.map((o, i) => <li key={i} className="flex items-start gap-2 text-xs text-dark-text/80"><CheckCircle2 size={11} className="text-signal-buy mt-0.5 shrink-0" />{o}</li>)}</ul></div>}
            {log.key_risks?.length > 0 && <div><p className="text-[10px] font-bold text-signal-sell uppercase tracking-widest mb-2 flex items-center gap-1.5"><AlertCircle size={10} /> Risks</p><ul className="space-y-1.5">{log.key_risks.map((r, i) => <li key={i} className="flex items-start gap-2 text-xs text-dark-text/80"><AlertCircle size={11} className="text-signal-sell mt-0.5 shrink-0" />{r}</li>)}</ul></div>}
          </div>
        </div>
      )}
    </div>
  );
}

/* ── API Call Log Card ────────────────────────────────────────────────── */
function CallCard({ call }) {
  const [expanded, setExpanded] = useState(false);
  const isError = call.status === 'ERROR';

  return (
    <div className={`bg-dark-card border rounded-2xl overflow-hidden transition-all duration-200 ${isError ? 'border-signal-sell/30 hover:border-signal-sell/50' : 'border-dark-border hover:border-accent/30'}`}>
      <button className="w-full text-left flex items-center gap-4 p-4 hover:bg-white/5 transition-colors" onClick={() => setExpanded(p => !p)}>
        {/* Status icon */}
        <div className={`p-2 rounded-xl shrink-0 ${isError ? 'bg-signal-sell/10' : 'bg-signal-buy/10'}`}>
          {isError ? <AlertCircle size={16} className="text-signal-sell" /> : <Cpu size={16} className="text-signal-buy" />}
        </div>

        {/* Meta */}
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-sm font-black font-mono text-accent">{call.symbol}</span>
            <span className="text-[10px] font-bold bg-dark-bg border border-dark-border px-2 py-0.5 rounded-full text-dark-muted flex items-center gap-1"><Cpu size={8} />{call.provider.toUpperCase()}</span>
            <span className="text-[10px] font-mono font-bold text-dark-muted bg-dark-bg border border-dark-border px-2 py-0.5 rounded-full">{call.model}</span>
            {call.skill_id && <span className="text-[10px] font-bold text-purple-400 bg-purple-500/10 border border-purple-500/20 px-2 py-0.5 rounded-full">{call.skill_id.replace(/_/g, ' ')}</span>}
            <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full ${TRIGGER_COLORS[call.trigger_reason] || 'bg-gray-700 text-gray-400'}`}>{call.trigger_reason}</span>
          </div>
          {isError && <p className="text-xs text-signal-sell mt-1 truncate">{call.error_message}</p>}
        </div>

        {/* Token + Duration badges */}
        <div className="flex items-center gap-3 shrink-0">
          <div className="hidden md:flex items-center gap-4 text-[10px]">
            <div className="flex items-center gap-1 text-dark-muted font-mono" title="Total tokens"><Hash size={10} /><span className="font-bold">{call.total_tokens?.toLocaleString() ?? '—'}</span></div>
            <div className="flex items-center gap-1 text-dark-muted font-mono" title="Duration"><Timer size={10} /><span className="font-bold">{call.duration_ms ? `${(call.duration_ms / 1000).toFixed(1)}s` : '—'}</span></div>
          </div>
          <div className="text-right">
            <p className="text-[10px] text-dark-muted font-mono">{new Date(call.called_at).toLocaleDateString('en-IN')}</p>
            <p className="text-[10px] text-dark-muted font-mono">{new Date(call.called_at).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}</p>
          </div>
          {expanded ? <ChevronUp size={16} className="text-dark-muted" /> : <ChevronDown size={16} className="text-dark-muted" />}
        </div>
      </button>

      {expanded && (
        <div className="border-t border-dark-border bg-dark-bg/50 p-5 space-y-5 animate-in fade-in slide-in-from-top-2 duration-200">
          {/* Token breakdown */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            {[
              { label: 'Prompt Tokens', value: call.prompt_tokens, icon: FileText, color: 'text-blue-400' },
              { label: 'Completion Tokens', value: call.completion_tokens, icon: Code2, color: 'text-green-400' },
              { label: 'Total Tokens', value: call.total_tokens, icon: Hash, color: 'text-accent' },
              { label: 'Latency', value: call.duration_ms ? `${(call.duration_ms / 1000).toFixed(2)}s` : '—', icon: Timer, color: 'text-orange-400' },
            ].map(({ label, value, icon: Icon, color }) => (
              <div key={label} className="bg-dark-card border border-dark-border rounded-xl p-3">
                <div className="flex items-center gap-1.5 mb-1"><Icon size={10} className={color} /><p className="text-[9px] font-bold text-dark-muted uppercase tracking-widest">{label}</p></div>
                <p className={`text-lg font-black font-mono ${color}`}>{typeof value === 'number' ? value.toLocaleString() : value}</p>
              </div>
            ))}
          </div>

          {/* Request Payload */}
          {call.request_payload && (
            <div>
              <p className="text-[10px] font-bold text-dark-muted uppercase tracking-widest mb-2 flex items-center gap-1.5"><FileText size={10} /> Request Payload (Prompt)</p>
              <pre className="bg-dark-card border border-dark-border rounded-xl p-4 text-[11px] text-dark-text/80 font-mono overflow-x-auto max-h-[300px] overflow-y-auto whitespace-pre-wrap">
                {typeof call.request_payload === 'string' ? call.request_payload : JSON.stringify(call.request_payload, null, 2)}
              </pre>
            </div>
          )}

          {/* Response */}
          {call.response_raw && (
            <div>
              <p className="text-[10px] font-bold text-signal-buy uppercase tracking-widest mb-2 flex items-center gap-1.5"><Code2 size={10} /> AI Response</p>
              <pre className="bg-dark-card border border-dark-border rounded-xl p-4 text-[11px] text-dark-text/80 font-mono overflow-x-auto max-h-[300px] overflow-y-auto whitespace-pre-wrap">
                {typeof call.response_raw === 'string' ? call.response_raw : JSON.stringify(call.response_raw, null, 2)}
              </pre>
            </div>
          )}

          {/* Error */}
          {call.error_message && (
            <div className="p-3 bg-signal-sell/10 border border-signal-sell/20 rounded-xl">
              <p className="text-[10px] font-bold text-signal-sell uppercase tracking-widest mb-1 flex items-center gap-1.5"><AlertCircle size={10} /> Error Detail</p>
              <p className="text-xs text-signal-sell/90 font-mono">{call.error_message}</p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}


/* ── Main Page ────────────────────────────────────────────────────────── */
export default function AILogs() {
  const [insights, setInsights] = useState([]);
  const [calls, setCalls] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('ALL');
  const [search, setSearch] = useState('');
  const [activeTab, setActiveTab] = useState('calls');     // 'insights' | 'calls'

  const fetchAll = useCallback(async () => {
    setLoading(true);
    try {
      const token = localStorage.getItem('token') || localStorage.getItem('mm_token');
      const headers = { 'Authorization': `Bearer ${token}` };
      const [insRes, callsRes] = await Promise.allSettled([
        fetch(`${API_URL}/api/ai-logs?limit=100`, { headers }),
        fetch(`${API_URL}/api/ai-logs/calls?limit=100`, { headers }),
      ]);
      if (insRes.status === 'fulfilled' && insRes.value.ok) setInsights(await insRes.value.json());
      if (callsRes.status === 'fulfilled' && callsRes.value.ok) setCalls(await callsRes.value.json());
    } catch (e) {
      console.error('AI Logs fetch failed:', e);
    }
    setLoading(false);
  }, []);

  useEffect(() => { fetchAll(); }, [fetchAll]);

  const TRIGGERS = ['ALL', 'MANUAL', 'WEEKLY', 'PRICE_SPIKE'];

  const filteredInsights = insights.filter(l => {
    const mt = filter === 'ALL' || l.trigger_reason === filter;
    const ms = !search || l.symbol.toUpperCase().includes(search.toUpperCase()) || (l.skill_id || '').includes(search.toLowerCase());
    return mt && ms;
  });

  const filteredCalls = calls.filter(c => {
    const mt = filter === 'ALL' || c.trigger_reason === filter;
    const ms = !search || c.symbol.toUpperCase().includes(search.toUpperCase()) || (c.skill_id || '').includes(search.toLowerCase()) || c.model.includes(search.toLowerCase());
    return mt && ms;
  });

  // Stats
  const totalTokens = calls.reduce((s, c) => s + (c.total_tokens || 0), 0);
  const totalCalls = calls.length;
  const avgLatency = totalCalls > 0 ? Math.round(calls.reduce((s, c) => s + (c.duration_ms || 0), 0) / totalCalls) : 0;
  const errorCount = calls.filter(c => c.status === 'ERROR').length;

  const TABS = [
    { id: 'calls', label: 'API Calls', Icon: Cpu },
    { id: 'insights', label: 'Insights', Icon: Brain },
  ];

  return (
    <div className="p-6 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-700">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-black text-dark-text flex items-center gap-3">
            <div className="p-2 bg-accent/10 rounded-xl"><Brain size={22} className="text-accent" /></div>
            AI Intelligence Logs
          </h1>
          <p className="text-xs text-dark-muted mt-1 ml-12">Full audit trail — every API call, token count, and payload.</p>
        </div>
        <button onClick={fetchAll} className="flex items-center gap-2 px-4 py-2 bg-accent hover:bg-accent/80 text-white rounded-xl text-xs font-bold transition-all shadow-lg shadow-accent/20">
          <RefreshCw size={13} className={loading ? 'animate-spin' : ''} /> Refresh
        </button>
      </div>

      {/* Stats Row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { label: 'Total API Calls', value: totalCalls, icon: Cpu, color: 'text-accent' },
          { label: 'Total Tokens Used', value: totalTokens.toLocaleString(), icon: Hash, color: 'text-blue-400' },
          { label: 'Avg Latency', value: avgLatency > 0 ? `${(avgLatency / 1000).toFixed(1)}s` : '—', icon: Timer, color: 'text-orange-400' },
          { label: 'Errors', value: errorCount, icon: AlertCircle, color: errorCount > 0 ? 'text-signal-sell' : 'text-signal-buy' },
        ].map(({ label, value, icon: Icon, color }) => (
          <div key={label} className="bg-dark-card border border-dark-border rounded-2xl p-4">
            <div className="flex items-center gap-2 mb-2"><Icon size={13} className={color} /><p className="text-[10px] font-bold text-dark-muted uppercase tracking-widest">{label}</p></div>
            <p className={`text-2xl font-black font-mono ${color}`}>{value}</p>
          </div>
        ))}
      </div>

      {/* Tab Switcher */}
      <div className="flex items-center gap-3 border-b border-dark-border pb-0">
        {TABS.map(({ id, label, Icon }) => (
          <button
            key={id}
            onClick={() => setActiveTab(id)}
            className={`flex items-center gap-2 px-4 py-3 text-xs font-bold uppercase tracking-wider transition-all border-b-2 ${
              activeTab === id
                ? 'border-accent text-accent'
                : 'border-transparent text-dark-muted hover:text-dark-text'
            }`}
          >
            <Icon size={14} /> {label}
            <span className="text-[10px] font-mono bg-dark-bg border border-dark-border px-1.5 py-0.5 rounded ml-1">
              {id === 'calls' ? filteredCalls.length : filteredInsights.length}
            </span>
          </button>
        ))}
      </div>

      {/* Filters */}
      <div className="flex flex-col sm:flex-row gap-3">
        <input
          type="text"
          placeholder="Search by symbol, skill, or model…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="flex-1 bg-dark-card border border-dark-border rounded-xl px-4 py-2 text-sm text-dark-text placeholder-dark-muted focus:outline-none focus:border-accent/50 transition-colors"
        />
        <div className="flex gap-2">
          {TRIGGERS.map(t => (
            <button key={t} onClick={() => setFilter(t)} className={`px-3 py-2 rounded-xl text-[10px] font-bold uppercase tracking-wider transition-all ${filter === t ? 'bg-accent text-white shadow-lg shadow-accent/20' : 'bg-dark-card border border-dark-border text-dark-muted hover:border-accent/30 hover:text-accent'}`}>{t}</button>
          ))}
        </div>
      </div>

      {/* Content */}
      {loading ? (
        <div className="flex items-center justify-center py-20 gap-3 text-dark-muted">
          <RefreshCw size={20} className="animate-spin" /><span className="text-sm">Loading AI logs…</span>
        </div>
      ) : activeTab === 'calls' ? (
        filteredCalls.length === 0 ? (
          <div className="text-center py-20 space-y-3">
            <Cpu size={40} className="mx-auto text-dark-muted/30" />
            <p className="text-sm text-dark-muted">No API calls logged yet.</p>
            <p className="text-xs text-dark-muted/60">Generate an insight from any stock's Deep Dive page to see call details here.</p>
          </div>
        ) : (
          <div className="space-y-3">{filteredCalls.map(c => <CallCard key={c.id} call={c} />)}</div>
        )
      ) : (
        filteredInsights.length === 0 ? (
          <div className="text-center py-20 space-y-3">
            <Brain size={40} className="mx-auto text-dark-muted/30" />
            <p className="text-sm text-dark-muted">No AI analyses found.</p>
          </div>
        ) : (
          <div className="space-y-3">{filteredInsights.map(log => <InsightCard key={log.id} log={log} />)}</div>
        )
      )}
    </div>
  );
}
