import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Search, Calendar, Filter, Loader2, AlertCircle, RefreshCw } from 'lucide-react';
import toast from 'react-hot-toast';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export default function NseActionsPage() {
  const [fromDate, setFromDate] = useState(() => {
    const d = new Date();
    d.setDate(d.getDate() - 30);
    return d.toISOString().split('T')[0];
  });
  const [toDate, setToDate] = useState(() => new Date().toISOString().split('T')[0]);
  const [symbol, setSymbol] = useState('');
  const [actions, setActions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const [allSymbols, setAllSymbols] = useState([]);
  const [suggestions, setSuggestions] = useState([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [syncing, setSyncing] = useState(false);

  const getToken = () => localStorage.getItem('mm_token') || localStorage.getItem('token');

  useEffect(() => {
    // Fetch symbols for autocomplete
    const fetchSymbols = async () => {
      try {
        const res = await axios.get(`${API_URL}/api/stocks/symbols`, {
          headers: { 'Authorization': `Bearer ${getToken()}` }
        });
        setAllSymbols(res.data);
      } catch (err) {
        console.error("Failed to fetch symbols", err);
      }
    };
    fetchSymbols();
    fetchActions();
  }, []);

  const handleSymbolChange = (val) => {
    setSymbol(val);
    if (val.trim().length > 0) {
      const filtered = allSymbols.filter(s => 
        s.symbol.toLowerCase().includes(val.toLowerCase()) || 
        s.name.toLowerCase().includes(val.toLowerCase())
      ).slice(0, 8);
      setSuggestions(filtered);
      setShowSuggestions(true);
    } else {
      setShowSuggestions(false);
    }
  };

  const formatDateForAPI = (dateStr) => {
    if (!dateStr) return '';
    const [y, m, d] = dateStr.split('-');
    return `${d}-${m}-${y}`;
  };

  const fetchActions = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await axios.get(`${API_URL}/api/corporate-actions`, {
        params: {
          from_date: formatDateForAPI(fromDate),
          to_date: formatDateForAPI(toDate),
          symbol: symbol.trim().toUpperCase() || undefined
        },
        headers: { 'Authorization': `Bearer ${getToken()}` }
      });
      setActions(res.data);
      if (res.data.length === 0) {
        toast('No records found for the selected range.', { icon: 'ℹ️' });
      }
    } catch (err) {
      const msg = err.response?.data?.detail || 'Failed to fetch NSE corporate actions';
      setError(msg);
      toast.error(msg);
    } finally {
      setLoading(false);
    }
  };

  const handleSync = async () => {
    setSyncing(true);
    try {
      const res = await axios.post(`${API_URL}/api/corporate-actions/sync`, null, {
        params: {
          from_date: formatDateForAPI(fromDate),
          to_date: formatDateForAPI(toDate),
          symbol: symbol.trim().toUpperCase() || undefined
        },
        headers: { 'Authorization': `Bearer ${getToken()}` }
      });
      toast.success(`Successfully synced ${res.data.synced_count} records from NSE.`);
      fetchActions();
    } catch (err) {
      const msg = err.response?.data?.detail || 'Manual sync failed. Access restricted or NSE servers busy.';
      toast.error(msg);
    } finally {
      setSyncing(false);
    }
  };

  return (
    <div className="p-6 max-w-[1600px] mx-auto space-y-6">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-dark-text tracking-tight flex items-center gap-2">
            NSE Corporate Actions
          </h1>
          <p className="text-dark-muted text-sm">Market-wide dividends, bonus, splits and more from official NSE data.</p>
        </div>
        <button
          onClick={handleSync}
          disabled={syncing || loading}
          className="bg-dark-card border border-dark-border hover:border-accent text-dark-text text-xs font-bold px-4 py-2 rounded-xl transition-all flex items-center gap-2 disabled:opacity-50"
        >
          {syncing ? <Loader2 size={14} className="animate-spin text-accent" /> : <RefreshCw size={14} className="text-accent" />}
          {syncing ? 'Syncing...' : 'Resync from NSE'}
        </button>
      </div>

      {/* Filters */}
      <div className="bg-dark-card border border-dark-border rounded-2xl p-5 shadow-sm">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 items-end">
          <div className="space-y-2">
            <label className="text-[10px] font-bold text-dark-muted uppercase tracking-widest ml-1">From Date</label>
            <div className="relative">
              <Calendar className="absolute left-3 top-1/2 -translate-y-1/2 text-dark-muted" size={14} />
              <input
                type="date"
                value={fromDate}
                onChange={(e) => setFromDate(e.target.value)}
                className="w-full bg-dark-bg border border-dark-border rounded-xl pl-10 pr-4 py-2.5 text-sm text-dark-text focus:border-accent outline-none transition-all"
              />
            </div>
          </div>
          <div className="space-y-2">
            <label className="text-[10px] font-bold text-dark-muted uppercase tracking-widest ml-1">To Date</label>
            <div className="relative">
              <Calendar className="absolute left-3 top-1/2 -translate-y-1/2 text-dark-muted" size={14} />
              <input
                type="date"
                value={toDate}
                onChange={(e) => setToDate(e.target.value)}
                className="w-full bg-dark-bg border border-dark-border rounded-xl pl-10 pr-4 py-2.5 text-sm text-dark-text focus:border-accent outline-none transition-all"
              />
            </div>
          </div>
          <div className="space-y-2 relative">
            <label className="text-[10px] font-bold text-dark-muted uppercase tracking-widest ml-1">Symbol (Optional)</label>
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-dark-muted" size={14} />
              <input
                type="text"
                placeholder="e.g. RELIANCE"
                value={symbol}
                onChange={(e) => handleSymbolChange(e.target.value)}
                onFocus={() => symbol.trim().length > 0 && setShowSuggestions(true)}
                onBlur={() => setTimeout(() => setShowSuggestions(false), 200)}
                className="w-full bg-dark-bg border border-dark-border rounded-xl pl-10 pr-4 py-2.5 text-sm text-dark-text focus:border-accent outline-none transition-all placeholder:text-dark-muted/30"
              />
            </div>
            {/* Autocomplete Dropdown */}
            {showSuggestions && suggestions.length > 0 && (
              <div className="absolute top-full left-0 right-0 z-50 mt-1 bg-dark-card border border-dark-border rounded-xl shadow-2xl overflow-hidden">
                {suggestions.map((s, idx) => (
                  <button
                    key={idx}
                    onClick={() => {
                      setSymbol(s.symbol);
                      setShowSuggestions(false);
                    }}
                    className="w-full px-4 py-2 text-left hover:bg-accent/10 flex flex-col border-b border-dark-border last:border-0 transition-colors"
                  >
                    <span className="text-xs font-bold text-accent font-mono">{s.symbol}</span>
                    <span className="text-[10px] text-dark-muted truncate">{s.name}</span>
                  </button>
                ))}
              </div>
            )}
          </div>
          <button
            onClick={fetchActions}
            disabled={loading}
            className="h-[42px] bg-accent hover:bg-accent/90 text-white font-bold rounded-xl px-8 transition-all flex items-center justify-center gap-2 disabled:opacity-50 shadow-lg shadow-accent/10"
          >
            {loading ? <Loader2 size={18} className="animate-spin" /> : <Filter size={18} />}
            Fetch Actions
          </button>
        </div>
      </div>

      {/* Error State */}
      {error && (
        <div className="bg-signal-sell/10 border border-signal-sell/20 rounded-xl p-4 flex items-center gap-3 text-signal-sell text-sm">
          <AlertCircle size={18} />
          <span>{error}</span>
        </div>
      )}

      {/* Results Table */}
      <div className="bg-dark-card border border-dark-border rounded-2xl overflow-hidden shadow-sm">
        <div className="overflow-x-auto overflow-y-auto max-h-[70vh] custom-scrollbar">
          <table className="w-full text-left border-collapse">
            <thead className="sticky top-0 z-10">
              <tr className="bg-dark-bg border-b border-dark-border">
                <th className="px-5 py-4 text-[10px] font-bold text-dark-muted uppercase tracking-widest">Symbol</th>
                <th className="px-5 py-4 text-[10px] font-bold text-dark-muted uppercase tracking-widest">Series</th>
                <th className="px-5 py-4 text-[10px] font-bold text-dark-muted uppercase tracking-widest">Company</th>
                <th className="px-5 py-4 text-[10px] font-bold text-dark-muted uppercase tracking-widest">Ex-Date</th>
                <th className="px-5 py-4 text-[10px] font-bold text-dark-muted uppercase tracking-widest">Purpose / Subject</th>
                <th className="px-5 py-4 text-[10px] font-bold text-dark-muted uppercase tracking-widest">Record Date</th>
                <th className="px-5 py-4 text-[10px] font-bold text-dark-muted uppercase tracking-widest">BC Start / End</th>
                <th className="px-5 py-4 text-[10px] font-bold text-dark-muted uppercase tracking-widest">ND Start / End</th>
                <th className="px-5 py-4 text-[10px] font-bold text-dark-muted uppercase tracking-widest text-right">Payment Date</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-dark-border">
              {loading ? (
                Array.from({ length: 10 }).map((_, i) => (
                  <tr key={i} className="animate-pulse">
                    {Array.from({ length: 9 }).map((__, j) => (
                      <td key={j} className="px-5 py-4"><div className="h-4 bg-dark-border rounded w-full" /></td>
                    ))}
                  </tr>
                ))
              ) : actions.length > 0 ? (
                actions.map((item, idx) => (
                  <tr key={idx} className="hover:bg-dark-border/10 transition-colors group">
                    <td className="px-5 py-4">
                      <div className="flex items-center gap-2">
                        <span className="font-mono font-bold text-accent">{item.symbol}</span>
                      </div>
                    </td>
                    <td className="px-5 py-4 text-[10px] font-mono text-dark-muted">
                      {item.series || '-'}
                    </td>
                    <td className="px-5 py-4 text-xs font-medium text-dark-text max-w-[200px] truncate" title={item.comp}>
                      {item.comp}
                    </td>
                    <td className="px-5 py-4 font-mono text-xs text-dark-text whitespace-nowrap">
                      {item.exDate || '-'}
                    </td>
                    <td className="px-5 py-4 text-xs text-dark-muted max-w-[250px]">
                      {item.subject || item.purpose || '-'}
                    </td>
                    <td className="px-5 py-4 font-mono text-xs text-dark-text whitespace-nowrap">
                      {item.recDate || '-'}
                    </td>
                    <td className="px-5 py-4 font-mono text-[10px] text-dark-muted whitespace-nowrap">
                      {item.bcStartDate && item.bcStartDate !== '-' ? `${item.bcStartDate} → ${item.bcEndDate}` : '-'}
                    </td>
                    <td className="px-5 py-4 font-mono text-[10px] text-dark-muted whitespace-nowrap">
                      {item.ndStartDate && item.ndStartDate !== '-' ? `${item.ndStartDate} → ${item.ndEndDate}` : '-'}
                    </td>
                    <td className="px-5 py-4 font-mono text-xs text-signal-buy font-bold text-right">
                      {item.paymentDate || '-'}
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan="9" className="px-5 py-16 text-center text-dark-muted text-sm italic">
                    No corporate actions found for the selected range.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
