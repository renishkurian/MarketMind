import React, { useState, useMemo } from 'react';
import { ChevronLeft, ChevronRight, TrendingUp, TrendingDown, Minus, Download } from 'lucide-react';

// ── Aggregation helpers ─────────────────────────────────────────────────────

function groupByMonth(rows) {
  const map = new Map();
  for (const r of rows) {
    const key = r.date.slice(0, 7); // e.g. "2026-04"
    if (!map.has(key)) {
      map.set(key, { ...r, _rows: [r] });
    } else {
      const g = map.get(key);
      g._rows.push(r);
      g.high   = Math.max(g.high ?? r.high, r.high ?? 0);
      g.low    = Math.min(g.low  ?? r.low,  r.low  ?? Infinity);
      g.volume = (g.volume ?? 0) + (r.volume ?? 0);
      g.close  = r.close;               // last close of the period
    }
  }
  // Fix open to first row value
  for (const [, g] of map) {
    g.open = g._rows[0].open;
    g.date = g._rows[0].date.slice(0, 7);
    delete g._rows;
  }
  return [...map.values()].reverse();
}

function groupByYear(rows) {
  const map = new Map();
  for (const r of rows) {
    const key = r.date.slice(0, 4);
    if (!map.has(key)) {
      map.set(key, { ...r, _rows: [r] });
    } else {
      const g = map.get(key);
      g._rows.push(r);
      g.high   = Math.max(g.high ?? r.high, r.high ?? 0);
      g.low    = Math.min(g.low  ?? r.low,  r.low  ?? Infinity);
      g.volume = (g.volume ?? 0) + (r.volume ?? 0);
      g.close  = r.close;
    }
  }
  for (const [, g] of map) {
    g.open = g._rows[0].open;
    g.date = g._rows[0].date.slice(0, 4);
    delete g._rows;
  }
  return [...map.values()].reverse();
}

function formatDate(dateStr, timeframe) {
  if (!dateStr) return '—';
  if (timeframe === 'yearly')  return dateStr; // just year "2026"
  if (timeframe === 'monthly') {
    const [y, m] = dateStr.split('-');
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    return `${months[parseInt(m, 10) - 1]} ${y}`;
  }
  // daily
  const d = new Date(dateStr);
  return d.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' });
}

function fmt(v, decimals = 2) {
  if (v == null) return '—';
  return Number(v).toLocaleString('en-IN', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

function fmtVol(v) {
  if (v == null) return '—';
  if (v >= 1_00_00_000) return (v / 1_00_00_000).toFixed(2) + ' Cr';
  if (v >= 1_00_000)    return (v / 1_00_000).toFixed(2) + ' L';
  return Number(v).toLocaleString('en-IN');
}

// ── Download helper ─────────────────────────────────────────────────────────

function downloadCSV(rows, symbol, timeframe) {
  const headers = ['Date', 'Open', 'High', 'Low', 'Close', 'Change %', 'Volume'];
  const csvRows = [headers.join(',')];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const prev = rows[i + 1];
    const chg = prev ? (((r.close - prev.close) / prev.close) * 100).toFixed(2) : '';
    csvRows.push([
      formatDate(r.date, timeframe),
      r.open ?? '', r.high ?? '', r.low ?? '',
      r.close ?? '', chg, r.volume ?? ''
    ].join(','));
  }
  const blob = new Blob([csvRows.join('\n')], { type: 'text/csv' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href     = url;
  a.download = `${symbol}_historical_${timeframe}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

// ── Main Component ──────────────────────────────────────────────────────────

const PAGE_SIZES = [20, 50, 100];

export default function HistoricalPricesTable({ history = [], symbol = '' }) {
  const [timeframe, setTimeframe] = useState('daily');
  const [page, setPage]           = useState(1);
  const [pageSize, setPageSize]   = useState(20);
  const [selectedYear, setSelectedYear] = useState('All');

  // Derive sorted daily rows and available years
  const sortedDaily = useMemo(
    () => [...history].sort((a, b) => b.date.localeCompare(a.date)),
    [history]
  );

  const availableYears = useMemo(() => {
    const years = [...new Set(sortedDaily.map(r => r.date.slice(0, 4)))];
    return years.sort((a, b) => b.localeCompare(a)); // newest first
  }, [sortedDaily]);

  // Apply year filter first (only meaningful for daily/monthly; for yearly it's a single row so no filter)
  const yearFiltered = useMemo(() => {
    if (selectedYear === 'All' || timeframe === 'yearly') return sortedDaily;
    return sortedDaily.filter(r => r.date.startsWith(selectedYear));
  }, [sortedDaily, selectedYear, timeframe]);

  const grouped = useMemo(() => {
    if (timeframe === 'monthly') return groupByMonth(yearFiltered);
    if (timeframe === 'yearly')  return groupByYear(sortedDaily); // yearly always shows all
    return yearFiltered;
  }, [yearFiltered, sortedDaily, timeframe]);

  const totalPages = Math.max(1, Math.ceil(grouped.length / pageSize));
  const pageStart  = (page - 1) * pageSize;
  const pageRows   = grouped.slice(pageStart, pageStart + pageSize);

  // Helper: compute change % from next row (which is the older period)
  function getChange(idx) {
    const globalIdx = pageStart + idx;
    const prev = grouped[globalIdx + 1];
    if (!prev) return null;
    return ((grouped[globalIdx].close - prev.close) / prev.close) * 100;
  }

  const changeToPage = (n) => setPage(Math.min(Math.max(1, n), totalPages));
  const onTimeframeChange = (tf) => { setTimeframe(tf); setPage(1); };
  const onPageSizeChange  = (ps) => { setPageSize(ps); setPage(1); };
  const onYearChange      = (yr) => { setSelectedYear(yr); setPage(1); };

  if (!history.length) {
    return (
      <div className="flex items-center justify-center py-20 text-dark-muted text-sm italic">
        No historical price data available for this symbol.
      </div>
    );
  }

  return (
    <div className="space-y-5">
      {/* ── Controls bar ── */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        {/* Timeframe toggle */}
        <div className="flex bg-dark-bg border border-dark-border rounded-xl p-1 gap-1">
          {['daily', 'monthly', 'yearly'].map(tf => (
            <button
              key={tf}
              onClick={() => onTimeframeChange(tf)}
              className={`px-4 py-1.5 rounded-lg text-xs font-bold uppercase tracking-widest transition-all
                ${timeframe === tf
                  ? 'bg-accent text-white shadow-lg shadow-accent/30'
                  : 'text-dark-muted hover:text-dark-text'}`}
            >
              {tf}
            </button>
          ))}
        </div>

        {/* Year filter (hidden in yearly mode since it's redundant) */}
        {timeframe !== 'yearly' && (
          <div className="flex items-center gap-2">
            <span className="text-xs text-dark-muted font-bold uppercase tracking-widest">Year:</span>
            <div className="flex flex-wrap gap-1.5">
              {['All', ...availableYears].map(yr => (
                <button
                  key={yr}
                  onClick={() => onYearChange(yr)}
                  className={`px-3 py-1 rounded-lg text-xs font-bold border transition-all
                    ${selectedYear === yr
                      ? 'bg-accent border-accent text-white shadow shadow-accent/30'
                      : 'border-dark-border text-dark-muted hover:text-dark-text hover:border-accent/40'}`}
                >
                  {yr}
                </button>
              ))}
            </div>
          </div>
        )}

        <div className="flex items-center gap-3">
          {/* Rows per page */}
          <div className="flex items-center gap-2 text-xs text-dark-muted">
            <span>Show:</span>
            <select
              value={pageSize}
              onChange={e => onPageSizeChange(Number(e.target.value))}
              className="bg-dark-bg border border-dark-border rounded-lg px-2 py-1 text-dark-text text-xs focus:outline-none focus:border-accent"
            >
              {PAGE_SIZES.map(s => <option key={s} value={s}>{s} rows</option>)}
            </select>
          </div>

          {/* CSV Download */}
          <button
            onClick={() => downloadCSV(grouped, symbol, timeframe)}
            className="flex items-center gap-2 px-3 py-1.5 bg-dark-card border border-dark-border rounded-xl text-dark-muted hover:text-accent hover:border-accent/40 transition-all text-xs font-bold"
          >
            <Download size={13} />
            Export CSV
          </button>
        </div>
      </div>

      {/* ── Table ── */}
      <div className="overflow-x-auto rounded-2xl border border-dark-border">
        <table className="w-full border-collapse">
          <thead>
            <tr className="bg-dark-bg/80 text-[10px] text-dark-muted uppercase tracking-widest font-bold">
              <th className="text-left px-5 py-3.5 border-b border-dark-border">Date</th>
              <th className="text-right px-4 py-3.5 border-b border-dark-border">Open</th>
              <th className="text-right px-4 py-3.5 border-b border-dark-border">High</th>
              <th className="text-right px-4 py-3.5 border-b border-dark-border">Low</th>
              <th className="text-right px-4 py-3.5 border-b border-dark-border">Close</th>
              <th className="text-right px-4 py-3.5 border-b border-dark-border">Change %</th>
              <th className="text-right px-5 py-3.5 border-b border-dark-border">Volume</th>
            </tr>
          </thead>
          <tbody>
            {pageRows.map((row, idx) => {
              const chg = getChange(idx);
              const isPos = chg > 0;
              const isNeg = chg < 0;
              return (
                <tr
                  key={row.date}
                  className="border-b border-dark-border/50 hover:bg-accent/5 transition-colors group"
                >
                  <td className="px-5 py-3 text-sm font-bold text-dark-text whitespace-nowrap">
                    {formatDate(row.date, timeframe)}
                  </td>
                  <td className="px-4 py-3 text-right text-sm font-mono text-dark-muted">
                    ₹{fmt(row.open)}
                  </td>
                  <td className="px-4 py-3 text-right text-sm font-mono text-signal-buy">
                    ₹{fmt(row.high)}
                  </td>
                  <td className="px-4 py-3 text-right text-sm font-mono text-signal-sell">
                    ₹{fmt(row.low)}
                  </td>
                  <td className="px-4 py-3 text-right text-sm font-mono font-bold text-dark-text">
                    ₹{fmt(row.close)}
                  </td>
                  <td className="px-4 py-3 text-right">
                    {chg == null ? (
                      <span className="text-dark-muted text-xs">—</span>
                    ) : (
                      <span className={`inline-flex items-center gap-1 text-sm font-bold font-mono
                        ${isPos ? 'text-signal-buy' : isNeg ? 'text-signal-sell' : 'text-dark-muted'}`}>
                        {isPos ? <TrendingUp size={12} /> : isNeg ? <TrendingDown size={12} /> : <Minus size={12} />}
                        {isPos ? '+' : ''}{chg.toFixed(2)}%
                      </span>
                    )}
                  </td>
                  <td className="px-5 py-3 text-right text-sm font-mono text-dark-muted">
                    {fmtVol(row.volume)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* ── Pagination ── */}
      <div className="flex items-center justify-between flex-wrap gap-4 pt-2">
        <span className="text-xs text-dark-muted">
          Showing <span className="text-dark-text font-bold">{pageStart + 1}–{Math.min(pageStart + pageSize, grouped.length)}</span> of <span className="text-dark-text font-bold">{grouped.length}</span> {timeframe} records
        </span>

        <div className="flex items-center gap-2">
          <button
            onClick={() => changeToPage(1)}
            disabled={page === 1}
            className="px-2 py-1.5 rounded-lg border border-dark-border text-dark-muted hover:text-dark-text hover:border-accent/40 disabled:opacity-30 transition-all text-xs font-bold"
          >
            «
          </button>
          <button
            onClick={() => changeToPage(page - 1)}
            disabled={page === 1}
            className="p-1.5 rounded-lg border border-dark-border text-dark-muted hover:text-dark-text hover:border-accent/40 disabled:opacity-30 transition-all"
          >
            <ChevronLeft size={14} />
          </button>

          {/* Page pills */}
          {Array.from({ length: Math.min(7, totalPages) }, (_, i) => {
            let p;
            if (totalPages <= 7) {
              p = i + 1;
            } else if (page <= 4) {
              p = i + 1;
            } else if (page >= totalPages - 3) {
              p = totalPages - 6 + i;
            } else {
              p = page - 3 + i;
            }
            return (
              <button
                key={p}
                onClick={() => changeToPage(p)}
                className={`min-w-[32px] py-1 rounded-lg text-xs font-bold border transition-all
                  ${p === page
                    ? 'bg-accent border-accent text-white shadow-md shadow-accent/30'
                    : 'border-dark-border text-dark-muted hover:text-dark-text hover:border-accent/40'}`}
              >
                {p}
              </button>
            );
          })}

          <button
            onClick={() => changeToPage(page + 1)}
            disabled={page === totalPages}
            className="p-1.5 rounded-lg border border-dark-border text-dark-muted hover:text-dark-text hover:border-accent/40 disabled:opacity-30 transition-all"
          >
            <ChevronRight size={14} />
          </button>
          <button
            onClick={() => changeToPage(totalPages)}
            disabled={page === totalPages}
            className="px-2 py-1.5 rounded-lg border border-dark-border text-dark-muted hover:text-dark-text hover:border-accent/40 disabled:opacity-30 transition-all text-xs font-bold"
          >
            »
          </button>
        </div>
      </div>
    </div>
  );
}
