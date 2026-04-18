import React, { useState, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { ArrowUpDown, ArrowUp, ArrowDown } from 'lucide-react';
import SignalBadge from './SignalBadge';
import DataQualityFlag from './DataQualityFlag';

const SortIcon = ({ field, sortField, sortDir }) => {
  if (sortField !== field) return <ArrowUpDown size={11} className="opacity-30 ml-1 inline" />;
  return sortDir === 'asc'
    ? <ArrowUp size={11} className="text-accent ml-1 inline" />
    : <ArrowDown size={11} className="text-accent ml-1 inline" />;
};

const Th = ({ label, field, sortField, sortDir, onSort, align = 'left' }) => (
  <th
    className={`px-4 py-3 font-semibold uppercase tracking-wider text-[11px] cursor-pointer select-none hover:text-accent transition-colors text-${align}`}
    onClick={() => onSort(field)}
  >
    {label}
    <SortIcon field={field} sortField={sortField} sortDir={sortDir} />
  </th>
);

export default function PortfolioTable({ stocks }) {
  const navigate = useNavigate();
  const [sortField, setSortField] = useState('buy_date');
  const [sortDir, setSortDir] = useState('asc');
  const [page, setPage] = useState(1);
  const PAGE_SIZE = 20;

  const handleSort = (field) => {
    if (sortField === field) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDir('asc');
    }
    setPage(1);
  };

  const hasPosition = stocks.some(s => s.quantity);

  const sorted = useMemo(() => {
    return [...stocks].sort((a, b) => {
      let aVal, bVal;
      const aSig = a.signal || {};
      const bSig = b.signal || {};

      switch (sortField) {
        case 'symbol':
          aVal = a.symbol ?? '';
          bVal = b.symbol ?? '';
          break;
        case 'price':
          aVal = aSig.current_price ?? 0;
          bVal = bSig.current_price ?? 0;
          break;
        case 'change_pct':
          aVal = aSig.change_pct ?? 0;
          bVal = bSig.change_pct ?? 0;
          break;
        case 'confidence':
          aVal = aSig.confidence_pct ?? 0;
          bVal = bSig.confidence_pct ?? 0;
          break;
        case 'buy_date':
          aVal = a.buy_date ?? '';
          bVal = b.buy_date ?? '';
          break;
        case 'profit_pct': {
          const aCur = aSig.current_price;   // null if no price yet
          const bCur = bSig.current_price;
          aVal = (a.avg_buy_price && aCur) ? ((aCur - parseFloat(a.avg_buy_price)) / parseFloat(a.avg_buy_price)) * 100 : null;
          bVal = (b.avg_buy_price && bCur) ? ((bCur - parseFloat(b.avg_buy_price)) / parseFloat(b.avg_buy_price)) * 100 : null;
          if (aVal === null) aVal = -Infinity;
          if (bVal === null) bVal = -Infinity;
          break;
        }
        default:
          aVal = 0;
          bVal = 0;
      }

      if (typeof aVal === 'string') {
        const cmp = aVal.localeCompare(bVal);
        return sortDir === 'asc' ? cmp : -cmp;
      }
      return sortDir === 'asc' ? aVal - bVal : bVal - aVal;
    });
  }, [stocks, sortField, sortDir]);

  const totalPages = Math.ceil(sorted.length / PAGE_SIZE);
  const paginated = sorted.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  return (
    <div className="bg-dark-card border border-dark-border rounded-xl overflow-hidden shadow-sm">
      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left">
          <thead className="bg-gray-900/60 text-dark-muted border-b border-dark-border">
            <tr>
              <Th label="Symbol"       field="symbol"      sortField={sortField} sortDir={sortDir} onSort={handleSort} />
              <th className="px-4 py-3 font-semibold uppercase tracking-wider text-[11px]">Company / Sector</th>
              <Th label="First Bought" field="buy_date"    sortField={sortField} sortDir={sortDir} onSort={handleSort} />
              <Th label="Price"        field="price"       sortField={sortField} sortDir={sortDir} onSort={handleSort} align="right" />
              <Th label="Change"       field="change_pct"  sortField={sortField} sortDir={sortDir} onSort={handleSort} align="right" />
              <th className="px-4 py-3 font-semibold uppercase tracking-wider text-[11px] text-center">Short</th>
              <th className="px-4 py-3 font-semibold uppercase tracking-wider text-[11px] text-center">Long</th>
              <Th label="Confidence"   field="confidence"  sortField={sortField} sortDir={sortDir} onSort={handleSort} align="center" />
              {hasPosition && (
                <th className="px-4 py-3 font-semibold uppercase tracking-wider text-[11px] text-right">Position</th>
              )}
              {hasPosition && (
                <Th label="Profit %" field="profit_pct" sortField={sortField} sortDir={sortDir} onSort={handleSort} align="right" />
              )}
            </tr>
          </thead>
          <tbody className="divide-y divide-dark-border">
            {paginated.map((stock) => {
              const sig = stock.signal || {};
              const flashClass = stock._lastPriceChange === 'up' ? 'flash-green'
                : stock._lastPriceChange === 'down' ? 'flash-red' : '';

              // Only compute profit when we have a real live/last-close price
              const curPrice = sig.current_price;   // null → no data yet
              const avgBuy = stock.avg_buy_price ? parseFloat(stock.avg_buy_price) : null;
              const profitPct = (curPrice && avgBuy)
                ? ((curPrice - avgBuy) / avgBuy) * 100
                : null;
              const isProfit = profitPct !== null && profitPct >= 0;

              return (
                <tr
                  key={stock.symbol}
                  onClick={() => navigate(`/stock/${stock.symbol}`)}
                  className="hover:bg-gray-800/40 cursor-pointer transition-colors group"
                >
                  {/* Symbol */}
                  <td className="px-4 py-3">
                    <div className="flex flex-col">
                      <span className="font-mono font-bold text-accent group-hover:underline text-base">
                        {stock.symbol}
                      </span>
                      <DataQualityFlag quality={sig.data_quality} />
                    </div>
                  </td>

                  {/* Company / Sector */}
                  <td className="px-4 py-3">
                    <div className="flex flex-col">
                      <span className="text-dark-text font-medium truncate max-w-[150px]">{stock.company_name}</span>
                      <span className="text-[10px] text-dark-muted font-mono">{stock.sector || 'N/A'}</span>
                    </div>
                  </td>

                  {/* First Bought */}
                  <td className="px-4 py-3">
                    {stock.buy_date ? (
                      <div className="flex flex-col">
                        <span className="font-mono text-xs text-dark-text">
                          {new Date(stock.buy_date).toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' })}
                        </span>
                        <span className="text-[10px] text-dark-muted">
                          {Math.floor((Date.now() - new Date(stock.buy_date)) / (1000 * 60 * 60 * 24))}d held
                        </span>
                      </div>
                    ) : (
                      <span className="text-dark-muted">—</span>
                    )}
                  </td>

                  {/* Price */}
                  <td className={`px-4 py-3 text-right font-mono font-bold transition-colors duration-300 ${flashClass}`}>
                    ₹{sig.current_price?.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) || '—'}
                  </td>

                  {/* Change % */}
                  <td className={`px-4 py-3 text-right font-mono font-semibold ${
                    (sig.change_pct ?? 0) > 0 ? 'text-signal-buy'
                    : (sig.change_pct ?? 0) < 0 ? 'text-signal-sell' : 'text-dark-muted'
                  }`}>
                    {sig.change_pct != null
                      ? `${sig.change_pct > 0 ? '+' : ''}${sig.change_pct.toFixed(2)}%`
                      : '—'}
                  </td>

                  {/* Short / Long signals */}
                  <td className="px-4 py-3">
                    <div className="flex justify-center">
                      <SignalBadge signal={sig.st_signal} score={sig.st_score} />
                    </div>
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex justify-center">
                      <SignalBadge signal={sig.lt_signal} score={sig.lt_score} />
                    </div>
                  </td>

                  {/* Confidence */}
                  <td className="px-4 py-3">
                    <div className="flex items-center justify-center gap-2">
                      <div className="w-14 bg-gray-700 h-1.5 rounded-full overflow-hidden">
                        <div
                          className={`h-full rounded-full transition-all ${
                            (sig.confidence_pct ?? 0) >= 70 ? 'bg-signal-buy'
                            : (sig.confidence_pct ?? 0) >= 45 ? 'bg-signal-hold' : 'bg-signal-sell'
                          }`}
                          style={{ width: `${sig.confidence_pct || 0}%` }}
                        />
                      </div>
                      <span className="text-[10px] font-mono font-bold w-8">
                        {sig.confidence_pct?.toFixed(0) || 0}%
                      </span>
                    </div>
                  </td>

                  {/* Position */}
                  {hasPosition && (
                    <td className="px-4 py-3 text-right">
                      {stock.quantity ? (
                        <div className="flex flex-col">
                          <span className="font-mono text-xs font-bold text-dark-text">
                            {stock.quantity} Qty
                          </span>
                          <span className="font-mono text-[10px] text-dark-muted">@ ₹{stock.avg_buy_price}</span>
                        </div>
                      ) : (
                        <span className="text-dark-muted">—</span>
                      )}
                    </td>
                  )}

                  {/* Profit % */}
                  {hasPosition && (
                    <td className={`px-4 py-3 text-right font-mono font-bold text-sm ${
                      profitPct === null ? 'text-dark-muted'
                      : isProfit ? 'text-signal-buy' : 'text-signal-sell'
                    }`}>
                      {profitPct !== null
                        ? `${isProfit ? '+' : ''}${profitPct.toFixed(2)}%`
                        : '—'}
                    </td>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {/* Pagination controls */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between px-5 py-3 border-t border-dark-border bg-gray-900/40">
          <span className="text-xs text-dark-muted font-mono">
            Showing {(page - 1) * PAGE_SIZE + 1}–{Math.min(page * PAGE_SIZE, sorted.length)} of {sorted.length}
          </span>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setPage(p => Math.max(1, p - 1))}
              disabled={page === 1}
              className="px-3 py-1.5 text-xs font-semibold rounded-lg bg-dark-card border border-dark-border hover:border-accent hover:text-accent disabled:opacity-30 disabled:cursor-not-allowed transition-all"
            >
              ← Prev
            </button>
            {Array.from({ length: totalPages }, (_, i) => i + 1)
              .filter(p => p === 1 || p === totalPages || Math.abs(p - page) <= 1)
              .reduce((acc, p, idx, arr) => {
                if (idx > 0 && p - arr[idx - 1] > 1) acc.push('...');
                acc.push(p);
                return acc;
              }, [])
              .map((p, idx) =>
                p === '...' ? (
                  <span key={`ellipsis-${idx}`} className="px-1 text-dark-muted text-xs">…</span>
                ) : (
                  <button
                    key={p}
                    onClick={() => setPage(p)}
                    className={`px-3 py-1.5 text-xs font-semibold rounded-lg border transition-all ${
                      page === p
                        ? 'bg-accent border-accent text-white shadow-lg shadow-accent/20'
                        : 'bg-dark-card border-dark-border hover:border-accent hover:text-accent'
                    }`}
                  >
                    {p}
                  </button>
                )
              )
            }
            <button
              onClick={() => setPage(p => Math.min(totalPages, p + 1))}
              disabled={page === totalPages}
              className="px-3 py-1.5 text-xs font-semibold rounded-lg bg-dark-card border border-dark-border hover:border-accent hover:text-accent disabled:opacity-30 disabled:cursor-not-allowed transition-all"
            >
              Next →
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
