import React from 'react';
import { useNavigate } from 'react-router-dom';
import SignalBadge from './SignalBadge';
import DataQualityFlag from './DataQualityFlag';

const StockTable = ({ stocks, showType = false }) => {
  const navigate = useNavigate();

  return (
    <div className="bg-dark-card border border-dark-border rounded-xl overflow-hidden shadow-sm">
      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left">
          <thead className="bg-gray-900/60 text-dark-muted border-b border-dark-border">
            <tr>
              <th className="px-5 py-4 font-semibold uppercase tracking-wider text-[11px]">Symbol</th>
              <th className="px-5 py-4 font-semibold uppercase tracking-wider text-[11px]">Company (Sector)</th>
              <th className="px-5 py-4 font-semibold uppercase tracking-wider text-[11px] text-right">Price</th>
              <th className="px-5 py-4 font-semibold uppercase tracking-wider text-[11px] text-right">Change</th>
              <th className="px-5 py-4 font-semibold uppercase tracking-wider text-[11px] text-center">Short Term</th>
              <th className="px-5 py-4 font-semibold uppercase tracking-wider text-[11px] text-center">Long Term</th>
              <th className="px-5 py-4 font-semibold uppercase tracking-wider text-[11px] text-center">Confidence</th>
              {stocks.some(s => s.quantity) && (
                <th className="px-5 py-4 font-semibold uppercase tracking-wider text-[11px] text-right">Position</th>
              )}
              <th className="px-5 py-4 font-semibold uppercase tracking-wider text-[11px] text-center">Status</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-dark-border">
            {stocks.map((stock) => {
              const sig = stock.signal || {};
              const flashClass = stock._lastPriceChange === 'up' ? 'flash-green' : stock._lastPriceChange === 'down' ? 'flash-red' : '';
              
              return (
                <tr
                  key={stock.symbol}
                  onClick={() => navigate(`/stock/${stock.symbol}`)}
                  className="hover:bg-gray-800/40 cursor-pointer transition-colors group"
                >
                  <td className="px-5 py-4">
                    <div className="flex flex-col">
                      <span className="font-mono font-bold text-accent group-hover:underline text-base">{stock.symbol}</span>
                      <DataQualityFlag quality={sig.data_quality} />
                    </div>
                  </td>
                  <td className="px-5 py-4">
                    <div className="flex flex-col">
                      <span className="text-dark-text font-medium truncate max-w-[150px]">{stock.company_name}</span>
                      <span className="text-[10px] text-dark-muted font-mono">{stock.sector || 'N/A'}</span>
                    </div>
                  </td>
                  <td className={`px-5 py-4 text-right font-mono font-bold text-dark-text transition-colors duration-300 ${flashClass}`}>
                    ₹{sig.current_price?.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) || '—'}
                  </td>
                  <td className={`px-5 py-4 text-right font-mono font-semibold ${
                    (sig.change_pct ?? 0) > 0 ? 'text-signal-buy' : (sig.change_pct ?? 0) < 0 ? 'text-signal-sell' : 'text-dark-muted'
                  }`}>
                    {sig.change_pct != null ? `${sig.change_pct > 0 ? '+' : ''}${sig.change_pct.toFixed(2)}%` : '—'}
                  </td>
                  <td className="px-5 py-4">
                    <div className="flex justify-center">
                      <SignalBadge signal={sig.st_signal} score={sig.st_score} />
                    </div>
                  </td>
                  <td className="px-5 py-4">
                    <div className="flex justify-center">
                      <SignalBadge signal={sig.lt_signal} score={sig.lt_score} />
                    </div>
                  </td>
                  <td className="px-5 py-4">
                    <div className="flex items-center justify-center gap-2">
                      <div className="w-16 bg-gray-700 h-1.5 rounded-full overflow-hidden">
                        <div
                          className={`h-full rounded-full transition-all ${
                            (sig.confidence_pct ?? 0) >= 70 ? 'bg-signal-buy' :
                            (sig.confidence_pct ?? 0) >= 45 ? 'bg-signal-hold' : 'bg-signal-sell'
                          }`}
                          style={{ width: `${sig.confidence_pct || 0}%` }}
                        />
                      </div>
                      <span className="text-[10px] font-mono font-bold w-8">{sig.confidence_pct?.toFixed(0) || 0}%</span>
                    </div>
                  </td>
                  {stocks.some(s => s.quantity) && (
                    <td className="px-5 py-4 text-right">
                      {stock.quantity ? (
                        <div className="flex flex-col">
                          <span className="font-mono text-xs font-bold text-dark-text">{stock.quantity} Qty</span>
                          <span className="font-mono text-[10px] text-dark-muted">@ ₹{stock.avg_buy_price}</span>
                        </div>
                      ) : (
                        <span className="text-dark-muted">—</span>
                      )}
                    </td>
                  )}
                  <td className="px-5 py-4 text-center">
                    {showType && (
                        <span className={`text-[10px] px-2 py-0.5 rounded font-bold ${
                            stock.type === 'PORTFOLIO' ? 'bg-accent/10 text-accent border border-accent/30' : 'bg-blue-500/10 text-blue-400 border border-blue-500/30'
                        }`}>
                            {stock.type}
                        </span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default StockTable;
