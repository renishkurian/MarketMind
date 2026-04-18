import React from 'react';
import { ArrowUpRight, ArrowRight, ArrowDownRight } from 'lucide-react';

const SignalBadge = ({ signal, score, size = 'sm' }) => {
  if (!signal) return <span className="text-gray-500 text-xs">—</span>;

  const isLg = size === 'lg';
  const baseClasses = `flex items-center gap-1 font-semibold border rounded-xl transition-all ${
    isLg ? 'px-4 py-2 text-sm shadow-md' : 'px-2 py-1 text-[10px]'
  }`;

  if (signal === 'BUY') return (
    <div className={`${baseClasses} bg-signal-buy/15 text-signal-buy border-signal-buy/30 hover:bg-signal-buy/20`}>
      <ArrowUpRight size={isLg ? 16 : 12} className="shrink-0" />
      <span>BUY {score && (isLg ? `Score: ${score}` : `(${score})`)}</span>
    </div>
  );

  if (signal === 'SELL') return (
    <div className={`${baseClasses} bg-signal-sell/15 text-signal-sell border-signal-sell/30 hover:bg-signal-sell/20`}>
      <ArrowDownRight size={isLg ? 16 : 12} className="shrink-0" />
      <span>SELL {score && (isLg ? `Score: ${score}` : `(${score})`)}</span>
    </div>
  );

  return (
    <div className={`${baseClasses} bg-signal-hold/15 text-signal-hold border-signal-hold/30 hover:bg-signal-hold/20`}>
      <ArrowRight size={isLg ? 16 : 12} className="shrink-0" />
      <span>HOLD {score && (isLg ? `Score: ${score}` : `(${score})`)}</span>
    </div>
  );
};

export default SignalBadge;
