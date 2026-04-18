import React from 'react';
import { useStockStore } from '../store/stockStore';
import { Activity, Clock } from 'lucide-react';

const MarketStatusBadge = () => {
  const { marketStatus } = useStockStore();
  
  const statusConfig = {
    OPEN: {
      color: 'bg-signal-buy/10 text-signal-buy border-signal-buy/30',
      label: 'LIVE',
      pulse: true
    },
    PRE_OPEN: {
      color: 'bg-signal-hold/10 text-signal-hold border-signal-hold/30',
      label: 'PRE-OPEN',
      pulse: false
    },
    CLOSED: {
      color: 'bg-gray-800/10 text-dark-muted border-dark-border',
      label: 'CLOSED',
      pulse: false
    }
  };

  const config = statusConfig[marketStatus] || statusConfig.CLOSED;

  return (
    <div className={`flex items-center gap-2 px-3 py-1.5 rounded-full border text-[11px] font-bold tracking-wider ${config.color}`}>
      <div className="relative flex items-center justify-center">
        {config.pulse && (
          <span className="absolute inline-flex h-2 w-2 animate-ping rounded-full bg-signal-buy opacity-75"></span>
        )}
        <span className={`relative inline-flex h-2 w-2 rounded-full ${marketStatus === 'OPEN' ? 'bg-signal-buy' : marketStatus === 'PRE_OPEN' ? 'bg-signal-hold' : 'bg-gray-600'}`}></span>
      </div>
      <span>{config.label}</span>
      <Clock size={12} className="opacity-60" />
    </div>
  );
};

export default MarketStatusBadge;
