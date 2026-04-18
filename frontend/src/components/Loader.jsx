import React from 'react';

/**
 * A premium, animated loader for MarketMind.
 * Uses a pulse and spinning ring effect.
 */
export default function Loader({ size = 'md' }) {
  const sizes = {
    sm: 'w-4 h-4',
    md: 'w-8 h-8',
    lg: 'w-12 h-12'
  };

  const ringSizes = {
    sm: 'border-2',
    md: 'border-3',
    lg: 'border-4'
  };

  const selectedSize = sizes[size] || sizes.md;
  const selectedRing = ringSizes[size] || ringSizes.md;

  return (
    <div className="flex flex-col items-center justify-center gap-4 animate-in fade-in duration-500">
      <div className={`relative ${selectedSize}`}>
        {/* Outer Ring */}
        <div className={`absolute inset-0 rounded-full ${selectedRing} border-accent/20`} />
        {/* Spinning Segment */}
        <div className={`absolute inset-0 rounded-full ${selectedRing} border-t-accent animate-spin`} />
        {/* Inner Pulse */}
        <div className="absolute inset-2 rounded-full bg-accent/10 animate-pulse" />
      </div>
      <span className="text-xs font-mono font-bold text-dark-muted tracking-[0.2em] uppercase">
        Syncing Market Data
      </span>
    </div>
  );
}
