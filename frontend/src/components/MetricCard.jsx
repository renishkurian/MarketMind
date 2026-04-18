import React from 'react';

const MetricCard = ({ label, value, sub, color = 'text-dark-text', icon: Icon }) => {
  return (
    <div className="bg-dark-card border border-dark-border rounded-xl p-4 shadow-sm hover:border-accent/40 transition-all group">
      <div className="flex justify-between items-start mb-1">
        <span className="text-dark-muted text-xs font-semibold uppercase tracking-wider">{label}</span>
        {Icon && <Icon size={14} className="text-dark-muted group-hover:text-accent transition-colors" />}
      </div>
      <div className={`text-2xl font-bold font-mono tracking-tight ${color}`}>
        {value}
      </div>
      {sub && <div className="text-[10px] text-dark-muted/70 mt-1 font-medium">{sub}</div>}
    </div>
  );
};

export default MetricCard;
