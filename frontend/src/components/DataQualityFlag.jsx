import React from 'react';
import { AlertCircle, AlertTriangle } from 'lucide-react';

const DataQualityFlag = ({ quality }) => {
  if (quality === 'FULL' || !quality) return null;

  const isPartial = quality === 'PARTIAL';

  return (
    <div className={`flex items-center gap-1.5 px-2 py-1 rounded text-[10px] whitespace-nowrap border ${
      isPartial 
        ? 'bg-yellow-500/10 text-yellow-500 border-yellow-500/30' 
        : 'bg-red-500/10 text-red-500 border-red-500/30'
    }`}>
      {isPartial ? <AlertTriangle size={10} strokeWidth={3} /> : <AlertCircle size={10} strokeWidth={3} />}
      <span className="font-bold uppercase tracking-tight">
        {isPartial ? 'Partial Data' : 'Technicals Only'}
      </span>
    </div>
  );
};

export default DataQualityFlag;
