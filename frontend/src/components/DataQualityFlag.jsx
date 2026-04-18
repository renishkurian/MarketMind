import React from 'react';
import { AlertCircle } from 'lucide-react';

const DataQualityFlag = ({ quality }) => {
  if (quality === 'FULL' || !quality) return null;

  return (
    <div className="flex items-center gap-1.5 px-2 py-1 bg-yellow-500/10 text-yellow-500 border border-yellow-500/30 rounded text-[10px] whitespace-nowrap">
      <AlertCircle size={10} strokeWidth={3} />
      <span className="font-bold uppercase tracking-tight">Technicals Only</span>
    </div>
  );
};

export default DataQualityFlag;
