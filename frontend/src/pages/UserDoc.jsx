import React from 'react';
import { Info } from 'lucide-react';

export default function UserDoc() {
  return (
    <div className="h-full w-full flex flex-col animate-in fade-in duration-500 bg-dark-bg">
      <iframe
        src="/marketmind_user_guide.html"
        className="flex-1 w-full border-none"
        title="MarketMind AI Features Guide"
      />
    </div>
  );
}
