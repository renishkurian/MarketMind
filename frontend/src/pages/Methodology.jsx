import React from 'react';
import { 
  BarChart2, Activity, Zap, Eye, Brain, Shield, 
  Target, Info, Layers, CheckCircle, Database 
} from 'lucide-react';

const SECTIONS = [
  {
    title: 'Institutional Scoring Architecture',
    desc: 'The Composite Score (0–100) is a weighted blend of four specialized dimensions designed for long-term compounding.',
    icon: Layers,
    items: [
      { name: 'Fundamental (FA)', weight: '45%', focus: 'Quality, Valuations, 3Y Growth CAGR' },
      { name: 'Technical (TA)', weight: '25%', focus: 'Trend Structure, SMAs, Momentum' },
      { name: 'Momentum', weight: '15%', focus: 'Relative Strength, Volume Shocks, ROC' },
      { name: 'Sector Rank', weight: '15%', focus: 'Percentile performance vs. Industry Peers' },
    ]
  },
  {
    title: 'Data Confidence Metric',
    desc: 'The Confidence Score (0.0 to 1.0) measures the reliability and completeness of the incoming data streams.',
    icon: Shield,
    items: [
      { name: 'Full Coverage (1.0)', focus: 'All fundamental, price, and sector data is present and vetted.' },
      { name: 'Partial Coverage (<0.5)', focus: 'Missing debt levels or growth figures; score is safe but less certain.' },
      { name: 'Graceful Degradation', focus: 'Missing fields are treated as neutral to avoid unfair penalization.' },
    ]
  },
  {
    title: 'Expert Signal Logic',
    desc: 'Unlike simple oscillators, our BUY/SELL/HOLD signals are derived from expert-curated trend identification.',
    icon: Target,
    items: [
      { name: 'Bullish Setup', focus: 'EMA Crossover + MACD Acceleration + RSI below Overbought.' },
      { name: 'Bearish Veto', focus: 'Forensic AI detects accounting anomalies or high promoter pledge.' },
      { name: 'Trend Guardians', focus: 'SMA 50/200 gates protect against catching a falling knife.' },
    ]
  },
  {
    title: 'The Neutral Baseline (Score 50)',
    desc: 'Why do most metrics start at 50? Our engine is designed to be "Missing-Data Proof."',
    icon: Info,
    items: [
      { name: 'Missing Data', focus: 'If a value like ROE is missing, the engine assigns a neutral 50 instead of a 0.' },
      { name: 'No Unfair Bias', focus: 'This prevents a stock from looking "bad" just because its data provider is lagging.' },
      { name: 'Trust confidence', focus: 'A score of 50 with low confidence means "We have no opinion yet due to lack of info."' },
    ]
  }
];

export default function Methodology() {
  return (
    <div className="p-8 pb-16">
      {/* Header */}
      <div className="mb-10">
        <h1 className="text-3xl font-bold text-light-text dark:text-white mb-2">Platform Methodology</h1>
        <p className="text-dark-muted max-w-2xl">
          Understanding the institutional engine behind MarketMind. We blend quantitative data with expert rules to find high-probability compounding opportunities.
        </p>
      </div>

      <div className="grid gap-8 lg:grid-cols-1">
        {SECTIONS.map((section, idx) => (
          <div key={idx} className="relative group">
            <div className="absolute -inset-0.5 bg-gradient-to-r from-accent/20 to-purple-500/20 rounded-2xl blur opacity-25 group-hover:opacity-40 transition" />
            <div className="relative bg-white dark:bg-dark-card border border-light-border dark:border-dark-border rounded-2xl overflow-hidden shadow-sm">
              <div className="p-6 md:p-8">
                <div className="flex items-start gap-6">
                  <div className="shrink-0 w-14 h-14 rounded-2xl bg-accent/10 flex items-center justify-center text-accent">
                    <section.icon size={28} />
                  </div>
                  <div className="flex-1">
                    <h2 className="text-xl font-bold dark:text-white mb-2">{section.title}</h2>
                    <p className="text-sm text-dark-muted mb-6">{section.desc}</p>

                    <div className="grid sm:grid-cols-2 xl:grid-cols-3 gap-4">
                      {section.items.map((item, i) => (
                        <div key={i} className="flex flex-col p-4 rounded-xl bg-gray-50 dark:bg-black/20 border border-light-border dark:border-white/5 transition-all hover:border-accent/40">
                          <div className="flex items-center justify-between mb-2">
                            <span className="text-xs font-bold text-accent uppercase tracking-wider">{item.name}</span>
                            {item.weight && (
                              <span className="text-[10px] px-2 py-0.5 rounded-full bg-accent/10 text-accent font-bold">{item.weight}</span>
                            )}
                          </div>
                          <span className="text-sm dark:text-gray-300 leading-snug">{item.focus}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Persistence Info */}
      <div className="mt-12 p-8 bg-accent/5 border border-accent/20 rounded-2xl flex items-center gap-6">
        <div className="shrink-0 w-12 h-12 rounded-full bg-accent/20 flex items-center justify-center text-accent">
          <Database size={24} />
        </div>
        <div>
          <h3 className="text-lg font-bold dark:text-white">Continuous Calibration</h3>
          <p className="text-sm text-dark-muted max-w-3xl">
            The platform performs an **End-of-Day (EOD)** sync every evening to re-calculate all signals based on the latest BhavanCopy. 
            **Fundamental refreshes** occur weekly every Monday morning to ensure your PEG and Growth metrics reflect the newest quarterly reports and shareholding patterns.
          </p>
        </div>
      </div>
    </div>
  );
}
