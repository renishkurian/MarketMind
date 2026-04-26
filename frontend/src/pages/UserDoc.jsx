import React, { useState } from 'react';
import { Target, Search, Zap, Bell, Users, ShieldAlert, Cpu, Database, Network, TrendingUp, Grid, Activity, BarChart, Copy, CheckCircle2, ChevronRight, Hash, Eye, Sparkles } from 'lucide-react';
import { useNavigate } from 'react-router-dom';

const FEATURES = [
  {
    id: 'f1',
    title: 'AI Price Target with Confidence Band',
    icon: Target,
    color: 'text-accent',
    bg: 'bg-accent/10',
    where: 'Stock DeepDive → Chat panel (right side of chart)',
    how: 'AI responds with analysis AND auto-plots a shaded confidence band on the candlestick chart. Band shows Confidence Low (green) and Confidence High (red) as separate series. Look for "AI Target" in the chart legend.',
    steps: [
      'Open any stock page — e.g. navigate to /stock/RELIANCE',
      'Find the chat panel on the right side of the chart',
      'Click "New Chat" if no session is active',
      'Type any message asking for a price target or chart analysis',
      'Ask follow-up questions — the band updates with each new target suggestion'
    ],
    chips: ['Is this a good entry right now?', 'Identify key support and resistance', 'Explain the recent price move', 'What\'s the risk/reward for next 3 months?', 'Set an alert at key support level'],
    prompts: [
      { label: 'Basic price target', text: 'Based on the current chart and indicators, what is your price target for this stock over the next 3 months? Give me a confidence range.' },
      { label: 'Bull vs bear target', text: 'Give me a bull-case target and a bear-case support level based on the technicals. I want to see both on the chart.' }
    ]
  },
  {
    id: 'f2',
    title: 'Pattern Recognition Auto-Detect',
    icon: Search,
    color: 'text-signal-buy',
    bg: 'bg-signal-buy/10',
    where: 'Stock DeepDive → Coloured badges near the top of the chart area',
    how: 'Loads silently on every page open — no action needed. Detects patterns with ≥60% confidence. Click any badge to open the Pattern Detail Modal showing target price, stop-loss, and confidence.',
    steps: [
      'Open any stock page — patterns load automatically',
      'Look for coloured badges near the top of the chart (e.g. "Cup & Handle 82%")',
      'Click any badge to open the Pattern Detail Modal',
      'Press "Ask AI to Analyse This Pattern" — opens chat with full pattern context pre-loaded'
    ],
    prompts: [
      { label: 'Measured move + stop-loss', text: 'What is the measured move target for this pattern and where should I place my stop-loss to keep risk under 5%?' },
      { label: 'Pattern + fundamentals check', text: 'The chart shows a pattern forming. Do the fundamentals support this setup or is there a reason to be cautious?' }
    ]
  },
  {
    id: 'f3',
    title: 'Move Explanation Cache',
    icon: Zap,
    color: 'text-signal-hold',
    bg: 'bg-signal-hold/10',
    where: 'Portfolio → Performance Center → Winners or Losers table → ⚡ button',
    how: 'Modal shows headline, explanation, catalyst chips, sentiment pill, and "AI Recommendation". Cached 4 hours. If the stock moves >2% the cache auto-invalidates.',
    steps: [
      'Navigate to Portfolio → Performance Center',
      'Find the Winners or Losers table',
      'Click the ⚡ button on any row to trigger the AI explanation',
      'Provides headline, explanation, catalyst chips, sentiment, AI recommendation'
    ],
    prompts: [
      { label: 'Sustainability check', text: 'The stock just moved heavily. Based on the catalysts and current chart, is this move sustainable or is it a dead-cat bounce?' },
      { label: 'Exit strategy after big gain', text: 'I\'m sitting on a solid gain on this stock. What is a sensible trailing stop or exit strategy to lock in profits without exiting too early?' }
    ]
  },
  {
    id: 'f4',
    title: 'AI-Generated Entry/Exit Alerts',
    icon: Bell,
    color: 'text-signal-sell',
    bg: 'bg-signal-sell/10',
    where: 'Stock DeepDive → Chat panel',
    how: 'Type a natural-language alert request. The chat engine intercepts alert keywords and routes to the alerts API. AI extracts the exact price from the chart context (SMA, BB, 52w high/low).',
    steps: [
      'Open any stock page and go to the chat panel',
      'Type a natural language alert request (e.g. "Alert me if it crosses 52w high")',
      'AI extracts the exact price level from the chart context and confirms',
      'Alert appears in the "Active Alerts" section below the chat chips'
    ],
    chips: ['Set an alert at key support level', 'Set a stop loss alert for me', 'Alert me at the breakout level'],
    prompts: [
      { label: 'Support alert', text: 'Set an alert if this stock falls to the key support level below the current price.' },
      { label: 'SMA stop-loss', text: 'Set a stop-loss alert just below the 50-day SMA. Notify me if the close crosses below that level.' }
    ]
  },
  {
    id: 'f5',
    title: 'Expert Skill Chat',
    icon: Users,
    color: 'text-purple-500',
    bg: 'bg-purple-500/10',
    where: 'Stock DeepDive → Chat header 🎭 icon',
    how: 'Analyses the stock through 7 legendary investor lenses (e.g., Warren Buffett value lens, SEBI Forensic lens). Pre-loads prompts mapped to their investing styles.',
    steps: [
      'Open the chat panel in any stock DeepDive',
      'Click the 🎭 (Mask) icon in the chat header',
      'Toggle between tabs to change the persona (Buffett, Dalio, Quantitative, SEBI, etc.)',
      'Click any of their specialized prompts to ask the AI'
    ],
    prompts: [
      { label: 'SEBI Forensic', text: 'Look carefully at the promoter pledges, debt levels, and cash flow. Are there any red flags or governance risks here?' },
      { label: 'Peter Lynch', text: 'What is the "story" behind this stock? Is this a fast grower, slow grower, stalwart, or turnaround?' }
    ]
  },
  {
    id: 'f6',
    title: 'Yearly Risk Explainer',
    icon: ShieldAlert,
    color: 'text-accent',
    bg: 'bg-accent/10',
    where: 'Benchmark Dashboard → "Why this year?" button',
    how: 'Explains what drove your portfolio return each calendar year, combining your specific holdings with historical Indian macro drivers (e.g., COVID crash, FII outflows).',
    steps: [
      'Navigate to Benchmark Dashboard from the sidebar (Performance)',
      'Find the Yearly Performance section',
      'Click "Why this year?" on any year card',
      'Modal opens with what worked, what didn\'t, and macro drivers for that year'
    ],
    prompts: [
      { label: 'Year repeat risk', text: 'My portfolio gained heavily last year. What should I change in my holdings to be better positioned if a similar macro environment repeats?' },
      { label: 'Alpha explanation', text: 'I outperformed Nifty significantly. Was this alpha from stock selection, sector allocation, or timing? How do I repeat this?' }
    ]
  },
  {
    id: 'f7',
    title: 'Alpha Discovery / ML Discovery',
    icon: Cpu,
    color: 'text-accent',
    bg: 'bg-accent/10',
    where: 'AI Intelligence → Alpha Discovery',
    how: 'Scans the entire market index to surface fundamental anomalies, technical breakouts, and AI-predicted momentum shifts. Used to discover actionable trade setups before they hit mainstream news.',
    steps: [
      'Navigate to AI Intelligence → Alpha Discovery from the sidebar',
      'Review the auto-generated list of high-probability setups',
      'Filter by Sector or Market Cap to refine ideas',
      'Click any stock to open its DeepDive interface for further analysis'
    ]
  },
  {
    id: 'f8',
    title: 'The Oracle AI',
    icon: Network,
    color: 'text-signal-buy',
    bg: 'bg-signal-buy/10',
    where: 'AI Intelligence → The Oracle',
    how: 'A centralized conversational interface for broad market thesis testing, multi-stock comparisons, and macro-economic inquiries not limited to a single stock ticker. It acts as your macro hedge-fund analyst.',
    steps: [
      'Navigate to AI Intelligence → The Oracle',
      'Ask broad questions e.g., "How does the RBI rate cut affect PSU banks vs Private banks?"',
      'Ask to compare multiple stocks e.g., "Compare the valuation of HDFC Bank and ICICI Bank"',
      'The Oracle maintains conversation context just like standard ChatGPT, but injected with live MarketMind data.'
    ]
  },
  {
    id: 'f9',
    title: 'War Room (Institutional Research)',
    icon: Database,
    color: 'text-signal-sell',
    bg: 'bg-signal-sell/10',
    where: 'AI Intelligence → War Room',
    how: 'A command center for deep-dive fundamental tearing apart of a company. Extracts quantitative scores (intel scores) from complex filings, supply chain dependencies, and competitor matrices.',
    steps: [
      'Navigate to AI Intelligence → War Room',
      'Search for a stock ticker to initiate a War Room snapshot',
      'Review the Institutional Intel Score and deep fundamental breakdown',
      'Switch between competitor matrix views and historical snapshots'
    ]
  },
  {
    id: 'f10',
    title: 'Portfolio Optimizer (MVO/HRP/CVAR)',
    icon: Activity,
    color: 'text-purple-500',
    bg: 'bg-purple-500/10',
    where: 'AI Intelligence → Portfolio Opt',
    how: 'Uses advanced financial mathematics (Mean-Variance Optimization, Hierarchical Risk Parity, Conditional Value at Risk) to suggest optimal capital allocation across your watchlists or existing portfolio to maximize the Sharpe ratio.',
    steps: [
      'Navigate to AI Intelligence → Portfolio Opt',
      'Select the assets you want to invest in (or import your current portfolio)',
      'Choose your optimization strategy (e.g. Max Sharpe or Min Volatility)',
      'Click "Run Optimization" to generate the weight allocations',
      'Review the efficient frontier chart and the recommended target weights'
    ]
  },
  {
    id: 'f11',
    title: 'Alpha Heatmap',
    icon: Grid,
    color: 'text-signal-hold',
    bg: 'bg-signal-hold/10',
    where: 'Market → Alpha Heatmap',
    how: 'A dynamic, visually striking tree-map representation of your portfolio\'s or the market\'s performance, instantly highlighting concentration risks, sector weighting, and outsized movers.',
    steps: [
      'Navigate to Market → Alpha Heatmap',
      'View the full tree-map grid (green for gainers, red for losers)',
      'Larger blocks indicate a higher weight or market cap vs smaller peers',
      'Hover over any block to see detailed exact percentage changes'
    ]
  },
  {
    id: 'f12',
    title: 'Portfolio Performance Center',
    icon: BarChart,
    color: 'text-accent',
    bg: 'bg-accent/10',
    where: 'Market → Performance Center',
    how: 'The central hub for tracking portfolio health. Features tables for Top Winners/Losers (which link directly to the F3 Move Explanation modal) and overall metric summaries.',
    steps: [
      'Navigate to Market → Performance Center',
      'Toggle between viewing Week, Month, Year, and YTD performance',
      'Review your biggest winners and losers',
      'Click the ⚡ icon next to any stock to trigger an AI Move Explanation (F3)'
    ]
  },
  {
    id: 'f13',
    title: 'Benchmark Dashboard',
    icon: TrendingUp,
    color: 'text-signal-buy',
    bg: 'bg-signal-buy/10',
    where: 'Market → Performance (Benchmark Dashboard)',
    how: 'Tracks long-term portfolio returns specifically against the Nifty 50. Houses the F6 Yearly Risk Explainer and tracks alpha generation over time.',
    steps: [
      'Navigate to Market → Performance (from the sidebar)',
      'Review the historical equity curve comparing your portfolio to the Nifty',
      'View the calendar-year breakdowns',
      'Click the "Why this year?" button to launch the Yearly Risk Explainer (F6)'
    ]
  }
];

const CopyButton = ({ text }) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  return (
    <button
      onClick={handleCopy}
      className={`absolute top-4 right-4 px-3 py-1.5 rounded-lg text-xs font-black transition-all flex items-center gap-1.5 ${
        copied
          ? 'bg-signal-buy/20 text-signal-buy border border-signal-buy/30'
          : 'bg-dark-bg text-dark-muted border border-dark-border hover:text-text hover:border-accent/40'
      }`}
    >
      {copied ? <CheckCircle2 size={14} /> : <Copy size={14} />}
      {copied ? 'Copied' : 'Copy'}
    </button>
  );
};

export default function UserDoc() {
  const [activeTab, setActiveTab] = useState('f1');
  const navigate = useNavigate();

  const scrollToFeature = (id) => {
    setActiveTab(id);
    const el = document.getElementById(id);
    if (el) {
      el.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  };

  return (
    <div className="h-full w-full flex bg-dark-bg animate-in fade-in duration-500 overflow-hidden relative selection:bg-accent/20">
      
      {/* Sidebar Navigation */}
      <div className="w-80 border-r border-dark-border bg-dark-card/30 flex flex-col items-stretch overflow-y-auto hidden md:flex shrink-0 z-10 sticky top-0">
        <div className="p-6 sticky top-0 bg-dark-card/90 backdrop-blur-xl border-b border-dark-border/50 z-20">
          <div className="flex items-center gap-2 mb-2">
            <Sparkles className="text-accent" size={24} />
            <h1 className="text-xl font-black text-white uppercase tracking-tight">MarketMind</h1>
          </div>
          <p className="text-xs font-bold text-dark-muted uppercase tracking-widest">Platform Guide</p>
        </div>

        <div className="p-4 flex flex-col gap-1.5 pb-20">
          {FEATURES.map((feature) => {
            const Icon = feature.icon;
            const isActive = activeTab === feature.id;
            return (
              <button
                key={feature.id}
                onClick={() => scrollToFeature(feature.id)}
                className={`flex items-start gap-3 w-full text-left p-3 rounded-xl transition-all border ${
                  isActive
                    ? `bg-dark-card ${feature.color} border-dark-border shadow-lg shadow-black/20`
                    : 'bg-transparent text-dark-muted border-transparent hover:bg-dark-card/50 hover:text-gray-300'
                }`}
              >
                <div className={`mt-0.5 rounded-lg p-1.5 shrink-0 ${isActive ? feature.bg : 'bg-dark-border'}`}>
                  <Icon size={16} className={isActive ? feature.color : 'text-dark-muted'} />
                </div>
                <div className="flex flex-col flex-1 pl-1">
                  <span className={`text-[10px] font-black uppercase tracking-widest mb-0.5 ${isActive ? feature.color : 'text-dark-muted/60'}`}>
                    {feature.id}
                  </span>
                  <span className={`text-sm font-bold leading-tight ${isActive ? 'text-white' : ''}`}>
                    {feature.title}
                  </span>
                </div>
                {isActive && <ChevronRight size={16} className={`ml-auto ${feature.color} self-center`} />}
              </button>
            );
          })}
        </div>
      </div>

      {/* Main Content Area */}
      <div className="flex-1 overflow-y-auto p-4 md:p-12 scroll-smooth" id="scroll-container">
        <div className="max-w-4xl mx-auto space-y-16 pb-32">

          {/* Intro Headers */}
          <div className="mb-12">
            <h1 className="text-4xl md:text-5xl font-black text-white italic tracking-tight mb-4 flex items-center gap-4">
              <span className="text-accent">AI</span> Core Features
            </h1>
            <p className="text-dark-muted text-lg font-medium max-w-2xl">
              The complete documentation for MarketMind's predictive intelligence modules. Read exactly how to interact with the cutting-edge agentic framework driving your portfolio.
            </p>
          </div>

          {FEATURES.map((f) => {
            const Icon = f.icon;
            return (
              <div key={f.id} id={f.id} className="scroll-mt-12 group">
                <div className="flex items-center gap-4 mb-6">
                  <div className={`p-4 rounded-2xl ${f.bg} border border-${f.color}/20 shadow-lg`}>
                    <Icon size={32} className={f.color} />
                  </div>
                  <div>
                    <h2 className="text-2xl font-black text-white flex items-center gap-3">
                      <span className={`text-[12px] px-2 py-0.5 rounded font-bold uppercase tracking-widest ${f.bg} ${f.color}`}>
                        {f.id}
                      </span>
                      {f.title}
                    </h2>
                  </div>
                </div>

                <div className="space-y-6 pl-0 md:pl-20">
                  {/* Where & How Cards */}
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="bg-dark-card border border-dark-border rounded-2xl p-5 hover:border-accent/40 transition-colors">
                      <div className="flex items-center gap-2 mb-3 text-dark-muted">
                        <Target size={16} />
                        <span className="text-xs font-black uppercase tracking-widest">Where to find it</span>
                      </div>
                      <p className="text-sm font-semibold text-gray-300 leading-relaxed">{f.where}</p>
                    </div>
                    
                    <div className="bg-dark-card border border-dark-border rounded-2xl p-5 hover:border-accent/40 transition-colors">
                      <div className="flex items-center gap-2 mb-3 text-dark-muted">
                        <Zap size={16} />
                        <span className="text-xs font-black uppercase tracking-widest">What it does</span>
                      </div>
                      <p className="text-sm font-semibold text-gray-300 leading-relaxed">{f.how}</p>
                    </div>
                  </div>

                  {/* Operational Steps */}
                  {f.steps && (
                    <div className="bg-dark-card border border-dark-border rounded-2xl p-6">
                      <h3 className="text-sm font-black text-white uppercase tracking-widest mb-4 flex items-center gap-2">
                        <Hash size={16} className="text-accent" />
                        Usage Workflow
                      </h3>
                      <ol className="space-y-3">
                        {f.steps.map((step, idx) => (
                          <li key={idx} className="flex gap-4">
                            <span className="text-accent font-black text-sm">{idx + 1}.</span>
                            <span className="text-sm font-medium text-gray-400">{step}</span>
                          </li>
                        ))}
                      </ol>
                    </div>
                  )}

                  {/* UI Chips */}
                  {f.chips && (
                    <div className="flex gap-2 flex-wrap items-center">
                      <span className="text-xs font-black text-dark-muted uppercase tracking-widest mr-2">Built-in UI Chips:</span>
                      {f.chips.map((chip, idx) => (
                        <span key={idx} className="bg-dark-bg border border-dark-border text-gray-400 text-xs font-bold px-3 py-1.5 rounded-full">
                          {chip}
                        </span>
                      ))}
                    </div>
                  )}

                  {/* Anti-Gravity Prompts */}
                  {f.prompts && (
                    <div className="mt-8">
                      <h3 className="text-sm font-black text-white uppercase tracking-widest mb-4 flex items-center gap-2">
                        <Sparkles size={16} className="text-signal-buy" />
                        Anti-Gravity Prompts
                      </h3>
                      <div className="space-y-3">
                        {f.prompts.map((prompt, idx) => (
                          <div key={idx} className="relative bg-[#0d1421] border border-dark-border rounded-2xl p-5 group/prompt">
                            <span className="text-xs font-black text-dark-muted uppercase tracking-widest mb-2 block">
                              {prompt.label}
                            </span>
                            <p className="text-sm font-mono text-gray-300 leading-relaxed pr-24">
                              {prompt.text}
                            </p>
                            <CopyButton text={prompt.text} />
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                  
                </div>
              </div>
            );
          })}

        </div>
      </div>
    </div>
  );
}
