import React from 'react';
import { NavLink, useNavigate } from 'react-router-dom';
import {
  Activity, BarChart2, Eye, Zap,
  Sun, Moon, Bell, Wifi, WifiOff, Settings, LogOut
} from 'lucide-react';
import { useStockStore } from '../store/stockStore';
import { useAuthStore } from '../store/authStore';

const NAV_ITEMS = [
  { path: '/portfolio',     label: 'Portfolio',     Icon: BarChart2 },
  { path: '/watchlist',     label: 'Watchlist',     Icon: Eye        },
  { path: '/opportunities', label: 'Opportunities', Icon: Zap        },
  { path: '/settings',      label: 'Settings',      Icon: Settings   },
];

export default function Layout({ children }) {
  const { marketStatus, theme, toggleTheme, isConnected, alerts, lastUpdate } = useStockStore();
  const { logout } = useAuthStore();
  const navigate = useNavigate();
  const isOpen = marketStatus === 'OPEN';

  return (
    <div className="flex min-h-screen bg-light-bg dark:bg-dark-bg text-light-text dark:text-dark-text font-sans transition-colors duration-500">
      {/* Sidebar */}
      <aside className="w-64 shrink-0 flex flex-col border-r border-light-border dark:border-dark-border bg-white/80 dark:bg-dark-card/80 backdrop-blur-xl sticky top-0 h-screen z-50">
        {/* Brand */}
        <div className="flex items-center gap-3 px-6 py-6 border-b border-light-border dark:border-dark-border">
          <div className="w-10 h-10 rounded-xl bg-accent shadow-lg shadow-accent/30 flex items-center justify-center">
            <Activity size={22} className="text-white" />
          </div>
          <div>
            <span className="block font-bold font-mono tracking-tighter text-accent text-xl leading-none">MarketMind</span>
            <span className="text-[10px] text-light-muted dark:text-dark-muted font-mono uppercase tracking-widest mt-1 block">Intelligence</span>
          </div>
        </div>

        {/* Market Status pill */}
        <div className="px-5 py-4 border-b border-light-border dark:border-dark-border">
          <div className={`flex items-center justify-between px-4 py-3 rounded-2xl text-xs font-semibold border shadow-sm transition-all duration-300 ${
            isOpen
              ? 'border-signal-buy/20 text-signal-buy bg-signal-buy/5'
              : 'border-light-border dark:border-dark-border text-light-muted dark:text-dark-muted bg-gray-100/50 dark:bg-gray-800/30'
          }`}>
            <div className="flex items-center gap-2">
              <div className={`w-2 h-2 rounded-full ${isOpen ? 'bg-signal-buy animate-pulse' : 'bg-gray-400'}`} />
              <span>{isOpen ? 'MARKET LIVE' : `MARKET ${marketStatus}`}</span>
            </div>
            {isOpen && <div className="text-[10px] opacity-70 font-mono">IST</div>}
          </div>
          <div className="flex items-center justify-between mt-3 px-1 text-[10px] text-light-muted dark:text-dark-muted font-mono">
            <span>Last Update</span>
            <span>{lastUpdate.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}</span>
          </div>
        </div>

        {/* Nav links */}
        <nav className="flex-1 py-6 px-3 space-y-1.5">
          <p className="px-3 text-[10px] font-bold text-light-muted dark:text-dark-muted uppercase tracking-widest mb-3">Main Menu</p>
          {NAV_ITEMS.map(({ path, label, Icon }) => (
            <NavLink
              key={path}
              to={path}
              className={({ isActive }) =>
                `flex items-center gap-3 px-4 py-3 rounded-xl text-sm font-medium transition-all group ${
                  isActive
                    ? 'bg-accent text-white shadow-lg shadow-accent/20'
                    : 'text-light-muted dark:text-dark-muted hover:text-accent dark:hover:text-white hover:bg-accent/5'
                }`
              }
            >
              <Icon size={18} className="transition-transform group-hover:scale-110" />
              {label}
            </NavLink>
          ))}
        </nav>

        {/* Bottom status */}
        <div className="px-5 py-5 border-t border-light-border dark:border-dark-border space-y-4 bg-gray-50/50 dark:bg-black/10">
          {/* WebSocket status */}
          <div className={`flex items-center gap-3 text-xs font-medium ${isConnected ? 'text-signal-buy' : 'text-signal-sell'}`}>
            <div className={`p-1.5 rounded-lg ${isConnected ? 'bg-signal-buy/10' : 'bg-signal-sell/10'}`}>
              {isConnected ? <Wifi size={14} /> : <WifiOff size={14} />}
            </div>
            <span>{isConnected ? 'Real-time Active' : 'Feed Offline'}</span>
          </div>

          {/* Theme toggle */}
          <button
            onClick={toggleTheme}
            className="flex items-center gap-3 w-full text-xs font-medium text-light-muted dark:text-dark-muted hover:text-accent dark:hover:text-white transition-all group px-1"
          >
            <div className="p-1.5 rounded-lg bg-gray-200/50 dark:bg-gray-800/50 group-hover:bg-accent/10 transition-colors">
              {theme === 'dark' ? <Sun size={14} /> : <Moon size={14} />}
            </div>
            <span>{theme === 'dark' ? 'Switch to Light' : 'Switch to Dark'}</span>
          </button>

          {/* Logout */}
          <button
            onClick={() => {
              logout();
              navigate('/login');
            }}
            className="flex items-center gap-3 w-full text-xs font-medium text-signal-sell hover:text-red-500 transition-all group px-1"
          >
            <div className="p-1.5 rounded-lg bg-signal-sell/10 group-hover:bg-signal-sell/20 transition-colors">
              <LogOut size={14} />
            </div>
            <span>Logout</span>
          </button>
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 min-w-0 overflow-auto">
        {children}
      </main>
    </div>
  );
}
