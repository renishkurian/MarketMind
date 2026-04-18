import React, { useState } from 'react';
import { NavLink, useNavigate } from 'react-router-dom';
import {
  Activity, BarChart2, Eye, Zap,
  Sun, Moon, Wifi, WifiOff, Settings, LogOut,
  Menu, X, ChevronLeft
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
  const { marketStatus, theme, toggleTheme, isConnected, lastUpdate } = useStockStore();
  const { logout } = useAuthStore();
  const navigate = useNavigate();
  const isOpen = marketStatus === 'OPEN';
  const [sidebarOpen, setSidebarOpen] = useState(true);

  return (
    <div className="flex flex-col min-h-screen bg-light-bg dark:bg-dark-bg text-light-text dark:text-dark-text font-sans transition-colors duration-500">

      {/* ── Top Header ───────────────────────────────────────────────── */}
      <header className="sticky top-0 z-50 flex items-center justify-between px-4 h-14 border-b border-light-border dark:border-dark-border bg-white/80 dark:bg-dark-card/80 backdrop-blur-xl shrink-0">
        {/* Left: hamburger + brand */}
        <div className="flex items-center gap-3">
          <button
            onClick={() => setSidebarOpen(p => !p)}
            className="p-2 rounded-lg hover:bg-accent/10 text-dark-muted hover:text-accent transition-all"
            aria-label="Toggle sidebar"
          >
            {sidebarOpen ? <ChevronLeft size={20} /> : <Menu size={20} />}
          </button>

          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg bg-accent shadow-md shadow-accent/30 flex items-center justify-center">
              <Activity size={16} className="text-white" />
            </div>
            <span className="font-bold font-mono tracking-tighter text-accent text-lg leading-none">
              MarketMind
            </span>
          </div>
        </div>

        {/* Centre: market status pill */}
        <div className={`hidden sm:flex items-center gap-2 px-4 py-1.5 rounded-full text-xs font-semibold border transition-all duration-300 ${
          isOpen
            ? 'border-signal-buy/30 text-signal-buy bg-signal-buy/5'
            : 'border-dark-border text-dark-muted bg-gray-800/30'
        }`}>
          <div className={`w-2 h-2 rounded-full ${isOpen ? 'bg-signal-buy animate-pulse' : 'bg-gray-500'}`} />
          {isOpen ? 'MARKET LIVE' : `MARKET ${marketStatus}`}
          {isOpen && <span className="opacity-60 font-mono ml-1">IST</span>}
        </div>

        {/* Right: ws status + theme + logout */}
        <div className="flex items-center gap-1">
          {/* Last update */}
          <span className="hidden md:block text-[10px] font-mono text-dark-muted mr-2">
            {lastUpdate.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
          </span>

          {/* WS indicator */}
          <div className={`flex items-center gap-1.5 text-xs font-medium px-2 py-1 rounded-lg mr-1 ${
            isConnected ? 'text-signal-buy bg-signal-buy/10' : 'text-signal-sell bg-signal-sell/10'
          }`}>
            {isConnected ? <Wifi size={13} /> : <WifiOff size={13} />}
            <span className="hidden sm:inline">{isConnected ? 'Live' : 'Offline'}</span>
          </div>

          {/* Theme toggle */}
          <button
            onClick={toggleTheme}
            className="p-2 rounded-lg hover:bg-accent/10 text-dark-muted hover:text-accent transition-all"
            title="Toggle theme"
          >
            {theme === 'dark' ? <Sun size={17} /> : <Moon size={17} />}
          </button>

          {/* Logout */}
          <button
            onClick={() => { logout(); navigate('/login'); }}
            className="p-2 rounded-lg hover:bg-signal-sell/10 text-dark-muted hover:text-signal-sell transition-all"
            title="Logout"
          >
            <LogOut size={17} />
          </button>
        </div>
      </header>

      {/* ── Body row (sidebar + main) ─────────────────────────────────── */}
      <div className="flex flex-1 min-h-0 overflow-hidden">

        {/* Sidebar */}
        <aside
          className={`shrink-0 flex flex-col border-r border-light-border dark:border-dark-border bg-white/80 dark:bg-dark-card/80 backdrop-blur-xl h-full overflow-y-auto transition-all duration-300 ease-in-out ${
            sidebarOpen ? 'w-56' : 'w-0 overflow-hidden border-r-0'
          }`}
        >
          {/* Market status block (compact, no duplicate brand) */}
          <div className="px-4 py-4 border-b border-light-border dark:border-dark-border">
            <div className={`flex items-center justify-between px-3 py-2.5 rounded-xl text-xs font-semibold border shadow-sm transition-all duration-300 ${
              isOpen
                ? 'border-signal-buy/20 text-signal-buy bg-signal-buy/5'
                : 'border-dark-border text-dark-muted bg-gray-800/20'
            }`}>
              <div className="flex items-center gap-2">
                <div className={`w-2 h-2 rounded-full ${isOpen ? 'bg-signal-buy animate-pulse' : 'bg-gray-400'}`} />
                <span>{isOpen ? 'MARKET LIVE' : `MARKET ${marketStatus}`}</span>
              </div>
              {isOpen && <span className="text-[10px] opacity-60 font-mono">IST</span>}
            </div>
          </div>

          {/* Nav links */}
          <nav className="flex-1 py-5 px-2 space-y-1">
            <p className="px-3 text-[10px] font-bold text-dark-muted uppercase tracking-widest mb-3">
              Navigation
            </p>
            {NAV_ITEMS.map(({ path, label, Icon }) => (
              <NavLink
                key={path}
                to={path}
                className={({ isActive }) =>
                  `flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-all group ${
                    isActive
                      ? 'bg-accent text-white shadow-lg shadow-accent/20'
                      : 'text-dark-muted hover:text-white hover:bg-accent/10'
                  }`
                }
              >
                <Icon size={17} className="transition-transform group-hover:scale-110 shrink-0" />
                <span className="truncate">{label}</span>
              </NavLink>
            ))}
          </nav>

          {/* Bottom status */}
          <div className="px-4 py-4 border-t border-dark-border space-y-3 bg-black/10">
            <div className={`flex items-center gap-2.5 text-xs font-medium ${isConnected ? 'text-signal-buy' : 'text-signal-sell'}`}>
              <div className={`p-1.5 rounded-lg ${isConnected ? 'bg-signal-buy/10' : 'bg-signal-sell/10'}`}>
                {isConnected ? <Wifi size={13} /> : <WifiOff size={13} />}
              </div>
              <span>{isConnected ? 'Real-time Active' : 'Feed Offline'}</span>
            </div>
            <div className="text-[10px] text-dark-muted font-mono flex justify-between">
              <span>Last tick</span>
              <span>{lastUpdate.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}</span>
            </div>
          </div>
        </aside>

        {/* Main content */}
        <main className="flex-1 min-w-0 overflow-auto">
          {children}
        </main>
      </div>
    </div>
  );
}
