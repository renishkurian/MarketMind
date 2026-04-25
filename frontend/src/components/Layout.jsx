import React, { useState } from 'react';
import { NavLink, useNavigate } from 'react-router-dom';
import {
  Activity, BarChart2, Eye, Zap, Brain, Info,
  Sun, Moon, Wifi, WifiOff, Settings, LogOut,
  Menu, X, ChevronLeft, Users, PieChart, LayoutDashboard, Trophy,
  Shield
} from 'lucide-react';
import { useStockStore } from '../store/stockStore';
import { useAuthStore } from '../store/authStore';

const NAV_SECTIONS = [
  {
    label: 'Market',
    items: [
      { path: '/portfolio',     label: 'Portfolio',     Icon: BarChart2 },
      { path: '/watchlist',     label: 'Watchlist',     Icon: Eye        },
      { path: '/opportunities', label: 'Opportunities', Icon: Zap        },
      { path: '/benchmark',     label: 'Performance',   Icon: BarChart2  },
    ]
  },
  {
    label: 'AI Intelligence',
    items: [
      { path: '/ml-discovery',  label: 'Alpha Discovery', Icon: Brain      },
      { path: '/optimizer',     label: 'Portfolio Opt',   Icon: PieChart   },
      { path: '/oracle',        label: 'The Oracle',      Icon: Trophy     },
      { path: '/war-room',      label: 'War Room',        Icon: Shield     },
    ]
  },
  {
    label: 'System',
    items: [
      { path: '/ai-logs',       label: 'AI Logs',       Icon: LayoutDashboard },
      { path: '/methodology',   label: 'Methodology',   Icon: Info       },
      { path: '/settings',      label: 'Settings',      Icon: Settings   },
    ]
  }
];

export default function Layout({ children }) {
  const { marketStatus, theme, toggleTheme, isConnected, lastUpdate } = useStockStore();
  const { user, logout } = useAuthStore();
  const navigate = useNavigate();
  const isOpen = marketStatus === 'OPEN';
  const [sidebarOpen, setSidebarOpen] = useState(true);

  return (
    <div className="flex flex-col h-screen bg-light-bg dark:bg-dark-bg text-light-text dark:text-dark-text font-sans transition-colors duration-500 overflow-hidden">

      {/* ── Top Header ───────────────────────────────────────────────── */}
      <header className="fixed top-0 left-0 right-0 z-50 flex items-center justify-between px-4 h-14 border-b border-light-border dark:border-dark-border bg-white/80 dark:bg-dark-card/80 backdrop-blur-xl transition-all duration-300">
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
            <span className="font-bold font-mono tracking-tighter text-accent text-lg leading-none">MarketMind</span>
          </div>
        </div>

        {/* ws status + theme + user + logout */}
        <div className="flex items-center gap-1">
          <div className={`flex items-center gap-1.5 text-xs font-medium px-2 py-1 rounded-lg mr-1 ${
            isConnected ? 'text-signal-buy bg-signal-buy/10' : 'text-signal-sell bg-signal-sell/10'
          }`}>
            {isConnected ? <Wifi size={13} /> : <WifiOff size={13} />}
            <span className="hidden sm:inline">{isConnected ? 'Live' : 'Offline'}</span>
          </div>
          
          <button onClick={toggleTheme} className="p-2 rounded-lg hover:bg-accent/10 text-dark-muted hover:text-accent transition-all">
            {theme === 'dark' ? <Sun size={17} /> : <Moon size={17} />}
          </button>

          {user && (
            <div className="flex items-center gap-2 px-2 py-1 bg-accent/5 border border-accent/10 rounded-lg ml-1">
              <div className="w-6 h-6 rounded-md bg-accent flex items-center justify-center text-[10px] font-bold text-white uppercase">
                {user.full_name?.charAt(0) || user.email?.charAt(0)}
              </div>
              <span className="text-xs font-medium text-dark-text hidden md:inline max-w-[100px] truncate">
                {user.full_name || user.email}
              </span>
            </div>
          )}

          <button onClick={() => { logout(); navigate('/login'); }} className="p-2 rounded-lg hover:bg-signal-sell/10 text-dark-muted hover:text-signal-sell transition-all ml-1" title="Logout">
            <LogOut size={17} />
          </button>
        </div>
      </header>

      <div className="flex flex-1 pt-14 overflow-hidden">
        {/* Sidebar */}
        <aside
          className={`shrink-0 flex flex-col border-r border-light-border dark:border-dark-border bg-white dark:bg-dark-card transition-all duration-300 ease-in-out h-full ${
            sidebarOpen ? 'w-64' : 'w-0 overflow-hidden border-r-0'
          }`}
        >
          {/* Market status block */}
          <div className="px-4 py-6 border-b border-light-border dark:border-dark-border bg-gray-50/50 dark:bg-black/10">
            <div className={`flex items-center justify-between px-3 py-2.5 rounded-xl text-xs font-semibold border shadow-sm ${
              isOpen ? 'border-signal-buy/20 text-signal-buy bg-signal-buy/5' : 'border-dark-border text-dark-muted bg-gray-800/20'
            }`}>
              <div className="flex items-center gap-2">
                <div className={`w-2 h-2 rounded-full ${isOpen ? 'bg-signal-buy animate-pulse' : 'bg-gray-400'}`} />
                <span>{isOpen ? 'MARKET LIVE' : `MARKET ${marketStatus}`}</span>
              </div>
            </div>
          </div>

          {/* Nav links grouped by Section */}
          <nav className="flex-1 py-6 px-3 space-y-8 overflow-y-auto custom-scrollbar">
            {NAV_SECTIONS.map((section) => (
              <div key={section.label} className="space-y-1.5">
                <p className="px-4 mb-3 text-[10px] font-black text-dark-muted/50 uppercase tracking-[0.2em]">
                   {section.label}
                </p>
                {section.items.map(({ path, label, Icon }) => (
                  <NavLink
                    key={path}
                    to={path}
                    className={({ isActive }) =>
                      `flex items-center gap-3.5 px-4 py-3 rounded-xl text-sm font-semibold transition-all group ${
                        isActive
                          ? 'bg-accent text-white shadow-lg shadow-accent/20'
                          : 'text-dark-muted hover:bg-accent/10 hover:text-accent'
                      }`
                    }
                  >
                    <Icon size={18} className="shrink-0 transition-transform group-hover:scale-110" />
                    <span>{label}</span>
                  </NavLink>
                ))}
              </div>
            ))}

            {/* Admin only section */}
            {user?.role === 'ADMIN' && (
              <div className="pt-4 mt-4 border-t border-dark-border/30">
                <p className="px-4 mb-2 text-[10px] font-bold text-dark-muted uppercase tracking-widest">Administration</p>
                <NavLink
                  to="/admin/users"
                  className={({ isActive }) =>
                    `flex items-center gap-3.5 px-4 py-3 rounded-xl text-sm font-semibold transition-all group ${
                      isActive
                        ? 'bg-accent text-white shadow-lg shadow-accent/20'
                        : 'text-dark-muted hover:bg-accent/10 hover:text-accent'
                    }`
                  }
                >
                  <Users size={18} className="shrink-0 transition-transform group-hover:scale-110" />
                  <span>User Management</span>
                </NavLink>
              </div>
            )}
          </nav>

          {/* Stats Bar */}
          <div className="p-4 border-t border-dark-border/50 bg-gray-50 dark:bg-black/20">
            <div className="flex items-center gap-2 text-[10px] font-mono text-dark-muted">
              <Activity size={10} />
              <span>Session: {new Date().toLocaleDateString('en-IN')}</span>
            </div>
          </div>
        </aside>

        {/* Main Content */}
        <main className="flex-1 min-w-0 bg-light-bg dark:bg-dark-bg transition-colors overflow-y-auto custom-scrollbar">
          <div className="h-full">
            {children}
          </div>
        </main>
      </div>
    </div>
  );
}
