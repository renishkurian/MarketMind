import { create } from 'zustand';

export const useStockStore = create((set, get) => ({
  stocks: {}, // keyed by symbol
  marketStatus: 'CLOSED',
  lastUpdate: new Date(),
  alerts: [],
  theme: localStorage.getItem('theme') || 'dark',
  isConnected: false,
  tickerEnabled: localStorage.getItem('mm_ticker_enabled') !== 'false',

  toggleTicker: () => {
    const newState = !get().tickerEnabled;
    localStorage.setItem('mm_ticker_enabled', String(newState));
    set({ tickerEnabled: newState });
  },

  setStocks: (stocksArray) => {
    const newStocks = {};
    stocksArray.forEach(s => {
      newStocks[s.symbol] = { ...get().stocks[s.symbol], ...s };
    });
    set({ stocks: newStocks });
  },

  fetchPortfolio: async () => {
    try {
      const token = localStorage.getItem('mm_token');
      if (!token) return;
      
      const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';
      const response = await fetch(`${API_URL}/api/portfolio`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      
      if (response.ok) {
        const data = await response.json();
        // data.forEach(s => get().updatePrice(s.symbol, s.signal?.current_price, s.signal?.change_pct));
        get().setStocks(data);
      }
    } catch (error) {
      console.error("HTTP fetch failed:", error);
    }
  },

  updatePrice: (symbol, price, change_pct) => {
    set((state) => ({
      stocks: {
        ...state.stocks,
        [symbol]: {
          ...state.stocks[symbol],
          symbol,
          signal: {
            ...state.stocks[symbol]?.signal,
            current_price: price,
            change_pct: change_pct
          },
          _lastPriceChange: state.stocks[symbol]?.signal?.current_price && price !== state.stocks[symbol].signal.current_price 
                            ? (price > state.stocks[symbol].signal.current_price ? 'up' : 'down') 
                            : null
        }
      },
      lastUpdate: new Date()
    }));
  },

  updateSignal: (symbol, signals) => {
    set((state) => ({
      stocks: {
        ...state.stocks,
        [symbol]: {
          ...state.stocks[symbol],
          signal: {
            ...state.stocks[symbol]?.signal,
            ...signals
          }
        }
      }
    }));
  },

  setMarketStatus: (status) => set({ marketStatus: status }),
  
  setConnectionStatus: (status) => set({ isConnected: status }),

  addAlert: (alert) => set((state) => ({
    alerts: [{ ...alert, id: Date.now() }, ...state.alerts]
  })),

  clearAlert: (id) => set((state) => ({
    alerts: state.alerts.filter(a => a.id !== id)
  })),

  toggleTheme: () => {
    const newTheme = get().theme === 'dark' ? 'light' : 'dark';
    localStorage.setItem('theme', newTheme);
    set({ theme: newTheme });
    if (newTheme === 'dark') {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
  }
}));
