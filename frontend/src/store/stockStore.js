import { create } from 'zustand';

export const useStockStore = create((set, get) => ({
  stocks: {}, // keyed by symbol
  marketStatus: 'CLOSED',
  lastUpdate: new Date(),
  alerts: [],
  theme: localStorage.getItem('theme') || 'dark',
  isConnected: false,

  setStocks: (stocksArray) => {
    const newStocks = {};
    stocksArray.forEach(s => {
      newStocks[s.symbol] = { ...get().stocks[s.symbol], ...s };
    });
    set({ stocks: newStocks });
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
