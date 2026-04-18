import { useEffect, useRef } from 'react';
import { useStockStore } from '../store/stockStore';
import toast from 'react-hot-toast';

export const useWebSocket = (url) => {
  const ws = useRef(null);
  const reconnectTimeout = useRef(null);
  const { 
    setStocks, updatePrice, updateSignal, 
    setMarketStatus, setConnectionStatus, addAlert 
  } = useStockStore();

  const connect = () => {
    if (ws.current?.readyState === WebSocket.OPEN) return;

    ws.current = new WebSocket(url);

    ws.current.onopen = () => {
      console.log('WebSocket connected');
      setConnectionStatus(true);
      if (reconnectTimeout.current) clearTimeout(reconnectTimeout.current);
    };

    ws.current.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data);
        handleMessage(message);
      } catch (err) {
        console.error("Failed to parse WS message", err);
      }
    };

    ws.current.onclose = () => {
      console.log('WebSocket disconnected. Reconnecting...');
      setConnectionStatus(false);
      reconnectTimeout.current = setTimeout(connect, 3000); // Exponetial backoff could go here
    };

    ws.current.onerror = (error) => {
      console.error('WebSocket Error:', error);
      ws.current.close();
    };
  };

  const handleMessage = ({ type, data }) => {
    switch(type) {
      case 'snapshot':
        setStocks(data);
        break;
      case 'price_update':
        updatePrice(data.symbol, data.price, data.change_pct);
        break;
      case 'signal_update':
        updateSignal(data.symbol, data);
        break;
      case 'market_status':
        setMarketStatus(data.status);
        break;
      case 'alert':
        addAlert(data);
        toast(data.message, {
          icon: data.type === 'PRICE_SPIKE' ? '⚡' : '🔔',
          style: {
            background: '#111827',
            color: '#fff',
            border: '1px solid #3B82F6'
          }
        });
        break;
      default:
        console.warn('Unknown message type:', type);
    }
  };

  useEffect(() => {
    connect();
    return () => {
      if (reconnectTimeout.current) clearTimeout(reconnectTimeout.current);
      if (ws.current) ws.current.close();
    };
  }, [url]);

  return { ws: ws.current };
};
