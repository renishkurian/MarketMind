import React, { useEffect } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { Toaster } from 'react-hot-toast';
import { useWebSocket } from './hooks/useWebSocket';
import { useStockStore } from './store/stockStore';
import Layout from './components/Layout';

import Portfolio from './pages/Portfolio';
import Watchlist from './pages/Watchlist';
import Opportunities from './pages/Opportunities';
import DeepDive from './pages/DeepDive';
import Login from './pages/Login';
import Settings from './pages/Settings';
import AILogs from './pages/AILogs';
import { useAuthStore } from './store/authStore';

const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8000/ws/market';

// Simple wrapper for protected content
const ProtectedRoute = ({ children }) => {
  const { isAuthenticated } = useAuthStore();
  return isAuthenticated ? children : <Navigate to="/login" replace />;
};

function AppInner() {
  const { theme } = useStockStore();
  const { isAuthenticated, checkAuth } = useAuthStore();

  // Initialize Auth check once on mount
  useEffect(() => {
    checkAuth();
  }, [checkAuth]);

  // Initialize WebSocket at root if authenticated
  useEffect(() => {
    if (isAuthenticated) {
      // useWebSocket is a hook, it can't be called conditionally easily if it uses other hooks.
      // But we call it inside the component. We'll handle connection logic inside the hook itself
      // if it needs to wait for auth. For now, we'll keep it simple.
    }
  }, [isAuthenticated]);
  
  useWebSocket(isAuthenticated ? WS_URL : null);

  useEffect(() => {
    document.documentElement.classList.toggle('dark', theme === 'dark');
  }, [theme]);

  return (
    <>
      <Toaster
        position="top-right"
        toastOptions={{
          style: {
            background: '#111827',
            color: '#F9FAFB',
            border: '1px solid #1F2937',
            fontFamily: 'Inter, sans-serif',
            fontSize: '13px',
          },
        }}
      />
      <Routes>
        {/* Public Routes */}
        <Route path="/login" element={<Login />} />

        {/* Protected Routes */}
        <Route path="/" element={<ProtectedRoute><Navigate to="/portfolio" replace /></ProtectedRoute>} />

        <Route path="/portfolio" element={
          <ProtectedRoute><Layout><Portfolio /></Layout></ProtectedRoute>
        } />
        <Route path="/watchlist" element={
          <ProtectedRoute><Layout><Watchlist /></Layout></ProtectedRoute>
        } />
        <Route path="/opportunities" element={
          <ProtectedRoute><Layout><Opportunities /></Layout></ProtectedRoute>
        } />
        <Route path="/settings" element={
          <ProtectedRoute><Layout><Settings /></Layout></ProtectedRoute>
        } />

        <Route path="/stock/:symbol" element={
          <ProtectedRoute><Layout><DeepDive /></Layout></ProtectedRoute>
        } />

        <Route path="/ai-logs" element={
          <ProtectedRoute><Layout><AILogs /></Layout></ProtectedRoute>
        } />

        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <AppInner />
    </BrowserRouter>
  );
}
