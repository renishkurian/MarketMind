import React, { useEffect } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { Toaster } from 'react-hot-toast';
import { useWebSocket } from './hooks/useWebSocket';
import { useStockStore } from './store/stockStore';
import Layout from './components/Layout';

import Portfolio from './pages/Portfolio';
import Watchlist from './pages/Watchlist';
import Opportunities from './pages/Opportunities';
import AdminUsers from './pages/AdminUsers';
import DeepDive from './pages/DeepDive';
import Login from './pages/Login';
import Signup from './pages/Signup';
import Settings from './pages/Settings';
import AILogs from './pages/AILogs';
import Methodology from './pages/Methodology';
import MLDiscovery from './pages/MLDiscovery';
import PortfolioOptimizer from './pages/PortfolioOptimizer';
import OracleAI from './pages/OracleAI';
import WarRoom from './pages/WarRoom';
import BenchmarkDashboard from './pages/BenchmarkDashboard';
import StockPerformanceHeatmap from './pages/StockPerformanceHeatmap';
import { useAuthStore } from './store/authStore';

const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8000/ws/market';

// Simple wrapper for protected content
const ProtectedRoute = ({ children }) => {
  const { isAuthenticated } = useAuthStore();
  return isAuthenticated ? children : <Navigate to="/login" replace />;
};

function AppInner() {
  const { theme } = useStockStore();
  const { isAuthenticated, checkAuth, token } = useAuthStore();
  
  // ... (keeping other effects)

  useWebSocket(isAuthenticated ? `${WS_URL}?token=${token}` : null);

  useEffect(() => {
    checkAuth();
  }, []);

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
        <Route path="/signup" element={<Signup />} />

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
        <Route path="/ml-discovery" element={
          <ProtectedRoute><Layout><MLDiscovery /></Layout></ProtectedRoute>
        } />
        <Route path="/optimizer" element={
          <ProtectedRoute><Layout><PortfolioOptimizer /></Layout></ProtectedRoute>
        } />
        <Route path="/oracle" element={
          <ProtectedRoute><Layout><OracleAI /></Layout></ProtectedRoute>
        } />
        <Route path="/war-room" element={
          <ProtectedRoute><Layout><WarRoom /></Layout></ProtectedRoute>
        } />
        <Route path="/benchmark" element={
          <ProtectedRoute><Layout><BenchmarkDashboard /></Layout></ProtectedRoute>
        } />
        <Route path="/performance-heatmap" element={
          <ProtectedRoute><Layout><StockPerformanceHeatmap /></Layout></ProtectedRoute>
        } />

        <Route path="/stock/:symbol" element={
          <ProtectedRoute><Layout><DeepDive /></Layout></ProtectedRoute>
        } />

        <Route path="/ai-logs" element={
          <ProtectedRoute><Layout><AILogs /></Layout></ProtectedRoute>
        } />

        <Route path="/methodology" element={
          <ProtectedRoute><Layout><Methodology /></Layout></ProtectedRoute>
        } />

        <Route path="/admin/users" element={
          <ProtectedRoute><Layout><AdminUsers /></Layout></ProtectedRoute>
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
