import React, { useState } from 'react';
import { useAuthStore } from '../store/authStore';
import { Navigate } from 'react-router-dom';
import { Lock, User, LogIn } from 'lucide-react';
import toast from 'react-hot-toast';

export default function Login() {
  const [username, setUsername] = useState('admin');
  const [password, setPassword] = useState('');
  const { login, isAuthenticated, loading } = useAuthStore();

  if (isAuthenticated) return <Navigate to="/" replace />;

  const handleSubmit = async (e) => {
    e.preventDefault();
    const result = await login(username, password);
    if (result.success) {
      toast.success('Welcome back, Admin');
    } else {
      toast.error(result.message);
    }
  };

  return (
    <div className="min-h-screen bg-dark-bg flex items-center justify-center p-4">
      {/* Background blobs for depth */}
      <div className="absolute top-1/4 left-1/4 w-64 h-64 bg-accent/10 rounded-full blur-3xl" />
      <div className="absolute bottom-1/4 right-1/4 w-64 h-64 bg-signal-buy/5 rounded-full blur-3xl" />

      <div className="w-full max-w-md bg-dark-card border border-dark-border rounded-2xl p-8 shadow-2xl relative backdrop-blur-sm">
        <div className="flex flex-col items-center mb-8">
          <div className="w-16 h-16 bg-accent/10 rounded-2xl flex items-center justify-center mb-4 border border-accent/20">
            <LogIn size={32} className="text-accent" />
          </div>
          <h1 className="text-2xl font-bold text-dark-text tracking-tight">MarketMind Access</h1>
          <p className="text-dark-muted text-sm mt-1">Intelligence for your local portfolio</p>
        </div>

        <form onSubmit={handleSubmit} className="space-y-6 text-dark-text">
          <div className="space-y-2 text-dark-text">
            <label className="text-xs font-semibold text-dark-muted uppercase tracking-wider ml-1">Username</label>
            <div className="relative">
              <User className="absolute left-3 top-1/2 -translate-y-1/2 text-dark-muted" size={18} />
              <input
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                className="w-full pl-10 pr-4 py-3 bg-dark-bg border border-dark-border rounded-xl focus:outline-none focus:ring-2 focus:ring-accent transition-all text-sm font-medium"
                placeholder="Admin"
                required
              />
            </div>
          </div>

          <div className="space-y-2 text-dark-text">
            <label className="text-xs font-semibold text-dark-muted uppercase tracking-wider ml-1">Password</label>
            <div className="relative">
              <Lock className="absolute left-3 top-1/2 -translate-y-1/2 text-dark-muted" size={18} />
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="w-full pl-10 pr-4 py-3 bg-dark-bg border border-dark-border rounded-xl focus:outline-none focus:ring-2 focus:ring-accent transition-all text-sm font-medium"
                placeholder="••••••••"
                required
              />
            </div>
          </div>

          <button
            type="submit"
            disabled={loading}
            className="w-full py-3 bg-accent hover:bg-blue-500 text-white font-bold rounded-xl transition-all shadow-lg shadow-accent/20 flex items-center justify-center gap-2 group disabled:opacity-50"
          >
            {loading ? (
              <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
            ) : (
              <>
                Unlock Dashboard
                <LogIn size={18} className="group-hover:translate-x-1 transition-transform" />
              </>
            )}
          </button>
        </form>

        <p className="mt-8 text-center text-xs text-dark-muted">
          Your credentials are kept secure locally.
        </p>
      </div>
    </div>
  );
}
