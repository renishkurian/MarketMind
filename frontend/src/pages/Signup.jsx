import React, { useState } from 'react';
import { useAuthStore } from '../store/authStore';
import { Navigate, Link, useNavigate } from 'react-router-dom';
import { Lock, Mail, User, UserPlus, ArrowRight } from 'lucide-react';
import toast from 'react-hot-toast';

export default function Signup() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [fullName, setFullName] = useState('');
  const { signup, isAuthenticated, loading } = useAuthStore();
  const navigate = useNavigate();

  if (isAuthenticated) return <Navigate to="/" replace />;

  const handleSubmit = async (e) => {
    e.preventDefault();
    const result = await signup(email, password, fullName);
    if (result.success) {
      toast.success('Account created! Please sign in.');
      navigate('/login');
    } else {
      toast.error(result.message);
    }
  };

  return (
    <div className="min-h-screen bg-dark-bg flex items-center justify-center p-4">
      {/* Background blobs */}
      <div className="absolute top-1/4 right-1/4 w-64 h-64 bg-accent/10 rounded-full blur-3xl opacity-50" />
      <div className="absolute bottom-1/4 left-1/4 w-64 h-64 bg-blue-500/5 rounded-full blur-3xl opacity-50" />

      <div className="w-full max-w-md bg-dark-card border border-dark-border rounded-2xl p-8 shadow-2xl relative backdrop-blur-md">
        <div className="flex flex-col items-center mb-8">
          <div className="w-16 h-16 bg-accent/10 rounded-2xl flex items-center justify-center mb-4 border border-accent/20">
            <UserPlus size={32} className="text-accent" />
          </div>
          <h1 className="text-2xl font-bold text-dark-text tracking-tight">Create Account</h1>
          <p className="text-dark-muted text-sm mt-1">Join the MarketMind intelligence network</p>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-2">
            <label className="text-xs font-semibold text-dark-muted uppercase tracking-wider ml-1">Full Name</label>
            <div className="relative">
              <User className="absolute left-3 top-1/2 -translate-y-1/2 text-dark-muted" size={18} />
              <input
                type="text"
                value={fullName}
                onChange={(e) => setFullName(e.target.value)}
                className="w-full pl-10 pr-4 py-3 bg-dark-bg border border-dark-border rounded-xl focus:outline-none focus:ring-1 focus:ring-accent transition-all text-sm text-dark-text"
                placeholder="John Doe"
                required
              />
            </div>
          </div>

          <div className="space-y-2">
            <label className="text-xs font-semibold text-dark-muted uppercase tracking-wider ml-1">Email Address</label>
            <div className="relative">
              <Mail className="absolute left-3 top-1/2 -translate-y-1/2 text-dark-muted" size={18} />
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="w-full pl-10 pr-4 py-3 bg-dark-bg border border-dark-border rounded-xl focus:outline-none focus:ring-1 focus:ring-accent transition-all text-sm text-dark-text"
                placeholder="name@example.com"
                required
              />
            </div>
          </div>

          <div className="space-y-2">
            <label className="text-xs font-semibold text-dark-muted uppercase tracking-wider ml-1">Password</label>
            <div className="relative">
              <Lock className="absolute left-3 top-1/2 -translate-y-1/2 text-dark-muted" size={18} />
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="w-full pl-10 pr-4 py-3 bg-dark-bg border border-dark-border rounded-xl focus:outline-none focus:ring-1 focus:ring-accent transition-all text-sm text-dark-text"
                placeholder="••••••••"
                required
              />
            </div>
          </div>

          <button
            type="submit"
            disabled={loading}
            className="w-full py-3 bg-accent hover:bg-blue-600 text-white font-bold rounded-xl transition-all shadow-lg shadow-accent/10 flex items-center justify-center gap-2 group disabled:opacity-50 mt-4"
          >
            {loading ? (
              <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
            ) : (
              <>
                Create Account
                <ArrowRight size={18} className="group-hover:translate-x-1 transition-transform" />
              </>
            )}
          </button>
        </form>

        <div className="mt-8 pt-6 border-t border-dark-border text-center">
          <p className="text-sm text-dark-muted">
            Already have an account?{' '}
            <Link to="/login" className="text-accent hover:text-blue-400 font-semibold transition-colors">
              Sign In
            </Link>
          </p>
        </div>
      </div>
    </div>
  );
}
