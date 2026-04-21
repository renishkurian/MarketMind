import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Users, Shield, User as UserIcon, CheckCircle, XCircle, MoreVertical, RefreshCw } from 'lucide-react';
import { useAuthStore } from '../store/authStore';

const API_URL = import.meta.env.VITE_API_URL || 'http://127.0.0.1:8000';

export default function AdminUsers() {
  const { token } = useAuthStore();
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [actionLoading, setActionLoading] = useState(null);

  const fetchUsers = async () => {
    try {
      setLoading(true);
      const resp = await axios.get(`${API_URL}/api/admin/users`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      setUsers(resp.data);
      setError(null);
    } catch (err) {
      setError('Failed to fetch users. Ensure you have admin privileges.');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const toggleStatus = async (userId, currentStatus) => {
    try {
      setActionLoading(userId);
      await axios.patch(`${API_URL}/api/admin/users/${userId}`, 
        { is_active: !currentStatus },
        { headers: { Authorization: `Bearer ${token}` } }
      );
      await fetchUsers();
    } catch (err) {
      alert(err.response?.data?.detail || 'Failed to update user status');
    } finally {
      setActionLoading(null);
    }
  };

  const toggleRole = async (userId, currentRole) => {
    const newRole = currentRole === 'ADMIN' ? 'USER' : 'ADMIN';
    if (!window.confirm(`Are you sure you want to change this user to ${newRole}?`)) return;

    try {
      setActionLoading(userId);
      await axios.patch(`${API_URL}/api/admin/users/${userId}`, 
        { role: newRole },
        { headers: { Authorization: `Bearer ${token}` } }
      );
      await fetchUsers();
    } catch (err) {
      alert(err.response?.data?.detail || 'Failed to update user role');
    } finally {
      setActionLoading(null);
    }
  };

  useEffect(() => {
    fetchUsers();
  }, []);

  return (
    <div className="p-6 md:p-8 space-y-8 animate-in fade-in slide-in-from-bottom-4 duration-700">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-dark-text tracking-tight flex items-center gap-3">
            <Users className="text-accent" size={32} />
            User Management
          </h1>
          <p className="text-dark-muted mt-1 font-medium">Control platform access and administrative privileges</p>
        </div>
        <button 
          onClick={fetchUsers}
          className="flex items-center gap-2 px-4 py-2 bg-dark-card border border-dark-border rounded-xl text-sm font-semibold text-dark-text hover:bg-accent/10 hover:border-accent/30 transition-all"
        >
          <RefreshCw size={16} className={loading ? 'animate-spin' : ''} />
          Refresh
        </button>
      </div>

      {error ? (
        <div className="p-4 bg-signal-sell/10 border border-signal-sell/20 rounded-2xl text-signal-sell text-center font-medium">
          {error}
        </div>
      ) : (
        <div className="bg-dark-card border border-dark-border rounded-2xl overflow-hidden shadow-2xl shadow-black/20">
          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse">
              <thead>
                <tr className="border-b border-dark-border bg-black/20">
                  <th className="px-6 py-4 text-xs font-bold text-dark-muted uppercase tracking-wider">User</th>
                  <th className="px-6 py-4 text-xs font-bold text-dark-muted uppercase tracking-wider">Role</th>
                  <th className="px-6 py-4 text-xs font-bold text-dark-muted uppercase tracking-wider">Status</th>
                  <th className="px-6 py-4 text-xs font-bold text-dark-muted uppercase tracking-wider">Joined</th>
                  <th className="px-6 py-4 text-xs font-bold text-dark-muted uppercase tracking-wider text-right">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-dark-border/50">
                {loading ? (
                  Array(5).fill(0).map((_, i) => (
                    <tr key={i} className="animate-pulse">
                      <td colSpan="5" className="px-6 py-8 h-16 bg-white/5"></td>
                    </tr>
                  ))
                ) : users.map((user) => (
                  <tr key={user.id} className="hover:bg-white/5 transition-colors group">
                    <td className="px-6 py-4">
                      <div className="flex items-center gap-3">
                        <div className="w-10 h-10 rounded-xl bg-accent/10 flex items-center justify-center text-accent">
                          <UserIcon size={20} />
                        </div>
                        <div>
                          <p className="font-bold text-dark-text leading-none capitalize">{user.full_name || 'Guest User'}</p>
                          <p className="text-xs text-dark-muted mt-1">{user.email}</p>
                        </div>
                      </div>
                    </td>
                    <td className="px-6 py-4">
                      <div className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[10px] font-bold tracking-wider uppercase border ${
                        user.role === 'ADMIN' 
                          ? 'bg-accent/10 text-accent border-accent/20' 
                          : 'bg-dark-muted/10 text-dark-muted border-dark-border'
                      }`}>
                        <Shield size={10} />
                        {user.role}
                      </div>
                    </td>
                    <td className="px-6 py-4">
                      <div className={`inline-flex items-center gap-1.5 font-bold text-xs ${
                        user.is_active ? 'text-signal-buy' : 'text-signal-sell'
                      }`}>
                        {user.is_active ? <CheckCircle size={14} /> : <XCircle size={14} />}
                        {user.is_active ? 'Active' : 'Deactivated'}
                      </div>
                    </td>
                    <td className="px-6 py-4">
                      <p className="text-sm text-dark-muted font-mono">
                        {new Date(user.created_at).toLocaleDateString()}
                      </p>
                    </td>
                    <td className="px-6 py-4 text-right">
                      <div className="flex items-center justify-end gap-2">
                        <button
                          onClick={() => toggleRole(user.id, user.role)}
                          className="px-3 py-1.5 rounded-lg text-[10px] font-bold uppercase border border-dark-border text-dark-muted hover:border-accent hover:text-accent transition-all"
                          disabled={actionLoading === user.id}
                        >
                          Change Role
                        </button>
                        <button
                          onClick={() => toggleStatus(user.id, user.is_active)}
                          className={`px-3 py-1.5 rounded-lg text-[10px] font-bold uppercase border ${
                            user.is_active 
                              ? 'border-signal-sell/30 text-signal-sell/70 hover:bg-signal-sell/10 hover:text-signal-sell' 
                              : 'border-signal-buy/30 text-signal-buy/70 hover:bg-signal-buy/10 hover:text-signal-buy'
                          } transition-all`}
                          disabled={actionLoading === user.id}
                        >
                          {user.is_active ? 'Deactivate' : 'Activate'}
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {!loading && users.length === 0 && (
            <div className="p-12 text-center text-dark-muted font-medium">
              No users found.
            </div>
          )}
        </div>
      )}
    </div>
  );
}
