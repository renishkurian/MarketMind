import { create } from 'zustand';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export const useAuthStore = create((set, get) => ({
  token: localStorage.getItem('mm_token') || null,
  isAuthenticated: !!localStorage.getItem('mm_token'),
  user: null,
  loading: false,

  login: async (username, password) => {
    set({ loading: true });
    try {
      const res = await axios.post(`${API_URL}/api/auth/login`, { username, password });
      const { access_token } = res.data;
      
      localStorage.setItem('mm_token', access_token);
      axios.defaults.headers.common['Authorization'] = `Bearer ${access_token}`;
      set({ token: access_token, isAuthenticated: true, loading: false });
      
      // Fetch user profile immediately to populate role for UI
      await get().checkAuth();
      
      return { success: true };
    } catch (err) {
      set({ loading: false });
      return { 
        success: false, 
        message: err.response?.data?.detail || 'Login failed' 
      };
    }
  },

  signup: async (email, password, fullName) => {
    set({ loading: true });
    try {
      await axios.post(`${API_URL}/api/auth/register`, { 
        email, 
        password, 
        full_name: fullName 
      });
      set({ loading: false });
      return { success: true };
    } catch (err) {
      set({ loading: false });
      return { 
        success: false, 
        message: err.response?.data?.detail || 'Signup failed' 
      };
    }
  },

  logout: () => {
    localStorage.removeItem('mm_token');
    delete axios.defaults.headers.common['Authorization'];
    set({ token: null, isAuthenticated: false, user: null });
  },

  checkAuth: async () => {
    const { token } = get();
    if (!token) return;

    axios.defaults.headers.common['Authorization'] = `Bearer ${token}`;
    try {
      const res = await axios.get(`${API_URL}/api/auth/verify`);
      set({ user: res.data.user, isAuthenticated: true });
    } catch (err) {
      // Token expired or invalid
      get().logout();
    }
  }
}));
