import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Settings as SettingsIcon, Key, Save, Server, ShieldCheck, History, AlertCircle, CheckCircle2 } from 'lucide-react';
import toast from 'react-hot-toast';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export default function Settings() {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [settings, setSettings] = useState({
    anthropic_api_key: '',
    anthropic_model: 'claude-3-5-sonnet-20240620',
    openai_api_key: '',
    openai_model: 'gpt-5.4',
    xai_api_key: '',
    xai_model: 'grok-beta',
    ai_provider: 'anthropic',
    version: ''
  });
  const [config, setConfig] = useState({});
  const [logs, setLogs] = useState([]);
  const [fetchingLogs, setFetchingLogs] = useState(false);

  useEffect(() => {
    fetchSettings();
    fetchConfig();
    fetchLogs();
  }, []);

  const fetchConfig = async () => {
    try {
      const token = localStorage.getItem('mm_token');
      const res = await axios.get(`${API_URL}/api/settings/config`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      setConfig(res.data);
    } catch (err) {
      console.error('Failed to fetch config', err);
    }
  };

  const updateConfig = async (key, value) => {
    try {
      const token = localStorage.getItem('mm_token');
      await axios.patch(`${API_URL}/api/settings/config`, { key, value }, {
        headers: { Authorization: `Bearer ${token}` }
      });
      toast.success(`Updated ${key} successfully`);
      fetchConfig();
    } catch (err) {
      toast.error('Failed to update configuration');
    }
  };

  const fetchLogs = async () => {
    setFetchingLogs(true);
    try {
      const token = localStorage.getItem('mm_token');
      const res = await axios.get(`${API_URL}/api/market/bhavcopy/logs`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      setLogs(res.data);
    } catch (err) {
      console.error('Failed to fetch logs', err);
    } finally {
      setFetchingLogs(false);
    }
  };

  const fetchSettings = async () => {
    try {
      const token = localStorage.getItem('mm_token');
      const res = await axios.get(`${API_URL}/api/settings`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      setSettings(res.data);
    } catch (err) {
      toast.error('Failed to load settings');
    } finally {
      setLoading(false);
    }
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      const token = localStorage.getItem('mm_token');
      // Clear empty fields to avoid sending unnecessary data
      const payload = { ...settings };
      await axios.post(`${API_URL}/api/settings`, payload, {
        headers: { Authorization: `Bearer ${token}` }
      });
      toast.success('Settings updated successfully');
    } catch (err) {
      console.error('Save error:', err.response?.data || err.message);
      toast.error(err.response?.data?.detail || 'Failed to save settings');
    } finally {
      setSaving(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <div className="w-8 h-8 border-4 border-accent/20 border-t-accent rounded-full animate-spin" />
      </div>
    );
  }

  return (
    <div className="max-w-4xl mx-auto space-y-8 p-6 text-dark-text">
        <div className="flex items-end justify-between">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">System Settings</h1>
            <p className="text-dark-muted mt-2">Manage your AI keys and system configuration.</p>
          </div>
          <button
            onClick={handleSave}
            disabled={saving}
            className="flex items-center gap-2 px-6 py-2.5 bg-accent hover:bg-blue-500 text-white font-bold rounded-xl transition-all shadow-lg shadow-accent/20 disabled:opacity-50"
          >
            {saving ? <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin" /> : <Save size={20} />}
            Save Changes
          </button>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
          {/* Main settings area */}
          <div className="md:col-span-2 space-y-6">
            {/* AI Settings Section */}
            <div className="bg-dark-card border border-dark-border rounded-2xl p-6 shadow-sm overflow-hidden relative">
               <div className="flex items-center gap-3 mb-6">
                <div className="p-2 bg-accent/10 rounded-lg">
                  <Key size={20} className="text-accent" />
                </div>
                <h2 className="text-lg font-bold">AI Intelligence</h2>
              </div>

               <div className="space-y-6">
                <div className="space-y-2">
                  <label className="text-sm font-semibold text-dark-muted">Preferred AI Provider</label>
                  <select
                    value={settings.ai_provider}
                    onChange={(e) => setSettings({ ...settings, ai_provider: e.target.value })}
                    className="w-full px-4 py-3 bg-dark-bg border border-dark-border rounded-xl focus:outline-none focus:ring-2 focus:ring-accent transition-all text-sm appearance-none"
                  >
                    <option value="anthropic">Anthropic (Claude-3.5 Sonnet)</option>
                    <option value="openai">OpenAI (GPT-4o)</option>
                    <option value="xai">xAI (Grok-1)</option>
                  </select>
                </div>

                <div className="grid grid-cols-1 gap-6">
                  {/* Anthropic Section */}
                  <div className="p-4 bg-dark-bg/50 border border-dark-border rounded-xl space-y-4">
                    <h3 className="text-sm font-bold text-accent">Anthropic (Claude)</h3>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <label className="text-xs font-semibold text-dark-muted">API Key</label>
                        <input
                          type="password"
                          value={settings.anthropic_api_key}
                          onChange={(e) => setSettings({ ...settings, anthropic_api_key: e.target.value })}
                          className="w-full px-4 py-2.5 bg-dark-bg border border-dark-border rounded-lg focus:outline-none focus:ring-1 focus:ring-accent transition-all font-mono text-xs"
                          placeholder="sk-ant-..."
                        />
                      </div>
                      <div className="space-y-2">
                        <label className="text-xs font-semibold text-dark-muted">Model Version</label>
                        <select
                          value={settings.anthropic_model}
                          onChange={(e) => setSettings({ ...settings, anthropic_model: e.target.value })}
                          className="w-full px-4 py-2.5 bg-dark-bg border border-dark-border rounded-lg focus:outline-none focus:ring-1 focus:ring-accent transition-all text-xs"
                        >
                          <option value="claude-3-5-sonnet-20240620">Claude 3.5 Sonnet</option>
                          <option value="claude-3-opus-20240229">Claude 3 Opus</option>
                          <option value="claude-3-haiku-20240307">Claude 3 Haiku</option>
                        </select>
                      </div>
                    </div>
                  </div>

                  {/* OpenAI Section */}
                  <div className="p-4 bg-dark-bg/50 border border-dark-border rounded-xl space-y-4">
                    <h3 className="text-sm font-bold text-[#10A37F]">OpenAI (ChatGPT)</h3>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <label className="text-xs font-semibold text-dark-muted">API Key</label>
                        <input
                          type="password"
                          value={settings.openai_api_key}
                          onChange={(e) => setSettings({ ...settings, openai_api_key: e.target.value })}
                          className="w-full px-4 py-2.5 bg-dark-bg border border-dark-border rounded-lg focus:outline-none focus:ring-1 focus:ring-accent transition-all font-mono text-xs"
                          placeholder="sk-..."
                        />
                      </div>
                      <div className="space-y-2">
                        <label className="text-xs font-semibold text-dark-muted">Model Version</label>
                        <select
                          value={settings.openai_model}
                          onChange={(e) => setSettings({ ...settings, openai_model: e.target.value })}
                          className="w-full px-4 py-2.5 bg-dark-bg border border-dark-border rounded-lg focus:outline-none focus:ring-1 focus:ring-accent transition-all text-xs"
                        >
                          <optgroup label="Frontier (GPT-5.4)">
                            <option value="gpt-5.4">GPT-5.4 (Best)</option>
                            <option value="gpt-5.4-mini">GPT-5.4 Mini</option>
                            <option value="gpt-5.4-nano">GPT-5.4 Nano</option>
                          </optgroup>
                          <optgroup label="Balanced (GPT-4.1)">
                            <option value="gpt-4.1">GPT-4.1</option>
                            <option value="gpt-4.1-mini">GPT-4.1 Mini</option>
                            <option value="gpt-4.1-nano">GPT-4.1 Nano</option>
                          </optgroup>
                          <optgroup label="Multimodal (GPT-4o)">
                            <option value="gpt-4o">GPT-4o (Real-time)</option>
                            <option value="gpt-4o-mini">GPT-4o Mini</option>
                          </optgroup>
                          <optgroup label="Reasoning (O-Series)">
                            <option value="o3">O3 (Deep Thinking)</option>
                            <option value="o3-pro">O3 Pro</option>
                          </optgroup>
                        </select>
                        <div className="mt-3 p-3 bg-dark-bg border border-dark-border rounded-lg">
                          <p className="text-[10px] leading-relaxed text-dark-muted">
                            {settings.openai_model.startsWith('gpt-5.4') && (
                              <>
                                <span className="font-bold text-accent"> Frontier:</span> Best for complex reasoning, deep research, and long-context documents (~1M tokens).
                              </>
                            )}
                            {settings.openai_model.startsWith('gpt-4.1') && (
                              <>
                                <span className="font-bold text-[#10A37F]"> Balanced:</span> Strong general-purpose performance. Ideal for daily APIs and dashboards.
                              </>
                            )}
                            {settings.openai_model.startsWith('gpt-4o') && (
                              <>
                                <span className="font-bold text-white"> Multimodal:</span> Optimized for real-time vision, audio, and fast text interactions.
                              </>
                            )}
                            {settings.openai_model.startsWith('o3') && (
                              <>
                                <span className="font-bold text-purple-400"> Reasoning:</span> Specialized for deep math, logic-heavy planning, and strategy.
                              </>
                            )}
                          </p>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* xAI Section */}
                  <div className="p-4 bg-dark-bg/50 border border-dark-border rounded-xl space-y-4">
                    <h3 className="text-sm font-bold text-white">xAI (Grok)</h3>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <label className="text-xs font-semibold text-dark-muted">API Key</label>
                        <input
                          type="password"
                          value={settings.xai_api_key}
                          onChange={(e) => setSettings({ ...settings, xai_api_key: e.target.value })}
                          className="w-full px-4 py-2.5 bg-dark-bg border border-dark-border rounded-lg focus:outline-none focus:ring-1 focus:ring-accent transition-all font-mono text-xs"
                          placeholder="xai-..."
                        />
                      </div>
                      <div className="space-y-2">
                        <label className="text-xs font-semibold text-dark-muted">Model Version</label>
                        <select
                          value={settings.xai_model}
                          onChange={(e) => setSettings({ ...settings, xai_model: e.target.value })}
                          className="w-full px-4 py-2.5 bg-dark-bg border border-dark-border rounded-lg focus:outline-none focus:ring-1 focus:ring-accent transition-all text-xs"
                        >
                          <option value="grok-beta">Grok Beta</option>
                        </select>
                      </div>
                    </div>
                  </div>
                </div>
                
                <p className="text-xs text-dark-muted">Keys are stored locally and used for generating narratives and opportunities based on the selected provider.</p>
              </div>
            </div>

            {/* Data Source Configuration */}
            <div className="bg-dark-card border border-dark-border rounded-2xl p-6 shadow-sm">
               <div className="flex items-center gap-3 mb-6">
                <div className="p-2 bg-accent/10 rounded-lg">
                  <Server size={20} className="text-accent" />
                </div>
                <h2 className="text-lg font-bold">Data Source Configuration</h2>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                {/* NSE Source */}
                <div className="p-4 bg-dark-bg/50 border border-dark-border rounded-xl space-y-3">
                  <div className="flex justify-between items-center">
                    <h3 className="text-sm font-bold text-dark-text">NSE (National Stock Exchange)</h3>
                    <span className="text-[10px] px-1.5 py-0.5 bg-accent/10 text-accent rounded uppercase font-bold">Eq</span>
                  </div>
                  <div className="space-y-2">
                    <label className="text-xs font-semibold text-dark-muted">Ingestion Strategy</label>
                    <select
                      value={config.NSE_SOURCE?.value || 'OFFICIAL'}
                      onChange={(e) => updateConfig('NSE_SOURCE', e.target.value)}
                      className="w-full px-3 py-2 bg-dark-bg border border-dark-border rounded-lg focus:outline-none focus:ring-1 focus:ring-accent text-xs"
                    >
                      <option value="OFFICIAL">Official Archives (UDiFF/Legacy)</option>
                      <option value="SAMCO">Samco Mirror (Workaround)</option>
                    </select>
                  </div>
                  <p className="text-[10px] text-dark-muted italic">{config.NSE_SOURCE?.description}</p>
                </div>

                {/* BSE Source */}
                <div className="p-4 bg-dark-bg/50 border border-dark-border rounded-xl space-y-3">
                  <div className="flex justify-between items-center">
                    <h3 className="text-sm font-bold text-dark-text">BSE (Bombay Stock Exchange)</h3>
                    <span className="text-[10px] px-1.5 py-0.5 bg-[#FF9800]/10 text-[#FF9800] rounded uppercase font-bold">Eq</span>
                  </div>
                  <div className="space-y-2">
                    <label className="text-xs font-semibold text-dark-muted">Ingestion Strategy</label>
                    <select
                      value={config.BSE_SOURCE?.value || 'SAMCO'}
                      onChange={(e) => updateConfig('BSE_SOURCE', e.target.value)}
                      className="w-full px-3 py-2 bg-dark-bg border border-dark-border rounded-lg focus:outline-none focus:ring-1 focus:ring-accent text-xs"
                    >
                      <option value="SAMCO">Samco Mirror (Workaround)</option>
                      <option value="OFFICIAL" disabled>Official Archive (Restricted)</option>
                    </select>
                  </div>
                  <p className="text-[10px] text-dark-muted italic">{config.BSE_SOURCE?.description}</p>
                </div>
              </div>

              <div className="mt-6 p-4 bg-blue-500/5 border border-blue-500/10 rounded-xl flex gap-3">
                <AlertCircle size={20} className="text-accent shrink-0" />
                <p className="text-xs text-dark-muted leading-relaxed">
                  Toggle sources if official exchange URLs change or become restricted. <span className="text-accent font-semibold">Samco Mirror</span> is recommended for BSE to bypass Akamai 403 blocks.
                </p>
              </div>
            </div>

             {/* Security Section (Placeholder) */}
             <div className="bg-dark-card border border-dark-border rounded-2xl p-6 shadow-sm">
               <div className="flex items-center gap-3 mb-6">
                <div className="p-2 bg-signal-buy/10 rounded-lg">
                  <ShieldCheck size={20} className="text-signal-buy" />
                </div>
                <h2 className="text-lg font-bold">Access Control</h2>
              </div>
              <p className="text-sm text-dark-muted mb-4">You are currently logged in as administrator. Secure your .env file to rotate passwords.</p>
              <button disabled className="text-sm font-semibold text-accent opacity-50 cursor-not-allowed">Reset Password (Coming Soon)</button>
            </div>
          </div>

          {/* Sidebar info */}
          <div className="space-y-6">
             {/* Data Management Section */}
             <div className="bg-dark-card border border-dark-border rounded-2xl p-6 shadow-sm">
               <div className="flex items-center gap-3 mb-4">
                <Server size={18} className="text-dark-muted" />
                <h3 className="text-sm font-bold">Data Management</h3>
              </div>
              <div className="space-y-3">
                <p className="text-xs text-dark-muted">Fetch missing daily data manually.</p>
                <div className="flex flex-col gap-3">
                  <div className="space-y-1.5">
                    <label className="text-[10px] font-bold text-dark-muted uppercase ml-1">Exchange</label>
                    <select 
                      id="sync-exchange"
                      className="w-full px-3 py-2 bg-dark-bg border border-dark-border rounded-xl focus:outline-none focus:ring-1 focus:ring-accent transition-all text-xs font-bold text-dark-text"
                    >
                      <option value="NSE">NSE (Official Archive)</option>
                      <option value="BSE">BSE (Samco Mirror)</option>
                    </select>
                  </div>
                  
                  <div className="space-y-3">
                    <div className="space-y-1.5">
                      <label className="text-[10px] font-bold text-dark-muted uppercase ml-1">From Date</label>
                      <input 
                        type="date" 
                        id="sync-date-from"
                        className="w-full px-4 py-2 bg-dark-bg border border-dark-border rounded-xl focus:outline-none focus:ring-1 focus:ring-accent transition-all text-xs text-dark-text appearance-none"
                      />
                    </div>
                    <div className="space-y-1.5">
                      <label className="text-[10px] font-bold text-dark-muted uppercase ml-1">To Date <span className="text-[8px] opacity-60">(Optional Range)</span></label>
                      <input 
                        type="date" 
                        id="sync-date-to"
                        className="w-full px-4 py-2 bg-dark-bg border border-dark-border rounded-xl focus:outline-none focus:ring-1 focus:ring-accent transition-all text-xs text-dark-text appearance-none"
                      />
                    </div>
                  </div>

                  <button 
                    onClick={async () => {
                      const fromDate = document.getElementById('sync-date-from').value;
                      const toDate = document.getElementById('sync-date-to').value;
                      const exchangeVal = document.getElementById('sync-exchange').value;
                      
                      if (!fromDate) return toast.error('Please select at least a From date');
                      
                      try {
                        const payload = { 
                          from_date: fromDate,
                          to_date: toDate || null,
                          exchange: exchangeVal
                        };
                        
                        const res = await axios.post(`${API_URL}/api/market/bhavcopy/sync`, payload, {
                          headers: { Authorization: `Bearer ${localStorage.getItem('mm_token')}` }
                        });
                        
                        toast.success(res.data.message);
                        setTimeout(fetchLogs, 2000); 
                      } catch (err) {
                        toast.error(err.response?.data?.detail || 'Failed to queue sync');
                      }
                    }}
                    className="w-full px-4 py-2.5 bg-accent/10 border border-accent/20 hover:bg-accent/20 text-accent rounded-xl transition-all text-xs font-bold mt-2"
                  >
                    Initiate Bulk Sync
                  </button>
                </div>
              </div>
            </div>

            {/* Sync History Table */}
            <div className="bg-dark-card border border-dark-border rounded-2xl p-6 shadow-sm">
               <div className="flex items-center justify-between mb-6">
                <div className="flex items-center gap-3">
                  <div className="p-2 bg-accent/10 rounded-lg">
                    <History size={18} className="text-accent" />
                  </div>
                  <h3 className="text-sm font-bold">Sync History</h3>
                </div>
                <button 
                  onClick={fetchLogs} 
                  disabled={fetchingLogs}
                  className="text-[10px] font-bold text-accent uppercase tracking-wider hover:underline disabled:opacity-50"
                >
                  {fetchingLogs ? 'Refreshing...' : 'Refresh Logs'}
                </button>
              </div>

              <div className="space-y-3">
                {logs.length === 0 ? (
                  <div className="text-center py-8 border border-dashed border-dark-border rounded-xl">
                    <p className="text-xs text-dark-muted">No sync logs found.</p>
                  </div>
                ) : (
                  <div className="overflow-hidden border border-dark-border rounded-xl">
                    <table className="w-full text-left text-xs">
                      <thead className="bg-dark-bg/50 text-dark-muted border-b border-dark-border">
                        <tr>
                          <th className="px-3 py-2 font-semibold">Exch</th>
                          <th className="px-3 py-2 font-semibold">Date</th>
                          <th className="px-3 py-2 font-semibold">Status</th>
                          <th className="px-3 py-2 font-semibold text-right">Records</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-dark-border">
                        {logs.map((log) => (
                          <tr key={log.id} className="hover:bg-white/5 transition-colors">
                            <td className="px-3 py-2.5">
                              <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded ${
                                log.exchange === 'NSE' ? 'bg-accent/10 text-accent' : 'bg-[#FF9800]/10 text-[#FF9800]'
                              }`}>
                                {log.exchange}
                              </span>
                            </td>
                            <td className="px-3 py-2.5">
                              <div className="font-medium text-dark-text">{log.target_date}</div>
                              <div className="text-[10px] text-dark-muted">
                                {new Date(log.completed_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })} • {log.sync_type}
                              </div>
                            </td>
                            <td className="px-3 py-2.5">
                              <span className={`inline-flex items-center gap-1.2 px-1.5 py-0.5 rounded-md font-bold text-[9px] uppercase tracking-tighter ${
                                log.status === 'SUCCESS' ? 'bg-signal-buy/10 text-signal-buy' : 'bg-signal-sell/10 text-signal-sell'
                              }`}>
                                {log.status === 'SUCCESS' ? <CheckCircle2 size={10} /> : <AlertCircle size={10} />}
                                {log.status}
                              </span>
                            </td>
                            <td className="px-3 py-2.5 text-right font-mono text-dark-muted">
                              {log.records_count}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
                <p className="text-[10px] text-dark-muted italic text-center">Showing last 20 events</p>
              </div>
            </div>
             <div className="bg-dark-card border border-dark-border rounded-2xl p-6 shadow-sm">
               <div className="flex items-center gap-3 mb-4">
                <Server size={18} className="text-dark-muted" />
                <h3 className="text-sm font-bold">System Status</h3>
              </div>
              <div className="space-y-3">
                <div className="flex justify-between text-xs">
                  <span className="text-dark-muted">Environment</span>
                  <span className="font-mono text-accent">{settings.env}</span>
                </div>
                <div className="flex justify-between text-xs">
                  <span className="text-dark-muted">App Version</span>
                  <span className="font-mono">{settings.version}</span>
                </div>
                <div className="flex justify-between text-xs">
                  <span className="text-dark-muted">Market Time (IST)</span>
                  <span className="font-mono">Active</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
  );
}
