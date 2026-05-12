import { useState, useEffect, useCallback, useMemo } from 'react';
import {
  Settings, Search, RefreshCw, Loader2, AlertCircle, Save, X,
  ChevronDown, ChevronRight, Edit3, RotateCcw, Clock, ArrowRight,
  Info, AlertTriangle, CheckCircle, Filter, Plus, Layers,
} from 'lucide-react';
import axios from 'axios';
import { getAccessToken } from '../../lib/auth';

interface Props {
  clusterId: string;
}

// ── Types ────────────────────────────────────────────────

interface ConfigMetadata {
  key: string;
  type: string;
  description: string;
  dynamic: boolean;
  category: string;
}

interface BrokerConfig {
  broker_id: number;
  host_ip: string;
  service_id: string;
  configs?: Record<string, string>;
  raw?: string;
  error?: string;
}

interface AuditEntry {
  id: string;
  broker_id: number;
  config_key: string;
  old_value: string | null;
  new_value: string;
  changed_by: string;
  change_type: string;
  created_at: string;
}

interface UpdateResult {
  broker_id: number;
  config_key: string;
  old_value: string | null;
  new_value: string;
  requires_restart: boolean;
}

type ConfigTab = 'configuration' | 'audit';

// ── API helper ───────────────────────────────────────────

const authApi = axios.create({ baseURL: '/api' });
authApi.interceptors.request.use((config) => {
  const token = getAccessToken();
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

// ── Category order & colors ──────────────────────────────

const CATEGORY_ORDER = ['Core', 'Log', 'Network', 'Performance', 'Replication'];

const CATEGORY_COLORS: Record<string, string> = {
  Core: 'bg-blue-100 text-blue-800 border-blue-200',
  Log: 'bg-amber-100 text-amber-800 border-amber-200',
  Network: 'bg-purple-100 text-purple-800 border-purple-200',
  Performance: 'bg-emerald-100 text-emerald-800 border-emerald-200',
  Replication: 'bg-rose-100 text-rose-800 border-rose-200',
};

const CATEGORY_HEADER_COLORS: Record<string, string> = {
  Core: 'bg-blue-50 border-blue-200',
  Log: 'bg-amber-50 border-amber-200',
  Network: 'bg-purple-50 border-purple-200',
  Performance: 'bg-emerald-50 border-emerald-200',
  Replication: 'bg-rose-50 border-rose-200',
};

// ── Component ────────────────────────────────────────────

export default function BrokerConfigManager({ clusterId }: Props) {
  const [activeTab, setActiveTab] = useState<ConfigTab>('configuration');

  // Configuration state
  const [brokerConfigs, setBrokerConfigs] = useState<BrokerConfig[]>([]);
  const [metadata, setMetadata] = useState<ConfigMetadata[]>([]);
  const [configsLoading, setConfigsLoading] = useState(false);
  const [configsError, setConfigsError] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [categoryFilter, setCategoryFilter] = useState<string>('');
  const [collapsedCategories, setCollapsedCategories] = useState<Set<string>>(new Set());
  const [selectedBroker, setSelectedBroker] = useState<number | null>(null);

  // Edit state
  const [editingKey, setEditingKey] = useState<string | null>(null);
  const [editValue, setEditValue] = useState('');
  const [editBrokerId, setEditBrokerId] = useState<number | null>(null);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState('');
  const [saveSuccess, setSaveSuccess] = useState<UpdateResult | null>(null);

  // v1.4.0 #10 + #16 — add new key + apply-to-all-brokers modal state
  const [addModalOpen, setAddModalOpen] = useState(false);
  const [newConfigKey, setNewConfigKey] = useState('');
  const [newConfigValue, setNewConfigValue] = useState('');
  const [applyToAll, setApplyToAll] = useState(false);
  const [addSaving, setAddSaving] = useState(false);
  const [addError, setAddError] = useState('');
  const [bulkResult, setBulkResult] = useState<{
    config_key: string; broker_count: number; success_count: number;
    results: Array<{ broker_id: number; ok: boolean; error?: string }>;
  } | null>(null);

  // Audit state
  const [auditLog, setAuditLog] = useState<AuditEntry[]>([]);
  const [auditLoading, setAuditLoading] = useState(false);
  const [auditError, setAuditError] = useState('');

  // Rollback state
  const [rollbackId, setRollbackId] = useState<string | null>(null);
  const [rollbackLoading, setRollbackLoading] = useState(false);
  const [rollbackError, setRollbackError] = useState('');

  // ── Data Fetching ──────────────────────────────────────

  const fetchConfigs = useCallback(async () => {
    setConfigsLoading(true);
    setConfigsError('');
    try {
      const { data } = await authApi.get<BrokerConfig[]>(`/broker-config/clusters/${clusterId}/configs`);
      setBrokerConfigs(data);
      if (data.length > 0 && selectedBroker === null) {
        const firstValid = data.find(b => !b.error);
        if (firstValid) setSelectedBroker(firstValid.broker_id);
        else setSelectedBroker(data[0].broker_id);
      }
    } catch (err: unknown) {
      const axErr = err as { response?: { data?: { detail?: string } } };
      setConfigsError(axErr.response?.data?.detail || 'Failed to load broker configurations');
    } finally {
      setConfigsLoading(false);
    }
  }, [clusterId, selectedBroker]);

  const fetchMetadata = useCallback(async () => {
    try {
      const { data } = await authApi.get<ConfigMetadata[]>('/broker-config/metadata');
      setMetadata(data);
    } catch {
      // Metadata fetch failure is non-critical
    }
  }, []);

  const fetchAuditLog = useCallback(async () => {
    setAuditLoading(true);
    setAuditError('');
    try {
      const { data } = await authApi.get<AuditEntry[]>(`/broker-config/clusters/${clusterId}/audit?limit=100`);
      setAuditLog(data);
    } catch (err: unknown) {
      const axErr = err as { response?: { data?: { detail?: string } } };
      setAuditError(axErr.response?.data?.detail || 'Failed to load audit log');
    } finally {
      setAuditLoading(false);
    }
  }, [clusterId]);

  useEffect(() => {
    fetchMetadata();
  }, [fetchMetadata]);

  useEffect(() => {
    if (activeTab === 'configuration') {
      fetchConfigs();
    } else if (activeTab === 'audit') {
      fetchAuditLog();
    }
  }, [activeTab, fetchConfigs, fetchAuditLog]);

  // ── Metadata map ───────────────────────────────────────

  const metadataMap = useMemo(() => {
    const map: Record<string, ConfigMetadata> = {};
    metadata.forEach(m => { map[m.key] = m; });
    return map;
  }, [metadata]);

  // ── Selected broker data ───────────────────────────────

  const currentBroker = useMemo(() => {
    return brokerConfigs.find(b => b.broker_id === selectedBroker) || null;
  }, [brokerConfigs, selectedBroker]);

  // ── Build config list with metadata ────────────────────

  interface ConfigEntry {
    key: string;
    value: string;
    category: string;
    description: string;
    type: string;
    dynamic: boolean;
    isKnown: boolean;
  }

  const configEntries = useMemo((): ConfigEntry[] => {
    if (!currentBroker?.configs) return [];

    const entries: ConfigEntry[] = [];
    const configs = currentBroker.configs;

    // Add all known Kafka configs (whether present in file or not, show ones present)
    for (const [key, value] of Object.entries(configs)) {
      const meta = metadataMap[key];
      entries.push({
        key,
        value,
        category: meta?.category || 'Other',
        description: meta?.description || '',
        type: meta?.type || 'string',
        dynamic: meta?.dynamic ?? true,
        isKnown: !!meta,
      });
    }

    return entries;
  }, [currentBroker, metadataMap]);

  // ── Filtered & grouped configs ─────────────────────────

  const filteredEntries = useMemo(() => {
    let entries = configEntries;
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      entries = entries.filter(
        e => e.key.toLowerCase().includes(q) ||
             e.value.toLowerCase().includes(q) ||
             e.description.toLowerCase().includes(q)
      );
    }
    if (categoryFilter) {
      entries = entries.filter(e => e.category === categoryFilter);
    }
    return entries;
  }, [configEntries, searchQuery, categoryFilter]);

  const groupedEntries = useMemo(() => {
    const groups: Record<string, ConfigEntry[]> = {};
    for (const entry of filteredEntries) {
      if (!groups[entry.category]) groups[entry.category] = [];
      groups[entry.category].push(entry);
    }
    // Sort groups by defined order
    const sortedGroups: Array<[string, ConfigEntry[]]> = [];
    for (const cat of CATEGORY_ORDER) {
      if (groups[cat]) {
        sortedGroups.push([cat, groups[cat].sort((a, b) => a.key.localeCompare(b.key))]);
        delete groups[cat];
      }
    }
    // Add any remaining categories (e.g., "Other")
    for (const [cat, entries] of Object.entries(groups)) {
      sortedGroups.push([cat, entries.sort((a, b) => a.key.localeCompare(b.key))]);
    }
    return sortedGroups;
  }, [filteredEntries]);

  // ── Available categories for filter ────────────────────

  const availableCategories = useMemo(() => {
    const cats = new Set<string>();
    configEntries.forEach(e => cats.add(e.category));
    return Array.from(cats).sort((a, b) => {
      const aIdx = CATEGORY_ORDER.indexOf(a);
      const bIdx = CATEGORY_ORDER.indexOf(b);
      if (aIdx >= 0 && bIdx >= 0) return aIdx - bIdx;
      if (aIdx >= 0) return -1;
      if (bIdx >= 0) return 1;
      return a.localeCompare(b);
    });
  }, [configEntries]);

  // ── Actions ────────────────────────────────────────────

  const toggleCategory = (category: string) => {
    setCollapsedCategories(prev => {
      const next = new Set(prev);
      if (next.has(category)) next.delete(category);
      else next.add(category);
      return next;
    });
  };

  const startEditing = (key: string, currentValue: string, brokerId: number) => {
    setEditingKey(key);
    setEditValue(currentValue);
    setEditBrokerId(brokerId);
    setSaveError('');
    setSaveSuccess(null);
  };

  const cancelEditing = () => {
    setEditingKey(null);
    setEditValue('');
    setEditBrokerId(null);
    setSaveError('');
  };

  const handleSave = async () => {
    if (!editingKey || editBrokerId === null) return;
    setSaving(true);
    setSaveError('');
    setSaveSuccess(null);
    const savedKey = editingKey;
    const savedValue = editValue;
    const savedBrokerId = editBrokerId;
    try {
      const { data } = await authApi.put<UpdateResult>(
        `/broker-config/clusters/${clusterId}/brokers/${editBrokerId}/config`,
        { config_key: editingKey, config_value: editValue }
      );
      setSaveSuccess(data);
      setEditingKey(null);
      setEditValue('');
      setEditBrokerId(null);

      // Optimistic local update — show the new value immediately so the
      // user doesn't see a stale field even if kafka-configs.sh returns
      // cached data for the next few seconds. fetchConfigs() will reconcile.
      setBrokerConfigs(prev => prev.map(b =>
        b.broker_id === savedBrokerId && b.configs
          ? { ...b, configs: { ...b.configs, [savedKey]: savedValue } }
          : b
      ));

      // Kafka's broker-config alter is async — describe sometimes returns
      // the OLD value if called immediately. Wait briefly, refetch, and
      // retry once if the just-saved key still shows the old value.
      const refetchUntilMatch = async () => {
        await new Promise(r => setTimeout(r, 800));
        await fetchConfigs();
        // If the latest fetch still shows the stale value, do one more
        // round 1.5s later.
        await new Promise(r => setTimeout(r, 1500));
        await fetchConfigs();
      };
      refetchUntilMatch();
    } catch (err: unknown) {
      const axErr = err as { response?: { data?: { detail?: string } } };
      setSaveError(axErr.response?.data?.detail || 'Failed to update configuration');
    } finally {
      setSaving(false);
    }
  };

  // v1.4.0 #10 + #16 — submit a brand-new config key, optionally
  // applying it to every broker in the cluster.
  const handleAddConfig = async () => {
    if (!newConfigKey.trim() || !newConfigValue.trim()) {
      setAddError('Both key and value are required');
      return;
    }
    if (!applyToAll && selectedBroker === null) {
      setAddError('Pick a broker or check "Apply to all brokers"');
      return;
    }
    setAddSaving(true);
    setAddError('');
    setBulkResult(null);
    try {
      if (applyToAll) {
        const { data } = await authApi.post(
          `/broker-config/clusters/${clusterId}/bulk-config`,
          { config_key: newConfigKey, config_value: newConfigValue }
        );
        setBulkResult(data);
        if (data.success_count === data.broker_count) {
          // All brokers succeeded — close modal and refresh
          setAddModalOpen(false);
          setNewConfigKey('');
          setNewConfigValue('');
          setApplyToAll(false);
        }
      } else {
        await authApi.post(
          `/broker-config/clusters/${clusterId}/configs?broker_id=${selectedBroker}`,
          { config_key: newConfigKey, config_value: newConfigValue }
        );
        setAddModalOpen(false);
        setNewConfigKey('');
        setNewConfigValue('');
      }
      // Refresh after a short delay so kafka-configs.sh has time to ack
      await new Promise(r => setTimeout(r, 800));
      await fetchConfigs();
      await fetchAuditLog();
    } catch (err: unknown) {
      const axErr = err as { response?: { data?: { detail?: string } } };
      setAddError(axErr.response?.data?.detail || 'Failed to add configuration');
    } finally {
      setAddSaving(false);
    }
  };

  const handleRollback = async (auditId: string) => {
    setRollbackLoading(true);
    setRollbackError('');
    try {
      await authApi.post(`/broker-config/audit/${auditId}/rollback`);
      setRollbackId(null);
      fetchAuditLog();
      fetchConfigs();
    } catch (err: unknown) {
      const axErr = err as { response?: { data?: { detail?: string } } };
      setRollbackError(axErr.response?.data?.detail || 'Failed to rollback configuration');
    } finally {
      setRollbackLoading(false);
    }
  };

  // ── Tab bar ────────────────────────────────────────────

  const tabs: Array<{ id: ConfigTab; label: string; icon: React.ReactNode }> = [
    { id: 'configuration', label: 'Configuration', icon: <Settings size={14} /> },
    { id: 'audit', label: 'Audit Log', icon: <Clock size={14} /> },
  ];

  return (
    <div className="space-y-4">
      {/* Sub-tab bar */}
      <div className="flex border-b border-gray-200">
        {tabs.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
              activeTab === tab.id
                ? 'border-blue-600 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700'
            }`}
          >
            {tab.icon} {tab.label}
          </button>
        ))}
      </div>

      {/* ══════════ CONFIGURATION TAB ══════════ */}
      {activeTab === 'configuration' && (
        <div>
          {/* Success banner */}
          {saveSuccess && (
            <div className="bg-green-50 border border-green-200 rounded-lg px-4 py-3 mb-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2 text-sm text-green-800">
                  <CheckCircle size={16} />
                  <span>
                    <span className="font-medium">{saveSuccess.config_key}</span> updated to{' '}
                    <code className="bg-green-100 px-1.5 py-0.5 rounded text-xs font-mono">{saveSuccess.new_value}</code>
                    {' '}on broker {saveSuccess.broker_id}
                  </span>
                  {saveSuccess.requires_restart && (
                    <span className="inline-flex items-center gap-1 px-2 py-0.5 bg-amber-100 text-amber-800 rounded text-xs font-medium">
                      <AlertTriangle size={12} /> Restart required
                    </span>
                  )}
                </div>
                <button onClick={() => setSaveSuccess(null)} className="text-green-600 hover:text-green-800 text-lg leading-none">
                  &times;
                </button>
              </div>
            </div>
          )}

          {/* Error banner */}
          {configsError && (
            <div className="flex items-center gap-2 bg-red-50 border border-red-200 rounded-lg px-4 py-3 mb-4 text-sm text-red-700">
              <AlertCircle size={16} />
              <span>{configsError}</span>
              <button onClick={fetchConfigs} className="ml-auto text-xs underline">Retry</button>
            </div>
          )}

          {/* Save error banner */}
          {saveError && (
            <div className="flex items-center gap-2 bg-red-50 border border-red-200 rounded-lg px-4 py-3 mb-4 text-sm text-red-700">
              <AlertCircle size={16} />
              <span>{saveError}</span>
              <button onClick={() => setSaveError('')} className="ml-auto text-lg leading-none">&times;</button>
            </div>
          )}

          {/* Toolbar */}
          <div className="flex items-center gap-3 mb-4 flex-wrap">
            {/* Broker selector */}
            <div className="flex items-center gap-2">
              <label className="text-xs text-gray-500 font-medium uppercase tracking-wide">Broker</label>
              <div className="flex rounded-lg border border-gray-200 overflow-hidden">
                {brokerConfigs.map(b => (
                  <button
                    key={b.broker_id}
                    onClick={() => setSelectedBroker(b.broker_id)}
                    className={`px-3 py-1.5 text-sm font-medium transition-colors ${
                      selectedBroker === b.broker_id
                        ? 'bg-blue-600 text-white'
                        : b.error
                        ? 'bg-red-50 text-red-600 hover:bg-red-100'
                        : 'bg-gray-50 text-gray-600 hover:bg-gray-100'
                    }`}
                    title={b.error ? `Error: ${b.error}` : `Broker ${b.broker_id} (${b.host_ip})`}
                  >
                    #{b.broker_id}
                    {b.error && <AlertCircle size={10} className="inline ml-1" />}
                  </button>
                ))}
              </div>
            </div>

            {/* Search */}
            <div className="relative flex-1 min-w-[200px] max-w-md">
              <Search size={14} className="absolute left-3 top-2.5 text-gray-400" />
              <input
                type="text"
                value={searchQuery}
                onChange={e => setSearchQuery(e.target.value)}
                placeholder="Search configs..."
                className="w-full pl-9 pr-3 py-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
              {searchQuery && (
                <button
                  onClick={() => setSearchQuery('')}
                  className="absolute right-2.5 top-2.5 text-gray-400 hover:text-gray-600"
                >
                  <X size={14} />
                </button>
              )}
            </div>

            {/* Category filter */}
            <div className="relative">
              <Filter size={14} className="absolute left-3 top-2.5 text-gray-400" />
              <select
                value={categoryFilter}
                onChange={e => setCategoryFilter(e.target.value)}
                className="pl-9 pr-8 py-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 appearance-none bg-white"
              >
                <option value="">All Categories</option>
                {availableCategories.map(cat => (
                  <option key={cat} value={cat}>{cat}</option>
                ))}
              </select>
            </div>

            {/* Refresh */}
            <button
              onClick={fetchConfigs}
              disabled={configsLoading}
              className="flex items-center gap-1.5 px-3 py-2 border border-gray-200 rounded-lg text-sm hover:bg-gray-50 transition-colors"
            >
              <RefreshCw size={14} className={configsLoading ? 'animate-spin' : ''} /> Refresh
            </button>

            {/* Add Config (v1.4.0 #10 + #16) */}
            <button
              onClick={() => {
                setAddModalOpen(true);
                setAddError('');
                setBulkResult(null);
              }}
              className="flex items-center gap-1.5 px-3 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700 transition-colors"
            >
              <Plus size={14} /> Add Config
            </button>
          </div>

          {/* Broker error display */}
          {currentBroker?.error && (
            <div className="bg-red-50 border border-red-200 rounded-xl p-4 mb-4">
              <div className="flex items-center gap-2 text-sm text-red-700">
                <AlertCircle size={16} />
                <span className="font-medium">Broker {currentBroker.broker_id} ({currentBroker.host_ip}):</span>
                <span>{currentBroker.error}</span>
              </div>
            </div>
          )}

          {/* Loading */}
          {configsLoading && brokerConfigs.length === 0 ? (
            <div className="flex items-center justify-center gap-2 py-16 text-gray-400 text-sm">
              <Loader2 size={16} className="animate-spin" /> Loading broker configurations...
            </div>
          ) : brokerConfigs.length === 0 ? (
            <div className="text-center py-16 text-gray-400 text-sm">
              <Settings size={32} className="mx-auto mb-2 opacity-50" />
              No brokers found in this cluster.
            </div>
          ) : currentBroker && !currentBroker.error ? (
            /* Config groups */
            <div className="space-y-3">
              {groupedEntries.length === 0 ? (
                <div className="text-center py-12 text-gray-400 text-sm">
                  <Search size={24} className="mx-auto mb-2 opacity-50" />
                  No configs match your search.
                </div>
              ) : (
                groupedEntries.map(([category, entries]) => {
                  const isCollapsed = collapsedCategories.has(category);
                  const headerColor = CATEGORY_HEADER_COLORS[category] || 'bg-gray-50 border-gray-200';
                  const badgeColor = CATEGORY_COLORS[category] || 'bg-gray-100 text-gray-800 border-gray-200';

                  return (
                    <div key={category} className="border border-gray-200 rounded-xl overflow-hidden">
                      {/* Category header */}
                      <button
                        onClick={() => toggleCategory(category)}
                        className={`w-full flex items-center justify-between px-4 py-3 text-left transition-colors hover:opacity-90 ${headerColor} border-b`}
                      >
                        <div className="flex items-center gap-2">
                          {isCollapsed ? <ChevronRight size={16} /> : <ChevronDown size={16} />}
                          <span className={`px-2.5 py-0.5 rounded-full text-xs font-semibold border ${badgeColor}`}>
                            {category}
                          </span>
                          <span className="text-xs text-gray-500">
                            {entries.length} {entries.length === 1 ? 'config' : 'configs'}
                          </span>
                        </div>
                      </button>

                      {/* Config rows */}
                      {!isCollapsed && (
                        <div className="divide-y divide-gray-100">
                          {entries.map(entry => {
                            const isEditing = editingKey === entry.key && editBrokerId === selectedBroker;

                            return (
                              <div
                                key={entry.key}
                                className={`px-4 py-3 transition-colors ${
                                  isEditing ? 'bg-blue-50' : 'hover:bg-gray-50'
                                }`}
                              >
                                <div className="flex items-start justify-between gap-4">
                                  {/* Key & description */}
                                  <div className="flex-1 min-w-0">
                                    <div className="flex items-center gap-2 mb-0.5">
                                      <code className="text-sm font-mono font-medium text-gray-900 break-all">
                                        {entry.key}
                                      </code>
                                      {entry.isKnown && (
                                        entry.dynamic ? (
                                          <span className="inline-flex items-center px-1.5 py-0.5 bg-green-100 text-green-700 rounded text-[10px] font-medium whitespace-nowrap">
                                            Dynamic
                                          </span>
                                        ) : (
                                          <span className="inline-flex items-center gap-0.5 px-1.5 py-0.5 bg-amber-100 text-amber-700 rounded text-[10px] font-medium whitespace-nowrap">
                                            <AlertTriangle size={9} /> Restart
                                          </span>
                                        )
                                      )}
                                      {entry.isKnown && (
                                        <span className="text-[10px] text-gray-400 font-mono">
                                          ({entry.type})
                                        </span>
                                      )}
                                    </div>
                                    {entry.description && (
                                      <p className="text-xs text-gray-500 flex items-center gap-1">
                                        <Info size={10} className="shrink-0 text-gray-400" />
                                        {entry.description}
                                      </p>
                                    )}
                                  </div>

                                  {/* Value & actions */}
                                  <div className="flex items-center gap-2 shrink-0">
                                    {isEditing ? (
                                      <div className="flex items-center gap-2">
                                        <input
                                          type="text"
                                          value={editValue}
                                          onChange={e => setEditValue(e.target.value)}
                                          className="w-48 px-2.5 py-1.5 border border-blue-300 rounded-lg text-sm font-mono focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                                          autoFocus
                                          onKeyDown={e => {
                                            if (e.key === 'Enter') handleSave();
                                            if (e.key === 'Escape') cancelEditing();
                                          }}
                                        />
                                        <button
                                          onClick={handleSave}
                                          disabled={saving || editValue === entry.value}
                                          className="flex items-center gap-1 px-2.5 py-1.5 bg-blue-600 text-white rounded-lg text-xs font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
                                        >
                                          {saving ? <Loader2 size={12} className="animate-spin" /> : <Save size={12} />}
                                          Save
                                        </button>
                                        <button
                                          onClick={cancelEditing}
                                          className="flex items-center gap-1 px-2.5 py-1.5 border border-gray-300 rounded-lg text-xs font-medium hover:bg-gray-50 transition-colors"
                                        >
                                          <X size={12} /> Cancel
                                        </button>
                                      </div>
                                    ) : (
                                      <>
                                        <code className="text-sm font-mono text-gray-700 bg-gray-100 px-2.5 py-1 rounded max-w-[300px] truncate block">
                                          {entry.value}
                                        </code>
                                        <button
                                          onClick={() => startEditing(entry.key, entry.value, selectedBroker!)}
                                          className="p-1.5 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded transition-colors"
                                          title="Edit value"
                                        >
                                          <Edit3 size={14} />
                                        </button>
                                      </>
                                    )}
                                  </div>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      )}
                    </div>
                  );
                })
              )}

              {/* Summary footer */}
              <div className="flex items-center justify-between px-1 pt-2 text-xs text-gray-400">
                <span>
                  Showing {filteredEntries.length} of {configEntries.length} configurations
                  {currentBroker && ` on broker #${currentBroker.broker_id} (${currentBroker.host_ip})`}
                </span>
                <span>
                  {configEntries.filter(e => e.isKnown && !e.dynamic).length} require restart after change
                </span>
              </div>
            </div>
          ) : null}
        </div>
      )}

      {/* ══════════ AUDIT LOG TAB ══════════ */}
      {activeTab === 'audit' && (
        <div>
          {/* Rollback confirmation modal */}
          {rollbackId && (
            <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
              <div className="bg-white rounded-xl shadow-xl p-6 w-full max-w-md mx-4">
                <h3 className="text-lg font-semibold text-gray-900 mb-2">Confirm Rollback</h3>
                {(() => {
                  const entry = auditLog.find(e => e.id === rollbackId);
                  if (!entry) return null;
                  return (
                    <div className="space-y-3">
                      <p className="text-sm text-gray-600">
                        Are you sure you want to rollback the following configuration change?
                      </p>
                      <div className="bg-gray-50 rounded-lg p-3 space-y-2 text-sm">
                        <div className="flex justify-between">
                          <span className="text-gray-500">Config key:</span>
                          <code className="font-mono text-gray-900">{entry.config_key}</code>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-gray-500">Broker:</span>
                          <span className="text-gray-900">#{entry.broker_id}</span>
                        </div>
                        <div className="flex items-center justify-between">
                          <span className="text-gray-500">Revert to:</span>
                          <code className="font-mono bg-green-100 text-green-800 px-2 py-0.5 rounded">
                            {entry.old_value ?? '(none)'}
                          </code>
                        </div>
                        <div className="flex items-center justify-between">
                          <span className="text-gray-500">Current:</span>
                          <code className="font-mono bg-red-100 text-red-800 px-2 py-0.5 rounded">
                            {entry.new_value}
                          </code>
                        </div>
                      </div>
                      {rollbackError && (
                        <div className="flex items-center gap-2 text-sm text-red-700 bg-red-50 rounded-lg px-3 py-2">
                          <AlertCircle size={14} />
                          {rollbackError}
                        </div>
                      )}
                    </div>
                  );
                })()}
                <div className="flex justify-end gap-2 mt-4">
                  <button
                    onClick={() => { setRollbackId(null); setRollbackError(''); }}
                    className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium hover:bg-gray-50 transition-colors"
                  >
                    Cancel
                  </button>
                  <button
                    onClick={() => handleRollback(rollbackId)}
                    disabled={rollbackLoading}
                    className="flex items-center gap-1.5 px-4 py-2 bg-amber-600 text-white rounded-lg text-sm font-medium hover:bg-amber-700 disabled:opacity-50 transition-colors"
                  >
                    {rollbackLoading ? <Loader2 size={14} className="animate-spin" /> : <RotateCcw size={14} />}
                    Rollback
                  </button>
                </div>
              </div>
            </div>
          )}

          {/* Audit error */}
          {auditError && (
            <div className="flex items-center gap-2 bg-red-50 border border-red-200 rounded-lg px-4 py-3 mb-4 text-sm text-red-700">
              <AlertCircle size={16} />
              <span>{auditError}</span>
              <button onClick={fetchAuditLog} className="ml-auto text-xs underline">Retry</button>
            </div>
          )}

          {/* Actions bar */}
          <div className="flex items-center gap-2 mb-4">
            <button
              onClick={fetchAuditLog}
              disabled={auditLoading}
              className="flex items-center gap-1.5 px-3 py-2 border border-gray-200 rounded-lg text-sm hover:bg-gray-50 transition-colors"
            >
              <RefreshCw size={14} className={auditLoading ? 'animate-spin' : ''} /> Refresh
            </button>
            <span className="text-xs text-gray-400 ml-2">
              {auditLog.length} {auditLog.length === 1 ? 'entry' : 'entries'}
            </span>
          </div>

          {/* Audit table */}
          {auditLoading && auditLog.length === 0 ? (
            <div className="flex items-center justify-center gap-2 py-16 text-gray-400 text-sm">
              <Loader2 size={16} className="animate-spin" /> Loading audit log...
            </div>
          ) : auditLog.length === 0 ? (
            <div className="text-center py-16 text-gray-400 text-sm">
              <Clock size={32} className="mx-auto mb-2 opacity-50" />
              No configuration changes have been recorded yet.
            </div>
          ) : (
            <div className="border border-gray-200 rounded-xl overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-gray-50 text-left text-xs text-gray-500 uppercase tracking-wide">
                    <th className="px-4 py-3 font-medium">Timestamp</th>
                    <th className="px-4 py-3 font-medium">Broker</th>
                    <th className="px-4 py-3 font-medium">Config Key</th>
                    <th className="px-4 py-3 font-medium">Change</th>
                    <th className="px-4 py-3 font-medium">Changed By</th>
                    <th className="px-4 py-3 font-medium">Type</th>
                    <th className="px-4 py-3 font-medium text-right">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {auditLog.map(entry => (
                    <tr key={entry.id} className="hover:bg-gray-50 transition-colors">
                      <td className="px-4 py-3 text-xs text-gray-500 whitespace-nowrap">
                        {new Date(entry.created_at).toLocaleString()}
                      </td>
                      <td className="px-4 py-3">
                        <span className="px-2 py-0.5 bg-gray-100 text-gray-700 rounded text-xs font-mono">
                          #{entry.broker_id}
                        </span>
                      </td>
                      <td className="px-4 py-3">
                        <code className="text-xs font-mono font-medium text-gray-900">
                          {entry.config_key}
                        </code>
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-1.5 text-xs">
                          <code className={`px-1.5 py-0.5 rounded font-mono ${
                            entry.old_value !== null
                              ? 'bg-red-50 text-red-700 line-through'
                              : 'bg-gray-50 text-gray-400 italic'
                          }`}>
                            {entry.old_value ?? '(new)'}
                          </code>
                          <ArrowRight size={12} className="text-gray-400 shrink-0" />
                          <code className="px-1.5 py-0.5 rounded font-mono bg-green-50 text-green-700">
                            {entry.new_value}
                          </code>
                        </div>
                      </td>
                      <td className="px-4 py-3 text-xs text-gray-600">
                        {entry.changed_by}
                      </td>
                      <td className="px-4 py-3">
                        <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                          entry.change_type === 'rollback'
                            ? 'bg-amber-100 text-amber-800'
                            : 'bg-blue-100 text-blue-800'
                        }`}>
                          {entry.change_type}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right">
                        {entry.old_value !== null && (
                          <button
                            onClick={() => { setRollbackId(entry.id); setRollbackError(''); }}
                            className="inline-flex items-center gap-1 px-2 py-1 text-amber-700 hover:bg-amber-50 rounded text-xs font-medium transition-colors"
                            title="Rollback this change"
                          >
                            <RotateCcw size={12} /> Rollback
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* v1.4.0 #10 + #16 — Add Config modal */}
      {addModalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4" onClick={() => !addSaving && setAddModalOpen(false)}>
          <div className="bg-white rounded-xl shadow-xl max-w-lg w-full p-6" onClick={e => e.stopPropagation()}>
            <div className="flex items-start justify-between mb-4">
              <div>
                <h3 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
                  <Plus size={18} /> Add broker config
                </h3>
                <p className="text-xs text-gray-500 mt-0.5">
                  Add a new key (or overwrite an existing one) on a single broker or all brokers in this cluster.
                </p>
              </div>
              <button onClick={() => !addSaving && setAddModalOpen(false)} className="text-gray-400 hover:text-gray-600">
                <X size={18} />
              </button>
            </div>

            <div className="space-y-3">
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">Config key</label>
                <input
                  type="text"
                  value={newConfigKey}
                  onChange={e => setNewConfigKey(e.target.value)}
                  placeholder="e.g. log.retention.hours"
                  className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm font-mono focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  autoFocus
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">Value</label>
                <input
                  type="text"
                  value={newConfigValue}
                  onChange={e => setNewConfigValue(e.target.value)}
                  placeholder="e.g. 168"
                  className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm font-mono focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
              <label className="flex items-start gap-2 p-3 bg-blue-50 border border-blue-200 rounded-lg cursor-pointer">
                <input
                  type="checkbox"
                  checked={applyToAll}
                  onChange={e => setApplyToAll(e.target.checked)}
                  className="mt-0.5"
                />
                <div className="text-sm">
                  <div className="font-medium text-blue-900 flex items-center gap-1.5">
                    <Layers size={14} /> Apply to all brokers
                  </div>
                  <div className="text-xs text-blue-700 mt-0.5">
                    Push this same value to every broker in the cluster ({brokerConfigs.length} broker{brokerConfigs.length === 1 ? '' : 's'}).
                  </div>
                </div>
              </label>
              {!applyToAll && (
                <div className="text-xs text-gray-500">
                  Will be applied to <span className="font-mono font-medium">broker #{selectedBroker ?? '—'}</span>.
                </div>
              )}

              {addError && (
                <div className="flex items-center gap-2 bg-red-50 border border-red-200 rounded-lg px-3 py-2 text-sm text-red-700">
                  <AlertCircle size={14} /> {addError}
                </div>
              )}

              {bulkResult && (
                <div className={`rounded-lg px-3 py-2 text-sm border ${
                  bulkResult.success_count === bulkResult.broker_count
                    ? 'bg-green-50 border-green-200 text-green-800'
                    : 'bg-amber-50 border-amber-200 text-amber-900'
                }`}>
                  <div className="font-medium mb-1">
                    {bulkResult.success_count}/{bulkResult.broker_count} brokers updated
                  </div>
                  <ul className="text-xs space-y-0.5">
                    {bulkResult.results.map(r => (
                      <li key={r.broker_id} className="flex items-center gap-2">
                        {r.ok
                          ? <CheckCircle size={11} className="text-green-600" />
                          : <AlertCircle size={11} className="text-red-600" />}
                        <span className="font-mono">#{r.broker_id}</span>
                        {!r.ok && <span className="text-red-700">{r.error}</span>}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>

            <div className="flex justify-end gap-2 mt-5">
              <button
                onClick={() => !addSaving && setAddModalOpen(false)}
                disabled={addSaving}
                className="px-4 py-2 text-sm border border-gray-200 rounded-lg hover:bg-gray-50"
              >
                Cancel
              </button>
              <button
                onClick={handleAddConfig}
                disabled={addSaving}
                className="px-4 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 flex items-center gap-1.5"
              >
                {addSaving && <Loader2 size={14} className="animate-spin" />}
                {applyToAll ? 'Apply to all brokers' : 'Add to broker'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
