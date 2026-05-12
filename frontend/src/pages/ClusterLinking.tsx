import { useState, useEffect, useRef, useCallback } from 'react';
import {
  Link2, Play, Square, Trash2, Rocket, RefreshCw, Loader2,
  XCircle, ChevronDown, ChevronUp, Plus, Settings,
  Activity, AlertTriangle, BarChart3,
} from 'lucide-react';
import axios from 'axios';
import { getAccessToken, isAdmin } from '../lib/auth';
import type { Cluster } from '../types';

const authApi = axios.create({ baseURL: '/api' });
authApi.interceptors.request.use((config) => {
  const token = getAccessToken();
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

interface ClusterLinkInfo {
  id: string;
  name: string;
  source_cluster_id: string;
  source_cluster_name: string;
  destination_cluster_id: string;
  destination_cluster_name: string;
  topics_pattern: string;
  sync_consumer_offsets: boolean;
  sync_topic_configs: boolean;
  state: string;
  mm2_port: number;
  deploy_host_id: string | null;
  created_at: string | null;
  updated_at: string | null;
}

interface LinkMetrics {
  link_id: string;
  link_name: string;
  state: string;
  connectors: string[];
  replication_lag: number | null;
  error: string | null;
  mm2_consumer_groups?: string[];
  connector_statuses?: Array<Record<string, unknown>>;
}

const STATE_COLORS: Record<string, string> = {
  created: 'bg-gray-100 text-gray-600',
  running: 'bg-green-100 text-green-700',
  stopped: 'bg-yellow-100 text-yellow-700',
  error: 'bg-red-100 text-red-700',
};

export default function ClusterLinking() {
  const [links, setLinks] = useState<ClusterLinkInfo[]>([]);
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [expandedLink, setExpandedLink] = useState<string | null>(null);
  const [metrics, setMetrics] = useState<Record<string, LinkMetrics>>({});
  const [deployingLink, setDeployingLink] = useState<string | null>(null);
  const [deployTaskId, setDeployTaskId] = useState<string | null>(null);
  const [deployLogs, setDeployLogs] = useState<string[]>([]);
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [error, setError] = useState('');
  const admin = isAdmin();

  // Create form
  const [formName, setFormName] = useState('');
  const [formSource, setFormSource] = useState('');
  const [formDest, setFormDest] = useState('');
  const [formTopics, setFormTopics] = useState('.*');
  const [formSyncOffsets, setFormSyncOffsets] = useState(true);
  const [formSyncConfigs, setFormSyncConfigs] = useState(true);
  const [creating, setCreating] = useState(false);

  const logRef = useRef<HTMLPreElement>(null);

  const fetchLinks = async () => {
    try {
      const { data } = await authApi.get<ClusterLinkInfo[]>('/cluster-linking/links');
      setLinks(data);
    } catch {
      // ignore
    }
  };

  const fetchClusters = async () => {
    try {
      const { data } = await authApi.get<Cluster[]>('/clusters');
      // v1.4.0 #4 — include external clusters too (they have
      // state="connected" / "ok", not "running"). Cluster linking on
      // an external destination is a real, supported flow.
      setClusters(data.filter(c =>
        c.state === 'running' ||
        c.state === 'connected' ||
        c.state === 'ok' ||
        c.kind === 'external'
      ));
    } catch {
      // ignore
    }
  };

  useEffect(() => {
    Promise.all([fetchLinks(), fetchClusters()]).finally(() => setLoading(false));
  }, []);

  // v1.4.0 #3 — poll links + clusters every 10s so the link list
  // reflects status changes (a connector that fails on the source
  // cluster, a destination that goes offline) without the user having
  // to mash the refresh button.
  useEffect(() => {
    const interval = setInterval(() => {
      fetchLinks();
      fetchClusters();
    }, 10000);
    return () => clearInterval(interval);
  }, []);

  // Poll deploy task
  useEffect(() => {
    if (!deployTaskId) return;
    const interval = setInterval(async () => {
      try {
        const { data } = await authApi.get(`/cluster-linking/tasks/${deployTaskId}`);
        setDeployLogs(data.logs || []);
        if (data.status !== 'running') {
          clearInterval(interval);
          setDeployingLink(null);
          setDeployTaskId(null);
          fetchLinks();
        }
      } catch {
        clearInterval(interval);
        setDeployingLink(null);
      }
    }, 2000);
    return () => clearInterval(interval);
  }, [deployTaskId]);

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [deployLogs]);

  const handleCreate = async () => {
    if (!formName || !formSource || !formDest) return;
    setCreating(true);
    setError('');
    try {
      await authApi.post('/cluster-linking/links', {
        name: formName,
        source_cluster_id: formSource,
        destination_cluster_id: formDest,
        topics_pattern: formTopics,
        sync_consumer_offsets: formSyncOffsets,
        sync_topic_configs: formSyncConfigs,
      });
      setShowCreate(false);
      setFormName('');
      setFormTopics('.*');
      fetchLinks();
    } catch (err: unknown) {
      setError((err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || 'Failed to create link');
    } finally {
      setCreating(false);
    }
  };

  const handleDeploy = async (linkId: string) => {
    setDeployingLink(linkId);
    setDeployLogs([]);
    try {
      const { data } = await authApi.post(`/cluster-linking/links/${linkId}/deploy`);
      setDeployTaskId(data.task_id);
    } catch (err: unknown) {
      setError((err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || 'Deploy failed');
      setDeployingLink(null);
    }
  };

  const handleAction = async (linkId: string, action: 'start' | 'stop' | 'delete') => {
    setActionLoading(`${linkId}-${action}`);
    setError('');
    try {
      if (action === 'delete') {
        await authApi.delete(`/cluster-linking/links/${linkId}`);
      } else {
        await authApi.post(`/cluster-linking/links/${linkId}/${action}`);
      }
      fetchLinks();
    } catch (err: unknown) {
      setError((err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || `${action} failed`);
    } finally {
      setActionLoading(null);
    }
  };

  const fetchMetrics = useCallback(async (linkId: string) => {
    try {
      const { data } = await authApi.get<LinkMetrics>(`/cluster-linking/links/${linkId}/metrics`);
      setMetrics(prev => ({ ...prev, [linkId]: data }));
    } catch {
      // ignore
    }
  }, []);

  const toggleExpand = (linkId: string) => {
    if (expandedLink === linkId) {
      setExpandedLink(null);
    } else {
      setExpandedLink(linkId);
      fetchMetrics(linkId);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="w-8 h-8 border-4 border-blue-500/30 border-t-blue-500 rounded-full animate-spin" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <Link2 size={24} />
            Cluster Linking
          </h1>
          <p className="text-gray-500 mt-1">MirrorMaker 2 cross-cluster replication</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => { fetchLinks(); fetchClusters(); }}
            className="flex items-center gap-2 px-3 py-2 text-gray-600 hover:text-gray-900 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
          >
            <RefreshCw size={16} /> Refresh
          </button>
          {admin && (
            <button
              onClick={() => setShowCreate(!showCreate)}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg font-medium transition-colors"
            >
              <Plus size={16} /> New Link
            </button>
          )}
        </div>
      </div>

      {error && (
        <div className="p-3 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm flex items-center justify-between">
          {error}
          <button onClick={() => setError('')} className="text-red-400 hover:text-red-600"><XCircle size={14} /></button>
        </div>
      )}

      {/* Create form */}
      {showCreate && (
        <div className="bg-blue-50 border border-blue-200 rounded-xl p-6">
          <h3 className="font-semibold text-blue-900 mb-4">Create Cluster Link</h3>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Link Name</label>
              <input
                value={formName} onChange={e => setFormName(e.target.value)}
                placeholder="e.g., prod-to-dr"
                className="w-full px-3 py-2 border rounded-lg text-sm"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Topics Pattern</label>
              <input
                value={formTopics} onChange={e => setFormTopics(e.target.value)}
                placeholder=".*"
                className="w-full px-3 py-2 border rounded-lg text-sm font-mono"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Source Cluster</label>
              <select value={formSource} onChange={e => setFormSource(e.target.value)}
                className="w-full px-3 py-2 border rounded-lg text-sm">
                <option value="">Select source...</option>
                {clusters.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Destination Cluster</label>
              <select value={formDest} onChange={e => setFormDest(e.target.value)}
                className="w-full px-3 py-2 border rounded-lg text-sm">
                <option value="">Select destination...</option>
                {clusters.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
              </select>
            </div>
          </div>
          <div className="flex items-center gap-6 mt-4">
            <label className="flex items-center gap-2 text-sm">
              <input type="checkbox" checked={formSyncOffsets} onChange={e => setFormSyncOffsets(e.target.checked)}
                className="rounded border-gray-300" />
              Sync Consumer Offsets
            </label>
            <label className="flex items-center gap-2 text-sm">
              <input type="checkbox" checked={formSyncConfigs} onChange={e => setFormSyncConfigs(e.target.checked)}
                className="rounded border-gray-300" />
              Sync Topic Configs
            </label>
          </div>
          <div className="flex gap-2 mt-4">
            <button onClick={handleCreate} disabled={creating || !formName || !formSource || !formDest}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white rounded-lg font-medium transition-colors">
              {creating ? <Loader2 size={16} className="animate-spin" /> : <Plus size={16} />}
              Create Link
            </button>
            <button onClick={() => setShowCreate(false)}
              className="px-4 py-2 border rounded-lg hover:bg-gray-50 text-sm">Cancel</button>
          </div>
        </div>
      )}

      {/* Links list */}
      {links.length === 0 ? (
        <div className="bg-gray-50 border border-gray-200 rounded-xl p-12 text-center">
          <Link2 size={40} className="mx-auto text-gray-400 mb-4" />
          <h3 className="font-semibold text-gray-700">No Cluster Links</h3>
          <p className="text-sm text-gray-500 mt-2">
            Create a cluster link to set up cross-cluster replication using MirrorMaker 2.
          </p>
        </div>
      ) : (
        <div className="space-y-4">
          {links.map(link => (
            <div key={link.id} className="bg-white border border-gray-200 rounded-xl shadow-sm overflow-hidden">
              {/* Link header */}
              <div className="p-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <Link2 size={18} className="text-blue-500" />
                    <div>
                      <h3 className="font-semibold text-gray-900">{link.name}</h3>
                      <p className="text-xs text-gray-500 mt-0.5">
                        {link.source_cluster_name}
                        <span className="mx-2 text-gray-300">→</span>
                        {link.destination_cluster_name}
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className={`px-2.5 py-1 rounded-full text-xs font-medium ${STATE_COLORS[link.state] || 'bg-gray-100 text-gray-600'}`}>
                      {link.state}
                    </span>
                    {admin && link.state === 'created' && (
                      <button onClick={() => handleDeploy(link.id)}
                        disabled={deployingLink === link.id}
                        className="flex items-center gap-1.5 px-3 py-1.5 bg-green-600 hover:bg-green-700 disabled:bg-green-400 text-white rounded-lg text-xs font-medium transition-colors">
                        {deployingLink === link.id ? <Loader2 size={12} className="animate-spin" /> : <Rocket size={12} />}
                        Deploy
                      </button>
                    )}
                    {admin && link.state === 'running' && (
                      <button onClick={() => handleAction(link.id, 'stop')}
                        disabled={actionLoading === `${link.id}-stop`}
                        className="flex items-center gap-1.5 px-3 py-1.5 bg-yellow-600 hover:bg-yellow-700 text-white rounded-lg text-xs font-medium transition-colors">
                        {actionLoading === `${link.id}-stop` ? <Loader2 size={12} className="animate-spin" /> : <Square size={12} />}
                        Stop
                      </button>
                    )}
                    {admin && link.state === 'stopped' && (
                      <button onClick={() => handleAction(link.id, 'start')}
                        disabled={actionLoading === `${link.id}-start`}
                        className="flex items-center gap-1.5 px-3 py-1.5 bg-green-600 hover:bg-green-700 text-white rounded-lg text-xs font-medium transition-colors">
                        {actionLoading === `${link.id}-start` ? <Loader2 size={12} className="animate-spin" /> : <Play size={12} />}
                        Start
                      </button>
                    )}
                    {admin && (
                      <button onClick={() => {
                        if (confirm(`Delete link "${link.name}"? This will stop MirrorMaker 2 and remove all configuration.`)) {
                          handleAction(link.id, 'delete');
                        }
                      }}
                        disabled={actionLoading === `${link.id}-delete`}
                        className="p-1.5 text-gray-400 hover:text-red-600 rounded transition-colors">
                        {actionLoading === `${link.id}-delete` ? <Loader2 size={14} className="animate-spin" /> : <Trash2 size={14} />}
                      </button>
                    )}
                    <button onClick={() => toggleExpand(link.id)}
                      className="p-1.5 text-gray-400 hover:text-gray-600 rounded transition-colors">
                      {expandedLink === link.id ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                    </button>
                  </div>
                </div>

                {/* Info row */}
                <div className="flex items-center gap-4 mt-3 text-xs text-gray-500">
                  <span>Topics: <code className="font-mono bg-gray-100 px-1 rounded">{link.topics_pattern}</code></span>
                  <span>Offsets: {link.sync_consumer_offsets ? '✓' : '✗'}</span>
                  <span>Configs: {link.sync_topic_configs ? '✓' : '✗'}</span>
                  <span>Port: {link.mm2_port}</span>
                  {link.created_at && <span>Created: {new Date(link.created_at).toLocaleDateString()}</span>}
                </div>
              </div>

              {/* Deploy logs */}
              {deployingLink === link.id && deployLogs.length > 0 && (
                <div className="border-t border-gray-100 bg-gray-900 p-4 max-h-48 overflow-y-auto">
                  <pre ref={logRef} className="text-xs text-gray-300 font-mono whitespace-pre-wrap">
                    {deployLogs.join('\n')}
                  </pre>
                </div>
              )}

              {/* Expanded metrics */}
              {expandedLink === link.id && (
                <div className="border-t border-gray-100 p-4 bg-gray-50">
                  {metrics[link.id] ? (
                    <div className="space-y-3">
                      <div className="grid grid-cols-3 gap-4">
                        <div className="bg-white border rounded-lg p-3 text-center">
                          <Activity size={16} className="mx-auto text-blue-500 mb-1" />
                          <div className="text-lg font-semibold text-gray-900">
                            {metrics[link.id].connectors.length}
                          </div>
                          <div className="text-xs text-gray-500">Connectors</div>
                        </div>
                        <div className="bg-white border rounded-lg p-3 text-center">
                          <BarChart3 size={16} className="mx-auto text-orange-500 mb-1" />
                          <div className="text-lg font-semibold text-gray-900">
                            {metrics[link.id].replication_lag !== null ? metrics[link.id].replication_lag : '—'}
                          </div>
                          <div className="text-xs text-gray-500">Replication Lag</div>
                        </div>
                        <div className="bg-white border rounded-lg p-3 text-center">
                          <Settings size={16} className="mx-auto text-purple-500 mb-1" />
                          <div className="text-lg font-semibold text-gray-900">
                            {metrics[link.id].mm2_consumer_groups?.length || 0}
                          </div>
                          <div className="text-xs text-gray-500">MM2 Consumer Groups</div>
                        </div>
                      </div>
                      {metrics[link.id].error && (
                        <div className="flex items-center gap-2 text-xs text-yellow-700 bg-yellow-50 border border-yellow-200 rounded-lg p-2">
                          <AlertTriangle size={14} />
                          {metrics[link.id].error}
                        </div>
                      )}
                      <button onClick={() => fetchMetrics(link.id)}
                        className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800">
                        <RefreshCw size={12} /> Refresh Metrics
                      </button>
                    </div>
                  ) : (
                    <div className="flex items-center justify-center py-4">
                      <Loader2 size={16} className="animate-spin text-gray-400" />
                      <span className="ml-2 text-sm text-gray-400">Loading metrics...</span>
                    </div>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
