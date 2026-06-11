import { useState, useEffect, useCallback } from 'react';
import { BarChart3, Cpu, HardDrive, Activity, RefreshCw, Server, Wifi, Database, Clock, MemoryStick } from 'lucide-react';
import { XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Area, AreaChart } from 'recharts';
import { getClusters, getClusterMetrics } from '../lib/api';
import type { Cluster } from '../types';

interface NodeMetrics {
  host_id: string;
  hostname: string;
  ip_address: string;
  role: string;
  node_id: number;
  status: string;
  system: {
    uptime?: string;
    cpu_cores?: number;
    cpu_usage_pct?: number;
    load_1m?: number;
    load_5m?: number;
    load_15m?: number;
    memory_total_mb?: number;
    memory_used_mb?: number;
    memory_available_mb?: number;
    memory_usage_pct?: number;
    error?: string;
  };
  kafka: {
    status?: string;
    pid?: number;
    uptime?: string;
    uptime_seconds?: number;
    memory_rss_mb?: number;
    data_size_mb?: number;
    log_size_mb?: number;
    topics?: number;
    partitions?: number;
    open_fds?: number;
    connections?: number;
    error?: string;
  };
  disk: {
    root?: { total_mb: number; used_mb: number; available_mb: number; usage_pct: number };
    data?: { total_mb: number; used_mb: number; available_mb: number; usage_pct: number };
    error?: string;
  };
}

interface ClusterMetrics {
  cluster_id: string;
  cluster_name: string;
  nodes: NodeMetrics[];
}

interface TimePoint {
  time: string;
  timestamp: number;
  [key: string]: string | number;
}

const MAX_HISTORY = 30; // 30 data points = 5 minutes at 10s intervals

function MetricCard({ icon: Icon, label, value, sub }: { icon: typeof Cpu; label: string; value: string | number; sub?: string }) {
  return (
    <div className="bg-gray-50 border border-gray-100 rounded-lg p-3">
      <div className="flex items-center gap-1.5 mb-1">
        <Icon size={13} className="text-gray-400" />
        <span className="text-[11px] font-medium text-gray-500 uppercase tracking-wide">{label}</span>
      </div>
      <p className="text-lg font-bold text-gray-800">{value}</p>
      {sub && <p className="text-[11px] text-gray-400 mt-0.5">{sub}</p>}
    </div>
  );
}

function LiveChart({ data, dataKey, color, label, unit = '%', max, id }: {
  data: TimePoint[];
  dataKey: string;
  color: string;
  label: string;
  unit?: string;
  max?: number;
  id?: string;
}) {
  const gradId = `grad-${id || dataKey}-${dataKey}`;
  const latest = data.length > 0 ? (data[data.length - 1][dataKey] as number) ?? 0 : 0;
  const statusColor = latest > 80 ? 'text-red-600' : latest > 60 ? 'text-yellow-600' : 'text-green-600';

  return (
    <div className="bg-white border border-gray-200 rounded-xl p-4">
      <div className="flex items-center justify-between mb-2">
        <span className="text-sm font-medium text-gray-700">{label}</span>
        <span className={`text-lg font-bold ${statusColor}`}>
          {typeof latest === 'number' ? latest.toFixed(1) : latest}{unit}
        </span>
      </div>
      <div style={{ height: 120 }}>
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 5, right: 5, left: -20, bottom: 0 }}>
            <defs>
              <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor={color} stopOpacity={0.3} />
                <stop offset="95%" stopColor={color} stopOpacity={0.05} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis dataKey="time" tick={{ fontSize: 10 }} stroke="#ccc" interval="preserveStartEnd" />
            <YAxis domain={[0, max || 'auto']} tick={{ fontSize: 10 }} stroke="#ccc" />
            <Tooltip
              contentStyle={{ fontSize: 12, borderRadius: 8, border: '1px solid #e5e7eb' }}
              formatter={(value: unknown) => [`${Number(value).toFixed(1)}${unit}`, label]}
              labelStyle={{ fontSize: 11, color: '#6b7280' }}
            />
            <Area
              type="monotone"
              dataKey={dataKey}
              stroke={color}
              strokeWidth={2}
              fill={`url(#${gradId})`}
              isAnimationActive={false}
              dot={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

export default function Monitoring() {
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [selectedCluster, setSelectedCluster] = useState<string>('');
  const [metrics, setMetrics] = useState<ClusterMetrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true); // ON by default
  const [error, setError] = useState<string | null>(null);
  // Time-series history per node: { hostId: TimePoint[] }
  const [history, setHistory] = useState<Record<string, TimePoint[]>>({});

  useEffect(() => {
    getClusters().then((data: Cluster[]) => {
      const running = data.filter((c: Cluster) => c.state === 'running' || c.state === 'connected');
      setClusters(running);
      if (running.length > 0) setSelectedCluster(running[0].id);
    }).finally(() => setLoading(false));
  }, []);

  const fetchMetrics = useCallback(async (silent = false) => {
    if (!selectedCluster) return;
    if (!silent) setRefreshing(true);
    setError(null);
    try {
      const data = await getClusterMetrics(selectedCluster);
      setMetrics(data);

      // Append to time-series history (new object to trigger re-render)
      const now = new Date();
      const timeLabel = now.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
      setHistory(prev => {
        const next = { ...prev };
        for (const node of data.nodes) {
          const key = node.host_id;
          const existing = next[key] ? [...next[key]] : [];
          existing.push({
            time: timeLabel,
            timestamp: now.getTime(),
            cpu: node.system.cpu_usage_pct ?? 0,
            memory: node.system.memory_usage_pct ?? 0,
            disk: node.disk.data?.usage_pct ?? node.disk.root?.usage_pct ?? 0,
            connections: node.kafka.connections ?? 0,
            kafkaMemory: node.kafka.memory_rss_mb ?? 0,
            load1m: node.system.load_1m ?? 0,
          });
          // Keep only last MAX_HISTORY points
          next[key] = existing.length > MAX_HISTORY ? existing.slice(-MAX_HISTORY) : existing;
        }
        return next;
      });
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      if (!silent) setError(msg || 'Failed to fetch metrics. Check cluster connectivity.');
    }
    if (!silent) setRefreshing(false);
  }, [selectedCluster]);

  useEffect(() => {
    if (selectedCluster) {
      setHistory({}); // Reset history on cluster change
      fetchMetrics();
    }
  }, [selectedCluster, fetchMetrics]);

  // Auto-refresh every 10s for real-time feel
  useEffect(() => {
    if (!autoRefresh || !selectedCluster) return;
    const interval = setInterval(() => {
      if (!document.hidden) fetchMetrics(true);
    }, 10000);
    return () => clearInterval(interval);
  }, [autoRefresh, selectedCluster, fetchMetrics]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="w-8 h-8 border-4 border-blue-500/30 border-t-blue-500 rounded-full animate-spin" />
      </div>
    );
  }

  if (clusters.length === 0) {
    return (
      <div className="space-y-6">
        <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
          <BarChart3 size={24} /> Monitoring
        </h1>
        <div className="bg-gray-50 border border-gray-200 rounded-xl p-12 text-center">
          <Server size={40} className="mx-auto text-gray-300 mb-4" />
          <h3 className="text-lg font-semibold text-gray-600">No Active Clusters</h3>
          <p className="text-gray-400 mt-2">Deploy or connect a Kafka cluster first, then monitoring metrics will appear here automatically.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <BarChart3 size={24} /> Monitoring
          </h1>
          <p className="text-gray-500 mt-1">Real-time Kafka & system metrics</p>
        </div>
        <div className="flex items-center gap-3">
          {clusters.length > 1 && (
            <select
              value={selectedCluster}
              onChange={e => setSelectedCluster(e.target.value)}
              className="border border-gray-300 rounded-lg px-3 py-2 text-sm"
            >
              {clusters.map(c => (
                <option key={c.id} value={c.id}>{c.name}</option>
              ))}
            </select>
          )}
          <label className="flex items-center gap-2 text-sm text-gray-600 cursor-pointer select-none">
            <input
              type="checkbox"
              checked={autoRefresh}
              onChange={e => setAutoRefresh(e.target.checked)}
              className="rounded border-gray-300"
            />
            <span className={autoRefresh ? 'text-green-600 font-medium' : ''}>
              Live {autoRefresh ? '● 10s' : '(off)'}
            </span>
          </label>
          <button
            onClick={() => fetchMetrics()}
            disabled={refreshing}
            className="flex items-center gap-2 px-3 py-2 text-gray-600 hover:text-gray-900 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
          >
            <RefreshCw size={16} className={refreshing ? 'animate-spin' : ''} />
            Refresh
          </button>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-sm text-red-700">
          {error}
        </div>
      )}

      {/* Nodes */}
      {metrics?.nodes.map(node => {
        const nodeHistory = history[node.host_id] || [];

        return (
          <div key={node.host_id} className="bg-white border border-gray-200 rounded-xl shadow-sm overflow-hidden">
            {/* Node Header */}
            <div className="px-6 py-4 border-b border-gray-100 bg-gray-50 flex items-center justify-between">
              <div className="flex items-center gap-3">
                <Server size={20} className="text-gray-500" />
                <div>
                  <h3 className="font-semibold text-gray-900">{node.hostname}</h3>
                  <p className="text-xs text-gray-500">{node.ip_address} · {node.role} · Node {node.node_id}</p>
                </div>
              </div>
              <span className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium ${
                node.kafka.status === 'active' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
              }`}>
                <span className={`w-2 h-2 rounded-full ${node.kafka.status === 'active' ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`} />
                Kafka {node.kafka.status === 'active' ? 'Running' : node.kafka.status || 'Unknown'}
              </span>
            </div>

            <div className="p-6 space-y-6">
              {/* Real-time Charts */}
              <div>
                <h4 className="text-sm font-semibold text-gray-700 mb-3 flex items-center gap-2">
                  <Activity size={14} /> Real-time Performance
                  {autoRefresh && <span className="text-[10px] text-green-600 bg-green-50 px-2 py-0.5 rounded-full font-normal">LIVE</span>}
                </h4>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <LiveChart data={nodeHistory} dataKey="cpu" color="#3b82f6" label="CPU Usage" max={100} id={node.host_id} />
                  <LiveChart data={nodeHistory} dataKey="memory" color="#10b981" label="Memory Usage" max={100} id={node.host_id} />
                  <LiveChart data={nodeHistory} dataKey="connections" color="#8b5cf6" label="Kafka Connections" unit="" id={node.host_id} />
                </div>
              </div>

              {/* Kafka Metrics */}
              <div>
                <h4 className="text-sm font-semibold text-gray-700 mb-3 flex items-center gap-2">
                  <Database size={14} /> Kafka Broker
                </h4>
                <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
                  <MetricCard icon={Clock} label="Uptime" value={node.kafka.uptime || '-'} />
                  <MetricCard icon={MemoryStick} label="Memory (RSS)" value={`${node.kafka.memory_rss_mb || 0} MB`} />
                  <MetricCard icon={Database} label="Data Size" value={`${node.kafka.data_size_mb || 0} MB`} />
                  <MetricCard icon={BarChart3} label="Topics" value={node.kafka.topics ?? 0} />
                  <MetricCard icon={HardDrive} label="Partitions" value={node.kafka.partitions ?? 0} />
                  <MetricCard icon={Wifi} label="Connections" value={node.kafka.connections ?? 0} />
                </div>
              </div>

              {/* System Metrics with progress bars */}
              <div>
                <h4 className="text-sm font-semibold text-gray-700 mb-3 flex items-center gap-2">
                  <Cpu size={14} /> System Resources
                </h4>
                <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                  <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-600">CPU ({node.system.cpu_cores || 0} cores)</span>
                      <span className="font-medium">{node.system.cpu_usage_pct ?? 0}%</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2.5">
                      <div
                        className={`h-2.5 rounded-full transition-all duration-500 ${
                          (node.system.cpu_usage_pct ?? 0) > 80 ? 'bg-red-500' :
                          (node.system.cpu_usage_pct ?? 0) > 60 ? 'bg-yellow-500' : 'bg-blue-500'
                        }`}
                        style={{ width: `${Math.min(node.system.cpu_usage_pct ?? 0, 100)}%` }}
                      />
                    </div>
                    <p className="text-xs text-gray-400">Load: {node.system.load_1m ?? 0} / {node.system.load_5m ?? 0} / {node.system.load_15m ?? 0}</p>
                  </div>

                  <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-600">Memory</span>
                      <span className="font-medium">{node.system.memory_used_mb ?? 0} / {node.system.memory_total_mb ?? 0} MB</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2.5">
                      <div
                        className={`h-2.5 rounded-full transition-all duration-500 ${
                          (node.system.memory_usage_pct ?? 0) > 80 ? 'bg-red-500' :
                          (node.system.memory_usage_pct ?? 0) > 60 ? 'bg-yellow-500' : 'bg-green-500'
                        }`}
                        style={{ width: `${Math.min(node.system.memory_usage_pct ?? 0, 100)}%` }}
                      />
                    </div>
                    <p className="text-xs text-gray-400">{node.system.memory_available_mb ?? 0} MB available</p>
                  </div>

                  <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-600">Disk (data)</span>
                      <span className="font-medium">{node.disk.data?.usage_pct ?? node.disk.root?.usage_pct ?? 0}%</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2.5">
                      <div
                        className={`h-2.5 rounded-full transition-all duration-500 ${
                          (node.disk.data?.usage_pct ?? node.disk.root?.usage_pct ?? 0) > 80 ? 'bg-red-500' :
                          (node.disk.data?.usage_pct ?? node.disk.root?.usage_pct ?? 0) > 60 ? 'bg-yellow-500' : 'bg-purple-500'
                        }`}
                        style={{ width: `${Math.min(node.disk.data?.usage_pct ?? node.disk.root?.usage_pct ?? 0, 100)}%` }}
                      />
                    </div>
                    <p className="text-xs text-gray-400">
                      {((node.disk.data?.available_mb ?? node.disk.root?.available_mb ?? 0) / 1024).toFixed(1)} GB free
                    </p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}
