import { useEffect, useState } from 'react';
import {
  Activity, ExternalLink, AlertCircle, Loader2, RefreshCw, CheckCircle2, Plus,
  Zap, Box, AlertTriangle, Database, Users, TrendingUp,
} from 'lucide-react';
import {
  getGrafanaInfo, deployMonitoring, getHosts, getFiringAlerts, getAlertRules,
  getMonitoringSummary, getAlertIncidents,
  type AlertRule, type MonitoringSummary,
} from '../../lib/api';
import type { Host } from '../../types';

type Props = {
  clusterId: string;
  isExternal: boolean;
};

type Status = {
  deployed: boolean;
  grafana_url?: string;
  prometheus_url?: string;
  grafana_port?: number;
  prometheus_port?: number;
};

type Incident = {
  id: string;
  alert_name: string;
  severity: string;
  status: string;
  summary?: string;
  started_at: string;
  resolved_at?: string | null;
};

const fmtBytes = (b: number): string => {
  if (b == null || isNaN(b)) return '—';
  const u = ['B', 'KB', 'MB', 'GB', 'TB'];
  let n = Math.max(0, b);
  let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return `${n.toFixed(n >= 100 ? 0 : 1)} ${u[i]}`;
};

const fmtNum = (n: number): string =>
  n == null || isNaN(n) ? '—' : n >= 1000 ? n.toLocaleString(undefined, { maximumFractionDigits: 0 }) : n.toFixed(1);

const validateExternalJmxEndpoints = (value: string): string | null => {
  const endpoints = value.split(',').map(s => s.trim()).filter(Boolean);
  if (!endpoints.length) {
    return 'Enter at least one broker JMX endpoint before deploying monitoring.';
  }

  for (const endpoint of endpoints) {
    const [host, port] = endpoint.split(/:(?=[^:]*$)/);
    const normalizedHost = host.replace(/^\[|\]$/g, '').toLowerCase();
    const portNumber = Number(port);
    if (!host || !port || !/^\d+$/.test(port) || portNumber < 1 || portNumber > 65535) {
      return `JMX endpoint "${endpoint}" must be a valid host:port value.`;
    }
    if (normalizedHost === 'localhost' || normalizedHost === '0.0.0.0' || normalizedHost === '::1' || normalizedHost.startsWith('127.')) {
      return `JMX endpoint "${endpoint}" points to the monitoring host itself. Use the broker IP/hostname reachable from the selected Tantor host.`;
    }
  }

  return null;
};

/**
 * Per-cluster Monitoring tab — customer asked for detailed metrics inside the
 * cluster view, not on a global sidebar page. This component pulls a digest
 * from /api/monitoring/clusters/{id}/summary every 10s and renders:
 *   - Throughput (msgs/sec, bytes in/out)
 *   - Broker up/down + scrape target table
 *   - Top 5 topics by message rate
 *   - Top 5 consumer groups by lag
 *   - Under-replicated partitions
 *   - JVM heap + GC rate
 *   - Recent firing/resolved incidents
 *   - Direct links to Tantor-rendered Grafana dashboards
 *
 * For not-yet-deployed clusters (typical for newly-imported external ones)
 * it renders a deploy form inline so monitoring is two clicks away.
 */
export default function ClusterMonitoring({ clusterId, isExternal }: Props) {
  const [status, setStatus] = useState<Status | null>(null);
  const [hosts, setHosts] = useState<Host[]>([]);
  const [rules, setRules] = useState<AlertRule[]>([]);
  const [firingCount, setFiringCount] = useState<number>(0);
  const [summary, setSummary] = useState<MonitoringSummary | null>(null);
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [loading, setLoading] = useState(true);
  const [showDeploy, setShowDeploy] = useState(false);
  const [deployHost, setDeployHost] = useState('');
  const [deployJmx, setDeployJmx] = useState('');
  const [deploying, setDeploying] = useState(false);
  const [deployResult, setDeployResult] = useState<string | null>(null);

  const refresh = async () => {
    setLoading(true);
    try {
      const [s, h, r, f, sum, inc] = await Promise.all([
        getGrafanaInfo(clusterId).catch(() => ({ deployed: false })),
        getHosts().catch(() => []),
        getAlertRules(clusterId).catch(() => [] as AlertRule[]),
        getFiringAlerts(clusterId).catch(() => ({ count: 0, alerts: [] })),
        getMonitoringSummary(clusterId).catch(() => ({ available: false })),
        getAlertIncidents(clusterId, undefined, 10).catch(() => []),
        new Promise(resolve => setTimeout(resolve, 500)) // Ensure spinner is visible for at least 500ms
      ]);
      setStatus(s);
      setHosts(h);
      setRules(r);
      setFiringCount(f.count || 0);
      setSummary(sum);
      setIncidents(inc as Incident[]);
      if (h.length > 0 && !deployHost) setDeployHost(h[0].id);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { refresh(); }, [clusterId]);

  // Auto-refresh every 10s when monitoring is deployed
  useEffect(() => {
    if (!status?.deployed) return;
    const i = setInterval(refresh, 10000);
    return () => clearInterval(i);
  }, [status?.deployed, clusterId]);

  const onDeploy = async () => {
    if (!deployHost) return;
    if (isExternal) {
      const validationError = validateExternalJmxEndpoints(deployJmx);
      if (validationError) {
        setDeployResult(`error: ${validationError}`);
        return;
      }
    }
    setDeploying(true);
    setDeployResult(null);
    try {
      const payload: { monitoring_host_id: string; external_jmx_endpoints?: string[] } = {
        monitoring_host_id: deployHost,
      };
      if (isExternal && deployJmx.trim()) {
        payload.external_jmx_endpoints = deployJmx.split(',').map(s => s.trim()).filter(Boolean);
      }
      await deployMonitoring(clusterId, payload);
      setDeployResult('success');
      setShowDeploy(false);
      refresh();
    } catch (e: unknown) {
      const apiErr = (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      setDeployResult(`error: ${apiErr || (e instanceof Error ? e.message : 'unknown')}`);
    } finally {
      setDeploying(false);
    }
  };

  if (loading && !status) {
    return <div className="flex items-center gap-2 text-sm text-gray-500"><Loader2 size={14} className="animate-spin" /> Loading monitoring…</div>;
  }

  // Public hostname for clickable Grafana / Prometheus links
  const publicHost = window.location.hostname;
  const grafanaPublic = (status?.grafana_url || '').replace(/\/\/(127\.0\.0\.1|localhost)/, `//${publicHost}`);
  const promPublic = (status?.prometheus_url || '').replace(/\/\/(127\.0\.0\.1|localhost)/, `//${publicHost}`);

  if (!status?.deployed) {
    return (
      <div className="space-y-4">
        <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-2">
          <Activity size={16} className="text-blue-600" /> Monitoring
        </h3>
        <div className="border rounded-lg p-4 bg-yellow-50 border-yellow-200">
          <div className="flex items-start gap-2 mb-3">
            <AlertCircle size={18} className="text-yellow-600 mt-0.5 shrink-0" />
            <div className="flex-1">
              <div className="text-sm font-medium text-yellow-900">Monitoring not deployed for this cluster</div>
              <div className="text-xs text-yellow-800 mt-1">
                {isExternal
                  ? 'Tantor will deploy Prometheus + Alertmanager + Grafana on a host you control, then scrape the JMX endpoint(s) you supply for your external brokers.'
                  : 'Managed clusters auto-deploy monitoring on cluster create. If this is missing, you can deploy it now.'
                }
              </div>
            </div>
          </div>
          {!showDeploy ? (
            <button onClick={() => setShowDeploy(true)} className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 text-white text-xs rounded-lg hover:bg-blue-700">
              <Plus size={13} /> Deploy monitoring
            </button>
          ) : (
            <div className="bg-white border border-gray-200 rounded p-3 space-y-3">
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">Tantor host (where Prometheus + Grafana will run)</label>
                <select value={deployHost} onChange={e => setDeployHost(e.target.value)} className="w-full px-2.5 py-1.5 border rounded text-sm">
                  {hosts.map(h => (<option key={h.id} value={h.id}>{h.hostname} ({h.ip_address})</option>))}
                </select>
              </div>
              {isExternal && (
                <div>
                  <label className="block text-xs font-medium text-gray-700 mb-1">JMX endpoints (comma-separated host:port that your brokers expose)</label>
                  <input type="text" value={deployJmx} onChange={e => setDeployJmx(e.target.value)}
                    placeholder="broker1.acme.com:7071, broker2.acme.com:7071"
                    className="w-full px-2.5 py-1.5 border rounded text-sm font-mono" />
                  <div className="text-[11px] text-amber-700 mt-1">Do not use localhost or 127.0.0.1; Prometheus runs on the selected Tantor host and must reach the broker address over the network.</div>
                  <div className="text-[11px] text-gray-500 mt-1">Your brokers must expose JMX (or JMX exporter) — Tantor doesn't own them.</div>
                </div>
              )}
              {deployResult && deployResult !== 'success' && (
                <div className="text-xs text-red-700 bg-red-50 border border-red-200 p-2 rounded">{deployResult}</div>
              )}
              <div className="flex justify-end gap-2">
                <button onClick={() => setShowDeploy(false)} className="px-3 py-1.5 text-xs border rounded hover:bg-gray-50">Cancel</button>
                <button onClick={onDeploy} disabled={deploying || !deployHost || (isExternal && !deployJmx.trim())}
                  className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 text-white text-xs rounded-lg hover:bg-blue-700 disabled:opacity-50">
                  {deploying && <Loader2 size={14} className="animate-spin" />} Deploy
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    );
  }

  const sum = summary?.available ? summary : null;

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-2">
          <Activity size={16} className="text-blue-600" /> Monitoring
          <span className="text-xs text-gray-400 font-normal">— auto-refresh 10s</span>
        </h3>
        <div className="flex items-center gap-2">
          <span className="text-sm text-green-700 inline-flex items-center gap-1">
            <CheckCircle2 size={14} /> deployed
          </span>
          <button onClick={refresh} disabled={loading} className="flex items-center gap-1.5 px-3 py-1.5 text-xs border rounded-lg hover:bg-gray-50 disabled:opacity-50">
            <RefreshCw size={13} className={loading ? "animate-spin" : ""} /> Refresh
          </button>
        </div>
      </div>

      {/* Top stat row — throughput */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Stat
          icon={<Zap size={14} />}
          label="Messages In"
          value={`${fmtNum(sum?.throughput?.messages_in_per_sec ?? NaN)} /s`}
          sub="cluster-wide rate (1m avg)"
        />
        <Stat
          icon={<TrendingUp size={14} />}
          label="Bytes In"
          value={`${fmtBytes(sum?.throughput?.bytes_in_per_sec ?? NaN)} /s`}
          sub="ingress to brokers"
        />
        <Stat
          icon={<TrendingUp size={14} />}
          label="Bytes Out"
          value={`${fmtBytes(sum?.throughput?.bytes_out_per_sec ?? NaN)} /s`}
          sub="egress from brokers"
        />
        <Stat
          icon={<Box size={14} />}
          label="Brokers up"
          value={`${sum?.broker_up_count ?? 0}/${sum?.broker_total_count ?? 0}`}
          sub="JMX scrape targets"
          warn={!!(sum && sum.broker_total_count != null && sum.broker_up_count != null && sum.broker_up_count < sum.broker_total_count)}
        />
      </div>

      {/* Second row — alerts + heap + URP */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <Stat
          icon={<AlertTriangle size={14} />}
          label="Firing alerts"
          value={firingCount.toString()}
          sub={`${rules.length} rules configured`}
          warn={firingCount > 0}
        />
        <Stat
          icon={<AlertTriangle size={14} />}
          label="Under-replicated"
          value={(sum?.under_replicated_partitions ?? 0).toString()}
          sub="partitions"
          warn={(sum?.under_replicated_partitions ?? 0) > 0}
        />
        <Stat
          icon={<Database size={14} />}
          label="JVM heap"
          value={`${fmtNum(sum?.jvm_heap_mb ?? NaN)} MB`}
          sub="across all brokers"
        />
        <Stat
          icon={<RefreshCw size={14} />}
          label="GC rate"
          value={`${fmtNum(sum?.jvm_gc_count_per_sec ?? NaN)}/s`}
          sub="5m avg, all brokers"
        />
      </div>

      {/* Third row — top topics + top consumer groups */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Card title="Top topics by msgs/sec">
          {(sum?.top_topics_by_msgs ?? []).length === 0 ? (
            <Empty msg="No traffic yet" />
          ) : (
            <table className="w-full text-sm">
              <thead className="bg-gray-50">
                <tr><th className="text-left px-2 py-1 font-medium">Topic</th><th className="text-right px-2 py-1 font-medium">Msgs/sec</th></tr>
              </thead>
              <tbody>
                {sum!.top_topics_by_msgs!.map((r, i) => (
                  <tr key={i} className="border-t">
                    <td className="px-2 py-1 font-mono text-xs">{r.key}</td>
                    <td className="px-2 py-1 text-right">{fmtNum(r.value)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Card>

        <Card title="Top consumer groups by lag">
          {(sum?.top_consumer_groups_by_lag ?? []).length === 0 ? (
            <Empty msg="No consumer groups yet" />
          ) : (
            <table className="w-full text-sm">
              <thead className="bg-gray-50">
                <tr><th className="text-left px-2 py-1 font-medium"><Users size={12} className="inline mr-1" />Group</th><th className="text-right px-2 py-1 font-medium">Lag</th></tr>
              </thead>
              <tbody>
                {sum!.top_consumer_groups_by_lag!.map((r, i) => (
                  <tr key={i} className="border-t">
                    <td className="px-2 py-1 font-mono text-xs">{r.key}</td>
                    <td className="px-2 py-1 text-right">{fmtNum(r.value)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Card>
      </div>

      {/* Scrape targets + recent incidents */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Card title="Scrape targets">
          {(sum?.scrape_targets ?? []).length === 0 ? (
            <Empty msg="No targets yet" />
          ) : (
            <table className="w-full text-sm">
              <thead className="bg-gray-50">
                <tr>
                  <th className="text-left px-2 py-1 font-medium">Job</th>
                  <th className="text-left px-2 py-1 font-medium">Instance</th>
                  <th className="text-left px-2 py-1 font-medium">Status</th>
                </tr>
              </thead>
              <tbody>
                {sum!.scrape_targets!.map((t, i) => (
                  <tr key={i} className="border-t">
                    <td className="px-2 py-1 font-mono text-xs">{t.job}</td>
                    <td className="px-2 py-1 font-mono text-xs">{t.instance}</td>
                    <td className="px-2 py-1">
                      <span className={`px-1.5 py-0.5 rounded text-xs ${t.up ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}>
                        {t.up ? 'up' : 'down'}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Card>

        <Card title="Recent incidents">
          {incidents.length === 0 ? (
            <Empty msg="No incidents" />
          ) : (
            <ul className="text-sm divide-y">
              {incidents.slice(0, 5).map(inc => (
                <li key={inc.id} className="py-2 flex items-start justify-between gap-3">
                  <div>
                    <div className="text-xs font-medium">
                      {inc.alert_name}
                      <span className={`ml-2 px-1.5 py-0.5 rounded text-[10px] uppercase ${
                        inc.severity === 'critical' ? 'bg-red-100 text-red-700' : 'bg-yellow-100 text-yellow-700'
                      }`}>{inc.severity}</span>
                    </div>
                    <div className="text-xs text-gray-500 mt-0.5">{inc.summary || '-'}</div>
                  </div>
                  <span className={`px-2 py-0.5 rounded text-xs shrink-0 ${
                    inc.status === 'firing' ? 'bg-red-100 text-red-700' : 'bg-green-100 text-green-700'
                  }`}>{inc.status}</span>
                </li>
              ))}
            </ul>
          )}
        </Card>
      </div>

      {/* Dashboard links */}
      <div className="border rounded-lg p-4">
        <h4 className="text-sm font-semibold text-gray-700 mb-3">Open in Grafana</h4>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
          <a href={`${grafanaPublic}/dashboards`} target="_blank" rel="noopener noreferrer"
            className="flex items-center justify-between border rounded p-2 hover:bg-blue-50 hover:border-blue-300">
            <span className="text-sm font-medium">Kafka Overview</span>
            <ExternalLink size={14} className="text-gray-400" />
          </a>
          <a href={`${grafanaPublic}/dashboards`} target="_blank" rel="noopener noreferrer"
            className="flex items-center justify-between border rounded p-2 hover:bg-blue-50 hover:border-blue-300">
            <span className="text-sm font-medium">Kafka — Topic Performance</span>
            <ExternalLink size={14} className="text-gray-400" />
          </a>
        </div>
        <div className="mt-3 grid grid-cols-1 md:grid-cols-2 gap-2 text-xs">
          <a href={grafanaPublic} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">
            Grafana → {grafanaPublic} (admin/admin)
          </a>
          <a href={promPublic} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">
            Prometheus → {promPublic}
          </a>
        </div>
      </div>
    </div>
  );
}

function Stat({ icon, label, value, sub, warn }: { icon?: React.ReactNode; label: string; value: string; sub?: string; warn?: boolean }) {
  return (
    <div className={`border rounded-lg px-3 py-2 ${warn ? 'border-red-300 bg-red-50' : 'bg-white'}`}>
      <div className={`text-[11px] uppercase tracking-wide flex items-center gap-1 ${warn ? 'text-red-700' : 'text-gray-500'}`}>
        {icon}
        {label}
      </div>
      <div className={`text-xl font-semibold mt-0.5 ${warn ? 'text-red-700' : 'text-gray-900'}`}>{value}</div>
      {sub && <div className="text-xs text-gray-500 mt-0.5">{sub}</div>}
    </div>
  );
}

function Card({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="border rounded-lg overflow-hidden">
      <div className="px-3 py-2 bg-gray-50 border-b text-xs font-semibold text-gray-700">{title}</div>
      <div className="p-2">{children}</div>
    </div>
  );
}

function Empty({ msg }: { msg: string }) {
  return <div className="px-3 py-4 text-sm text-gray-400 italic text-center">{msg}</div>;
}
