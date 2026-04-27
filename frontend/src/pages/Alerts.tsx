import { useEffect, useMemo, useState } from 'react';
import {
  Bell, RefreshCw, Plus, Trash2, Send, X, Check, AlertCircle, AlertTriangle, Info,
} from 'lucide-react';
import {
  getClusters,
  getRuleTemplates, getAlertRules, createAlertRule, updateAlertRule, deleteAlertRule,
  getFiringAlerts, getAlertIncidents,
  getNotificationChannels, createNotificationChannel, updateNotificationChannel,
  deleteNotificationChannel, testNotificationChannel,
  type AlertRule, type AlertRuleCreate, type RuleTemplate, type FiringAlertsResponse,
  type AlertIncident, type NotificationChannel, type Severity, type ChannelKind,
} from '../lib/api';
import { isAdmin } from '../lib/auth';

type Tab = 'firing' | 'rules' | 'channels';

const SEVERITY_STYLES: Record<Severity, { bg: string; text: string; border: string; icon: typeof AlertCircle }> = {
  critical: { bg: 'bg-red-50', text: 'text-red-700', border: 'border-red-200', icon: AlertCircle },
  warning: { bg: 'bg-amber-50', text: 'text-amber-700', border: 'border-amber-200', icon: AlertTriangle },
  info: { bg: 'bg-sky-50', text: 'text-sky-700', border: 'border-sky-200', icon: Info },
};

const KIND_LABELS: Record<ChannelKind, string> = {
  slack: 'Slack',
  webhook: 'Webhook',
  email: 'Email',
  tantor_internal: 'Tantor Internal',
};

interface ClusterOpt { id: string; name: string }

function SeverityPill({ severity }: { severity: Severity }) {
  const s = SEVERITY_STYLES[severity];
  const Icon = s.icon;
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium border ${s.bg} ${s.text} ${s.border}`}>
      <Icon size={12} />
      {severity}
    </span>
  );
}

export default function AlertsPage() {
  const admin = isAdmin();
  const [tab, setTab] = useState<Tab>('firing');
  const [clusters, setClusters] = useState<ClusterOpt[]>([]);
  const [clusterId, setClusterId] = useState<string>('');
  const [error, setError] = useState('');

  useEffect(() => {
    getClusters()
      .then((cs) => {
        const opts = cs.map((c) => ({ id: c.id, name: c.name }));
        setClusters(opts);
        if (opts.length && !clusterId) setClusterId(opts[0].id);
      })
      .catch((e) => setError(`Failed to load clusters: ${e?.message ?? e}`));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div className="p-8 max-w-7xl mx-auto">
      <div className="flex items-start justify-between mb-6 gap-4">
        <div>
          <h1 className="text-2xl font-semibold flex items-center gap-2">
            <Bell size={24} /> Alerts
          </h1>
          <p className="text-sm text-gray-500 mt-1">
            Prometheus-backed alerting via Alertmanager. Rules are scoped to a cluster; channels are shared.
          </p>
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Cluster</label>
          <select
            value={clusterId}
            onChange={(e) => setClusterId(e.target.value)}
            disabled={!clusters.length}
            className="px-3 py-2 border rounded text-sm min-w-[220px]"
          >
            {!clusters.length && <option>No clusters</option>}
            {clusters.map((c) => <option key={c.id} value={c.id}>{c.name}</option>)}
          </select>
        </div>
      </div>

      <div className="border-b mb-4 flex gap-1">
        {(['firing', 'rules', 'channels'] as Tab[]).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2 text-sm font-medium border-b-2 -mb-px capitalize ${
              tab === t ? 'border-blue-600 text-blue-700' : 'border-transparent text-gray-500 hover:text-gray-700'
            }`}
          >
            {t}
          </button>
        ))}
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded mb-4 text-sm">
          {error}
        </div>
      )}

      {!clusterId ? (
        <div className="bg-white border rounded p-8 text-center text-gray-500">
          Create a cluster first to manage alerts.
        </div>
      ) : tab === 'firing' ? (
        <FiringTab clusterId={clusterId} />
      ) : tab === 'rules' ? (
        <RulesTab clusterId={clusterId} admin={admin} />
      ) : (
        <ChannelsTab admin={admin} />
      )}
    </div>
  );
}

// ── Firing tab ────────────────────────────────────────────────────────────

function FiringTab({ clusterId }: { clusterId: string }) {
  const [data, setData] = useState<FiringAlertsResponse | null>(null);
  const [incidents, setIncidents] = useState<AlertIncident[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const load = useMemo(
    () => async () => {
      setLoading(true);
      setError('');
      try {
        const [firing, hist] = await Promise.all([
          getFiringAlerts(clusterId),
          getAlertIncidents(clusterId, undefined, 50),
        ]);
        setData(firing);
        setIncidents(hist);
      } catch (e: unknown) {
        const err = e as { response?: { data?: { detail?: string } }; message?: string };
        setError(err.response?.data?.detail || err.message || 'Failed to load alerts');
      } finally {
        setLoading(false);
      }
    },
    [clusterId],
  );

  useEffect(() => {
    load();
    const t = setInterval(load, 15000);
    return () => clearInterval(t);
  }, [load]);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          {data?.alertmanager_reachable ? (
            <span className="text-sm text-green-700 flex items-center gap-1.5">
              <Check size={14} /> Alertmanager connected
              {data.alertmanager_url && <span className="text-gray-400 font-mono text-xs">({data.alertmanager_url})</span>}
            </span>
          ) : (
            <span className="text-sm text-amber-700 flex items-center gap-1.5">
              <AlertTriangle size={14} /> Alertmanager not reachable — deploy the monitoring stack from the Monitoring page first
            </span>
          )}
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="px-3 py-1.5 text-sm border rounded flex items-center gap-2 hover:bg-gray-50 disabled:opacity-50"
        >
          <RefreshCw size={14} className={loading ? 'animate-spin' : ''} /> Refresh
        </button>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded text-sm">{error}</div>
      )}

      <section>
        <h2 className="text-sm font-semibold uppercase tracking-wide text-gray-600 mb-2">
          Currently Firing ({data?.count ?? 0})
        </h2>
        <div className="bg-white border rounded">
          {!data?.alerts.length ? (
            <div className="p-6 text-center text-gray-400 text-sm">No alerts firing right now.</div>
          ) : (
            <ul className="divide-y">
              {data.alerts.map((a) => (
                <li key={a.fingerprint} className="p-4 flex items-start gap-3">
                  <div className="pt-0.5"><SeverityPill severity={a.severity} /></div>
                  <div className="flex-1">
                    <div className="font-medium">{a.alert_name}</div>
                    {a.summary && <div className="text-sm text-gray-700 mt-0.5">{a.summary}</div>}
                    {a.description && <div className="text-sm text-gray-500 mt-0.5">{a.description}</div>}
                    <div className="text-xs text-gray-400 mt-1 font-mono">
                      state: {a.state} · started: {a.started_at ? new Date(a.started_at).toLocaleString() : '—'}
                    </div>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </div>
      </section>

      <section>
        <h2 className="text-sm font-semibold uppercase tracking-wide text-gray-600 mb-2">
          Recent Incidents (last 50)
        </h2>
        <div className="bg-white border rounded overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 text-left text-xs uppercase text-gray-500">
              <tr>
                <th className="px-3 py-2 w-[160px]">Started</th>
                <th className="px-3 py-2 w-[100px]">Severity</th>
                <th className="px-3 py-2 w-[100px]">Status</th>
                <th className="px-3 py-2">Alert</th>
                <th className="px-3 py-2 w-[160px]">Last seen</th>
              </tr>
            </thead>
            <tbody>
              {!incidents.length && (
                <tr><td colSpan={5} className="px-3 py-6 text-center text-gray-400">No incidents recorded yet.</td></tr>
              )}
              {incidents.map((i) => (
                <tr key={i.id} className="border-t hover:bg-gray-50 align-top">
                  <td className="px-3 py-2 text-xs font-mono text-gray-600">
                    {new Date(i.started_at).toLocaleString()}
                  </td>
                  <td className="px-3 py-2"><SeverityPill severity={i.severity} /></td>
                  <td className="px-3 py-2">
                    <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                      i.status === 'firing'
                        ? 'bg-red-50 text-red-700 border border-red-200'
                        : 'bg-green-50 text-green-700 border border-green-200'
                    }`}>{i.status}</span>
                  </td>
                  <td className="px-3 py-2">
                    <div className="font-medium">{i.alert_name}</div>
                    {i.summary && <div className="text-xs text-gray-500">{i.summary}</div>}
                  </td>
                  <td className="px-3 py-2 text-xs font-mono text-gray-500">
                    {new Date(i.last_seen_at).toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

// ── Rules tab ─────────────────────────────────────────────────────────────

function RulesTab({ clusterId, admin }: { clusterId: string; admin: boolean }) {
  const [rules, setRules] = useState<AlertRule[]>([]);
  const [templates, setTemplates] = useState<RuleTemplate[]>([]);
  const [channels, setChannels] = useState<NotificationChannel[]>([]);
  const [editing, setEditing] = useState<Partial<AlertRule> | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const load = useMemo(
    () => async () => {
      setLoading(true);
      setError('');
      try {
        const [r, t, c] = await Promise.all([
          getAlertRules(clusterId),
          getRuleTemplates(clusterId),
          getNotificationChannels(),
        ]);
        setRules(r); setTemplates(t); setChannels(c);
      } catch (e: unknown) {
        const err = e as { response?: { data?: { detail?: string } }; message?: string };
        setError(err.response?.data?.detail || err.message || 'Failed to load rules');
      } finally {
        setLoading(false);
      }
    },
    [clusterId],
  );

  useEffect(() => { load(); }, [load]);

  const startFromTemplate = (tpl: RuleTemplate) => {
    setEditing({
      name: tpl.name,
      expr: tpl.expr,
      for_seconds: tpl.for_seconds,
      severity: tpl.severity,
      summary: tpl.summary,
      description: tpl.description,
      channel_ids: [],
      template: tpl.id,
      enabled: true,
    });
  };

  const startBlank = () => {
    setEditing({
      name: '', expr: '', for_seconds: 60, severity: 'warning',
      summary: '', description: '', channel_ids: [], template: null, enabled: true,
    });
  };

  const save = async () => {
    if (!editing || !editing.name || !editing.expr) {
      setError('Name and expr are required');
      return;
    }
    try {
      const body: AlertRuleCreate = {
        name: editing.name!,
        expr: editing.expr!,
        for_seconds: editing.for_seconds ?? 60,
        severity: editing.severity ?? 'warning',
        summary: editing.summary ?? null,
        description: editing.description ?? null,
        channel_ids: editing.channel_ids ?? [],
        template: editing.template ?? null,
        enabled: editing.enabled ?? true,
      };
      if (editing.id) {
        await updateAlertRule(clusterId, editing.id, body);
      } else {
        await createAlertRule(clusterId, body);
      }
      setEditing(null);
      await load();
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Failed to save rule');
    }
  };

  const remove = async (rule: AlertRule) => {
    if (!confirm(`Delete rule "${rule.name}"?`)) return;
    try {
      await deleteAlertRule(clusterId, rule.id);
      await load();
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Delete failed');
    }
  };

  return (
    <div className="space-y-4">
      {admin && (
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-sm text-gray-500">From template:</span>
          {templates.map((t) => (
            <button
              key={t.id}
              onClick={() => startFromTemplate(t)}
              className="px-3 py-1.5 text-sm border rounded hover:bg-gray-50"
            >
              {t.name}
            </button>
          ))}
          <button
            onClick={startBlank}
            className="px-3 py-1.5 text-sm bg-blue-600 text-white rounded flex items-center gap-1.5 hover:bg-blue-700"
          >
            <Plus size={14} /> Custom rule
          </button>
          <button
            onClick={load}
            disabled={loading}
            className="px-3 py-1.5 text-sm border rounded flex items-center gap-1.5 hover:bg-gray-50 disabled:opacity-50 ml-auto"
          >
            <RefreshCw size={14} className={loading ? 'animate-spin' : ''} /> Refresh
          </button>
        </div>
      )}

      {error && <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded text-sm">{error}</div>}

      <div className="bg-white border rounded overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 text-left text-xs uppercase text-gray-500">
            <tr>
              <th className="px-3 py-2">Name</th>
              <th className="px-3 py-2 w-[100px]">Severity</th>
              <th className="px-3 py-2 w-[80px]">For</th>
              <th className="px-3 py-2">PromQL</th>
              <th className="px-3 py-2 w-[80px]">Channels</th>
              <th className="px-3 py-2 w-[80px]">Enabled</th>
              {admin && <th className="px-3 py-2 w-[140px]">Actions</th>}
            </tr>
          </thead>
          <tbody>
            {!rules.length && (
              <tr><td colSpan={admin ? 7 : 6} className="px-3 py-8 text-center text-gray-400">No rules yet.</td></tr>
            )}
            {rules.map((r) => (
              <tr key={r.id} className="border-t hover:bg-gray-50 align-top">
                <td className="px-3 py-2">
                  <div className="font-medium">{r.name}</div>
                  {r.template && <div className="text-xs text-gray-400">from template: {r.template}</div>}
                </td>
                <td className="px-3 py-2"><SeverityPill severity={r.severity} /></td>
                <td className="px-3 py-2 text-xs font-mono text-gray-600">{r.for_seconds}s</td>
                <td className="px-3 py-2"><code className="text-xs bg-gray-50 px-1.5 py-0.5 rounded break-all">{r.expr}</code></td>
                <td className="px-3 py-2 text-xs text-center">{r.channel_ids.length}</td>
                <td className="px-3 py-2 text-xs">
                  {r.enabled ? <span className="text-green-700">on</span> : <span className="text-gray-400">off</span>}
                </td>
                {admin && (
                  <td className="px-3 py-2 flex gap-1">
                    <button onClick={() => setEditing(r)} className="px-2 py-1 text-xs border rounded hover:bg-gray-50">Edit</button>
                    <button onClick={() => remove(r)} className="px-2 py-1 text-xs border rounded text-red-600 hover:bg-red-50">
                      <Trash2 size={12} />
                    </button>
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {editing && (
        <RuleEditor
          rule={editing}
          channels={channels}
          onCancel={() => setEditing(null)}
          onSave={save}
          onChange={(patch) => setEditing({ ...editing, ...patch })}
        />
      )}
    </div>
  );
}

function RuleEditor({
  rule, channels, onCancel, onSave, onChange,
}: {
  rule: Partial<AlertRule>;
  channels: NotificationChannel[];
  onCancel: () => void;
  onSave: () => void;
  onChange: (patch: Partial<AlertRule>) => void;
}) {
  const toggleChannel = (id: string) => {
    const cur = rule.channel_ids ?? [];
    onChange({ channel_ids: cur.includes(id) ? cur.filter((c) => c !== id) : [...cur, id] });
  };

  return (
    <div className="fixed inset-0 bg-black/40 flex items-center justify-center p-4 z-50">
      <div className="bg-white rounded-lg shadow-xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        <div className="px-6 py-4 border-b flex items-center justify-between">
          <h3 className="font-semibold">{rule.id ? 'Edit rule' : 'New rule'}</h3>
          <button onClick={onCancel} className="text-gray-400 hover:text-gray-600"><X size={18} /></button>
        </div>
        <div className="p-6 space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-xs font-medium text-gray-700 mb-1">Name</label>
              <input value={rule.name ?? ''} onChange={(e) => onChange({ name: e.target.value })}
                className="w-full px-3 py-2 border rounded text-sm" />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-700 mb-1">Severity</label>
              <select value={rule.severity ?? 'warning'} onChange={(e) => onChange({ severity: e.target.value as Severity })}
                className="w-full px-3 py-2 border rounded text-sm">
                <option value="info">info</option>
                <option value="warning">warning</option>
                <option value="critical">critical</option>
              </select>
            </div>
          </div>

          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">PromQL expression</label>
            <textarea value={rule.expr ?? ''} onChange={(e) => onChange({ expr: e.target.value })} rows={3}
              className="w-full px-3 py-2 border rounded text-sm font-mono" placeholder='up{job="kafka-jmx"} == 0' />
            <p className="text-xs text-gray-400 mt-1">
              Fires when the expression returns a non-empty vector for &gt; <code>for</code> seconds.
            </p>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-xs font-medium text-gray-700 mb-1">For (seconds)</label>
              <input type="number" min={0} max={86400} value={rule.for_seconds ?? 60}
                onChange={(e) => onChange({ for_seconds: parseInt(e.target.value || '0', 10) })}
                className="w-full px-3 py-2 border rounded text-sm" />
            </div>
            <div className="flex items-center pt-5">
              <label className="flex items-center gap-2 text-sm">
                <input type="checkbox" checked={rule.enabled ?? true} onChange={(e) => onChange({ enabled: e.target.checked })} />
                Enabled
              </label>
            </div>
          </div>

          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Summary</label>
            <input value={rule.summary ?? ''} onChange={(e) => onChange({ summary: e.target.value })}
              className="w-full px-3 py-2 border rounded text-sm" />
          </div>

          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Description</label>
            <textarea value={rule.description ?? ''} onChange={(e) => onChange({ description: e.target.value })} rows={2}
              className="w-full px-3 py-2 border rounded text-sm" />
          </div>

          <div>
            <label className="block text-xs font-medium text-gray-700 mb-2">Notification channels</label>
            {!channels.length ? (
              <p className="text-xs text-gray-400">No channels yet — create one in the Channels tab.</p>
            ) : (
              <div className="flex flex-wrap gap-2">
                {channels.map((c) => {
                  const checked = (rule.channel_ids ?? []).includes(c.id);
                  return (
                    <label key={c.id} className={`flex items-center gap-1.5 px-2 py-1 border rounded text-xs cursor-pointer ${checked ? 'bg-blue-50 border-blue-300' : ''}`}>
                      <input type="checkbox" checked={checked} onChange={() => toggleChannel(c.id)} />
                      <span className="font-medium">{c.name}</span>
                      <span className="text-gray-400">({KIND_LABELS[c.kind]})</span>
                    </label>
                  );
                })}
              </div>
            )}
          </div>
        </div>
        <div className="px-6 py-4 border-t flex justify-end gap-2 bg-gray-50">
          <button onClick={onCancel} className="px-4 py-2 text-sm border rounded hover:bg-white">Cancel</button>
          <button onClick={onSave} className="px-4 py-2 text-sm bg-blue-600 text-white rounded hover:bg-blue-700">
            Save
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Channels tab ──────────────────────────────────────────────────────────

function ChannelsTab({ admin }: { admin: boolean }) {
  const [channels, setChannels] = useState<NotificationChannel[]>([]);
  const [editing, setEditing] = useState<{ ch: Partial<NotificationChannel>; config: Record<string, unknown>; isNew: boolean } | null>(null);
  const [error, setError] = useState('');
  const [info, setInfo] = useState('');
  const [loading, setLoading] = useState(false);

  const load = async () => {
    setLoading(true);
    try {
      setChannels(await getNotificationChannels());
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Failed to load channels');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const startNew = (kind: ChannelKind) => {
    const blank: Record<string, unknown> = kind === 'slack'
      ? { webhook_url: '', channel: '' }
      : kind === 'webhook'
      ? { url: '', auth_header: '' }
      : kind === 'email'
      ? { smtp_host: '', smtp_port: 587, smtp_user: '', smtp_password: '', from_addr: '', to_addrs: [], require_tls: true }
      : {};
    setEditing({ ch: { name: '', kind, enabled: true }, config: blank, isNew: true });
  };

  const startEdit = (c: NotificationChannel) => {
    setEditing({ ch: { ...c }, config: { ...c.config_redacted }, isNew: false });
  };

  const save = async () => {
    if (!editing || !editing.ch.name || !editing.ch.kind) {
      setError('Name and kind required');
      return;
    }
    try {
      // For edits, only send config fields the operator actually typed —
      // redacted placeholders shouldn't overwrite the real stored secret.
      const cleanedConfig: Record<string, unknown> = {};
      Object.entries(editing.config).forEach(([k, v]) => {
        if (v === '' || v == null) return;
        if (typeof v === 'string' && v.includes('…')) return; // redacted placeholder
        cleanedConfig[k] = v;
      });
      const body = {
        name: editing.ch.name,
        kind: editing.ch.kind as ChannelKind,
        enabled: editing.ch.enabled ?? true,
        config: cleanedConfig,
      };
      if (editing.isNew) {
        await createNotificationChannel(body);
      } else if (editing.ch.id) {
        await updateNotificationChannel(editing.ch.id, body);
      }
      setEditing(null);
      await load();
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Save failed');
    }
  };

  const remove = async (c: NotificationChannel) => {
    if (!confirm(`Delete channel "${c.name}"?`)) return;
    try {
      await deleteNotificationChannel(c.id);
      await load();
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Delete failed');
    }
  };

  const test = async (c: NotificationChannel) => {
    setInfo('');
    try {
      const r = await testNotificationChannel(c.id, { severity: 'info', summary: `Tantor test → ${c.name}`, description: 'Manual test from the Channels tab.' });
      setInfo(r.success ? `✓ ${c.name}: ${r.message}` : `✗ ${c.name}: ${r.message}`);
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setInfo(`✗ ${c.name}: ${err.response?.data?.detail || 'failed'}`);
    }
  };

  return (
    <div className="space-y-4">
      {admin && (
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-sm text-gray-500">Add channel:</span>
          {(['slack', 'webhook', 'email', 'tantor_internal'] as ChannelKind[]).map((k) => (
            <button key={k} onClick={() => startNew(k)} className="px-3 py-1.5 text-sm border rounded hover:bg-gray-50">
              <Plus size={12} className="inline -mt-0.5 mr-1" /> {KIND_LABELS[k]}
            </button>
          ))}
          <button onClick={load} disabled={loading}
            className="px-3 py-1.5 text-sm border rounded flex items-center gap-1.5 hover:bg-gray-50 disabled:opacity-50 ml-auto">
            <RefreshCw size={14} className={loading ? 'animate-spin' : ''} /> Refresh
          </button>
        </div>
      )}

      {error && <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded text-sm">{error}</div>}
      {info && <div className="bg-blue-50 border border-blue-200 text-blue-700 px-4 py-3 rounded text-sm">{info}</div>}

      <div className="bg-white border rounded overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 text-left text-xs uppercase text-gray-500">
            <tr>
              <th className="px-3 py-2">Name</th>
              <th className="px-3 py-2 w-[120px]">Kind</th>
              <th className="px-3 py-2">Config (redacted)</th>
              <th className="px-3 py-2 w-[80px]">Enabled</th>
              {admin && <th className="px-3 py-2 w-[200px]">Actions</th>}
            </tr>
          </thead>
          <tbody>
            {!channels.length && (
              <tr><td colSpan={admin ? 5 : 4} className="px-3 py-8 text-center text-gray-400">No channels yet.</td></tr>
            )}
            {channels.map((c) => (
              <tr key={c.id} className="border-t hover:bg-gray-50 align-top">
                <td className="px-3 py-2 font-medium">{c.name}</td>
                <td className="px-3 py-2 text-xs">{KIND_LABELS[c.kind]}</td>
                <td className="px-3 py-2">
                  <code className="text-xs text-gray-500 break-all">{JSON.stringify(c.config_redacted)}</code>
                </td>
                <td className="px-3 py-2 text-xs">
                  {c.enabled ? <span className="text-green-700">on</span> : <span className="text-gray-400">off</span>}
                </td>
                {admin && (
                  <td className="px-3 py-2 flex gap-1">
                    <button onClick={() => test(c)} className="px-2 py-1 text-xs border rounded hover:bg-gray-50 flex items-center gap-1">
                      <Send size={12} /> Test
                    </button>
                    <button onClick={() => startEdit(c)} className="px-2 py-1 text-xs border rounded hover:bg-gray-50">Edit</button>
                    <button onClick={() => remove(c)} className="px-2 py-1 text-xs border rounded text-red-600 hover:bg-red-50">
                      <Trash2 size={12} />
                    </button>
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {editing && (
        <ChannelEditor
          state={editing}
          onCancel={() => setEditing(null)}
          onSave={save}
          onChangeChannel={(patch) => setEditing({ ...editing, ch: { ...editing.ch, ...patch } })}
          onChangeConfig={(patch) => setEditing({ ...editing, config: { ...editing.config, ...patch } })}
        />
      )}
    </div>
  );
}

function ChannelEditor({
  state, onCancel, onSave, onChangeChannel, onChangeConfig,
}: {
  state: { ch: Partial<NotificationChannel>; config: Record<string, unknown>; isNew: boolean };
  onCancel: () => void;
  onSave: () => void;
  onChangeChannel: (patch: Partial<NotificationChannel>) => void;
  onChangeConfig: (patch: Record<string, unknown>) => void;
}) {
  const { ch, config, isNew } = state;
  const kind = ch.kind as ChannelKind;
  return (
    <div className="fixed inset-0 bg-black/40 flex items-center justify-center p-4 z-50">
      <div className="bg-white rounded-lg shadow-xl max-w-xl w-full">
        <div className="px-6 py-4 border-b flex items-center justify-between">
          <h3 className="font-semibold">{isNew ? `New ${KIND_LABELS[kind]} channel` : `Edit ${ch.name}`}</h3>
          <button onClick={onCancel} className="text-gray-400 hover:text-gray-600"><X size={18} /></button>
        </div>
        <div className="p-6 space-y-4">
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Name</label>
            <input value={ch.name ?? ''} onChange={(e) => onChangeChannel({ name: e.target.value })}
              className="w-full px-3 py-2 border rounded text-sm" />
          </div>
          <label className="flex items-center gap-2 text-sm">
            <input type="checkbox" checked={ch.enabled ?? true} onChange={(e) => onChangeChannel({ enabled: e.target.checked })} />
            Enabled
          </label>
          {kind === 'slack' && (
            <>
              <Field label="Slack webhook URL"
                value={config.webhook_url as string ?? ''}
                onChange={(v) => onChangeConfig({ webhook_url: v })}
                placeholder="https://hooks.slack.com/services/T.../B.../..." />
              <Field label="Channel override (optional)"
                value={config.channel as string ?? ''}
                onChange={(v) => onChangeConfig({ channel: v })}
                placeholder="#alerts" />
            </>
          )}
          {kind === 'webhook' && (
            <>
              <Field label="URL"
                value={config.url as string ?? ''}
                onChange={(v) => onChangeConfig({ url: v })} />
              <Field label="Authorization header value (optional)"
                value={config.auth_header as string ?? ''}
                onChange={(v) => onChangeConfig({ auth_header: v })}
                placeholder="Bearer ey..." />
            </>
          )}
          {kind === 'email' && (
            <>
              <div className="grid grid-cols-2 gap-3">
                <Field label="SMTP host" value={config.smtp_host as string ?? ''} onChange={(v) => onChangeConfig({ smtp_host: v })} />
                <Field label="SMTP port" type="number" value={String(config.smtp_port ?? 587)} onChange={(v) => onChangeConfig({ smtp_port: parseInt(v || '0', 10) })} />
              </div>
              <div className="grid grid-cols-2 gap-3">
                <Field label="SMTP user" value={config.smtp_user as string ?? ''} onChange={(v) => onChangeConfig({ smtp_user: v })} />
                <Field label="SMTP password" type="password" value={config.smtp_password as string ?? ''} onChange={(v) => onChangeConfig({ smtp_password: v })} />
              </div>
              <Field label="From" value={config.from_addr as string ?? ''} onChange={(v) => onChangeConfig({ from_addr: v })} />
              <Field label="To (comma-separated)"
                value={(config.to_addrs as string[] | undefined)?.join(', ') ?? ''}
                onChange={(v) => onChangeConfig({ to_addrs: v.split(',').map((s) => s.trim()).filter(Boolean) })} />
              <label className="flex items-center gap-2 text-sm">
                <input type="checkbox" checked={config.require_tls !== false}
                  onChange={(e) => onChangeConfig({ require_tls: e.target.checked })} />
                Require STARTTLS
              </label>
            </>
          )}
          {kind === 'tantor_internal' && (
            <p className="text-sm text-gray-500">
              Internal channel — Alertmanager POSTs back to Tantor's webhook receiver. No config required.
            </p>
          )}
        </div>
        <div className="px-6 py-4 border-t flex justify-end gap-2 bg-gray-50">
          <button onClick={onCancel} className="px-4 py-2 text-sm border rounded hover:bg-white">Cancel</button>
          <button onClick={onSave} className="px-4 py-2 text-sm bg-blue-600 text-white rounded hover:bg-blue-700">Save</button>
        </div>
      </div>
    </div>
  );
}

function Field({
  label, value, onChange, placeholder, type = 'text',
}: {
  label: string; value: string; onChange: (v: string) => void; placeholder?: string; type?: string;
}) {
  return (
    <div>
      <label className="block text-xs font-medium text-gray-700 mb-1">{label}</label>
      <input type={type} value={value} onChange={(e) => onChange(e.target.value)} placeholder={placeholder}
        className="w-full px-3 py-2 border rounded text-sm" />
    </div>
  );
}
