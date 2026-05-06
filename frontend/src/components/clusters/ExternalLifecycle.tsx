import { useEffect, useState } from 'react';
import {
  Play, Square, RotateCw, Save, Plus, Trash2, Loader2,
  AlertCircle, CheckCircle2, XCircle, Server,
} from 'lucide-react';
import { getHosts, getExternalBrokerHosts, setExternalBrokerHosts, externalLifecycleAction } from '../../lib/api';
import type { Host } from '../../types';

type Props = { clusterId: string };

type BrokerHost = {
  host_id: string;
  kafka_unit: string;
  hostname?: string | null;
  ip_address?: string | null;
  online?: boolean;
};

type ActionResult = {
  host_id: string;
  hostname?: string;
  kafka_unit?: string;
  exit_code?: number;
  ok: boolean;
  message: string;
};

/**
 * External cluster lifecycle panel.
 *
 * Lets the operator register SSH-reachable broker hosts (referencing an
 * existing Tantor Host record) plus a systemd unit name, then issue
 * start / stop / restart against that unit on every host. Tantor never
 * touches Kafka data — this is a remote button for `systemctl <action> X`
 * with the customer's own credentials.
 */
export default function ExternalLifecycle({ clusterId }: Props) {
  const [hosts, setHosts] = useState<Host[]>([]);
  const [entries, setEntries] = useState<BrokerHost[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [acting, setActing] = useState<null | 'start' | 'stop' | 'restart'>(null);
  const [results, setResults] = useState<ActionResult[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const refresh = async () => {
    setLoading(true);
    try {
      const [hs, be] = await Promise.all([
        getHosts().catch(() => []),
        getExternalBrokerHosts(clusterId).catch(() => []),
      ]);
      setHosts(hs);
      setEntries(be);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { refresh(); }, [clusterId]);

  const addEntry = () => {
    if (hosts.length === 0) return;
    setEntries([...entries, { host_id: hosts[0].id, kafka_unit: 'kafka.service' }]);
  };

  const removeEntry = (i: number) => {
    setEntries(entries.filter((_, idx) => idx !== i));
  };

  const updateEntry = (i: number, patch: Partial<BrokerHost>) => {
    setEntries(entries.map((e, idx) => idx === i ? { ...e, ...patch } : e));
  };

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      const updated = await setExternalBrokerHosts(
        clusterId,
        entries.map(e => ({ host_id: e.host_id, kafka_unit: e.kafka_unit })),
      );
      setEntries(updated);
    } catch (e: unknown) {
      const apiErr = (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      setError(apiErr || (e instanceof Error ? e.message : 'Save failed'));
    } finally {
      setSaving(false);
    }
  };

  const runAction = async (action: 'start' | 'stop' | 'restart') => {
    setActing(action);
    setResults(null);
    setError(null);
    try {
      const r = await externalLifecycleAction(clusterId, action);
      setResults(r.results);
    } catch (e: unknown) {
      const apiErr = (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      setError(apiErr || (e instanceof Error ? e.message : 'Action failed'));
    } finally {
      setActing(null);
    }
  };

  if (loading) {
    return <div className="flex items-center gap-2 text-sm text-gray-500"><Loader2 size={14} className="animate-spin" /> Loading…</div>;
  }

  return (
    <div className="space-y-5">
      <div>
        <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-2 mb-1">
          <Server size={16} className="text-blue-600" /> Broker SSH hosts
        </h3>
        <p className="text-xs text-gray-500 mb-3">
          Register the SSH-reachable broker hosts and the Kafka systemd unit name.
          Tantor uses these to issue lifecycle actions on the external cluster — same SSH credentials
          you registered on the <a href="/hosts" className="text-blue-600 underline">Hosts</a> page.
        </p>

        {hosts.length === 0 ? (
          <div className="text-sm text-yellow-800 bg-yellow-50 border border-yellow-200 rounded p-3 flex items-start gap-2">
            <AlertCircle size={14} className="mt-0.5 shrink-0" />
            <div>
              No hosts registered yet. Go to <a href="/hosts" className="text-blue-600 underline">Hosts</a> first
              and add SSH credentials for each broker box.
            </div>
          </div>
        ) : (
          <div className="border rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 border-b">
                <tr>
                  <th className="text-left px-3 py-2 font-medium">Host</th>
                  <th className="text-left px-3 py-2 font-medium">systemd unit</th>
                  <th className="text-left px-3 py-2 font-medium">Status</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {entries.length === 0 && (
                  <tr><td colSpan={4} className="px-3 py-4 text-center text-gray-500 italic">No broker hosts registered yet.</td></tr>
                )}
                {entries.map((e, i) => (
                  <tr key={i} className="border-t">
                    <td className="px-3 py-1.5">
                      <select
                        value={e.host_id}
                        onChange={ev => updateEntry(i, { host_id: ev.target.value })}
                        className="px-2 py-1 border rounded text-xs"
                      >
                        {hosts.map(h => (
                          <option key={h.id} value={h.id}>{h.hostname} ({h.ip_address})</option>
                        ))}
                      </select>
                    </td>
                    <td className="px-3 py-1.5">
                      <input
                        value={e.kafka_unit}
                        onChange={ev => updateEntry(i, { kafka_unit: ev.target.value })}
                        placeholder="kafka.service"
                        className="px-2 py-1 border rounded text-xs font-mono w-48"
                      />
                    </td>
                    <td className="px-3 py-1.5 text-xs">
                      {e.online ? <span className="text-green-700">online</span> : <span className="text-gray-500">unknown</span>}
                    </td>
                    <td className="px-3 py-1.5 text-right">
                      <button onClick={() => removeEntry(i)} className="text-red-600 hover:text-red-700">
                        <Trash2 size={14} />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div className="flex justify-between items-center px-3 py-2 bg-gray-50 border-t">
              <button
                onClick={addEntry}
                className="flex items-center gap-1.5 text-xs px-2 py-1 border rounded hover:bg-white"
              >
                <Plus size={12} /> Add host
              </button>
              <button
                onClick={save}
                disabled={saving}
                className="flex items-center gap-1.5 text-xs px-3 py-1 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
              >
                {saving ? <Loader2 size={12} className="animate-spin" /> : <Save size={12} />}
                Save
              </button>
            </div>
          </div>
        )}
      </div>

      <div>
        <h3 className="text-sm font-semibold text-gray-700 mb-2">Lifecycle actions</h3>
        <p className="text-xs text-gray-500 mb-3">
          Tantor SSHes to each broker host and runs <code className="bg-gray-100 px-1 rounded">sudo systemctl &lt;action&gt; &lt;unit&gt;</code>.
          High blast radius — restarting an externally-operated cluster will interrupt traffic. Use with care.
        </p>
        <div className="flex gap-2">
          <button
            onClick={() => runAction('start')}
            disabled={acting !== null || entries.length === 0}
            className="flex items-center gap-2 px-3 py-1.5 bg-green-600 text-white text-sm rounded hover:bg-green-700 disabled:opacity-50"
          >
            {acting === 'start' ? <Loader2 size={14} className="animate-spin" /> : <Play size={14} />}
            Start
          </button>
          <button
            onClick={() => runAction('stop')}
            disabled={acting !== null || entries.length === 0}
            className="flex items-center gap-2 px-3 py-1.5 bg-red-600 text-white text-sm rounded hover:bg-red-700 disabled:opacity-50"
          >
            {acting === 'stop' ? <Loader2 size={14} className="animate-spin" /> : <Square size={14} />}
            Stop
          </button>
          <button
            onClick={() => runAction('restart')}
            disabled={acting !== null || entries.length === 0}
            className="flex items-center gap-2 px-3 py-1.5 bg-amber-600 text-white text-sm rounded hover:bg-amber-700 disabled:opacity-50"
          >
            {acting === 'restart' ? <Loader2 size={14} className="animate-spin" /> : <RotateCw size={14} />}
            Restart
          </button>
        </div>

        {error && (
          <div className="mt-3 text-sm text-red-700 bg-red-50 border border-red-200 rounded p-2 px-3">{error}</div>
        )}

        {results && (
          <div className="mt-3 border rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 border-b">
                <tr>
                  <th className="text-left px-3 py-2 font-medium">Host</th>
                  <th className="text-left px-3 py-2 font-medium">Unit</th>
                  <th className="text-left px-3 py-2 font-medium">Result</th>
                  <th className="text-left px-3 py-2 font-medium">Output</th>
                </tr>
              </thead>
              <tbody>
                {results.map((r, i) => (
                  <tr key={i} className="border-t">
                    <td className="px-3 py-1.5">{r.hostname || r.host_id.slice(0, 8)}</td>
                    <td className="px-3 py-1.5 font-mono text-xs">{r.kafka_unit}</td>
                    <td className="px-3 py-1.5">
                      {r.ok ? (
                        <span className="text-green-700 inline-flex items-center gap-1">
                          <CheckCircle2 size={14} /> ok
                        </span>
                      ) : (
                        <span className="text-red-700 inline-flex items-center gap-1">
                          <XCircle size={14} /> failed
                        </span>
                      )}
                    </td>
                    <td className="px-3 py-1.5 text-xs text-gray-600 font-mono whitespace-pre-wrap break-all max-w-[40ch]">
                      {r.message || '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
