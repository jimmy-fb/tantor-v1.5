/**
 * Per-cluster Schema Registry tab (APB v1.4.0 #2).
 *
 * If the cluster already has an SR Service row, render the subjects list
 * with quick "register schema" + "view version" actions. If not, render
 * a deploy form (host picker + port) that POSTs to the SR-deploy endpoint
 * and polls the resulting deploy task.
 *
 * Until 1.4.0 Schema Registry lived as a global sidebar page; the
 * customer pointed out that schemas are inherently tied to one cluster's
 * bootstrap servers — this tab brings the management surface into the
 * cluster detail page where the operator already is.
 */
import { useState, useEffect, useCallback } from 'react';
import { Database, Plus, Loader2, AlertCircle, RefreshCw, Server, Upload } from 'lucide-react';
import axios from 'axios';
import { getAccessToken } from '../../lib/auth';

interface Props {
  clusterId: string;
  /** Comes from the parent cluster detail page so we can pre-select hosts already in the cluster. */
  clusterHostIds?: string[];
}

interface Host {
  id: string;
  hostname: string;
  ip_address: string;
}

interface Service {
  id: string;
  role: string;
  host_id: string;
  status: string;
  node_id: number;
}

interface Subject {
  name: string;
  versions: number[];
}

const authApi = axios.create({ baseURL: '/api' });
authApi.interceptors.request.use((cfg) => {
  const t = getAccessToken();
  if (t) cfg.headers.Authorization = `Bearer ${t}`;
  return cfg;
});

export default function ClusterSchemaRegistry({ clusterId, clusterHostIds }: Props) {
  const [services, setServices] = useState<Service[]>([]);
  const [hosts, setHosts] = useState<Host[]>([]);
  const [subjects, setSubjects] = useState<string[]>([]);
  const [subjectDetail, setSubjectDetail] = useState<Subject | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  // Deploy form state
  const [deployHost, setDeployHost] = useState('');
  const [deployPort, setDeployPort] = useState(8085);
  const [deploying, setDeploying] = useState(false);
  const [deployTaskId, setDeployTaskId] = useState<string | null>(null);
  const [deployLogs, setDeployLogs] = useState<string[]>([]);

  // Register schema state
  const [showRegister, setShowRegister] = useState(false);
  const [newSubject, setNewSubject] = useState('');
  const [newSchemaType, setNewSchemaType] = useState('AVRO');
  const [newSchemaText, setNewSchemaText] = useState('');
  const [registering, setRegistering] = useState(false);

  const sr = services.find(s => s.role === 'schema_registry');

  const fetchAll = useCallback(async () => {
    setError('');
    try {
      const [{ data: c }, { data: h }] = await Promise.all([
        authApi.get(`/clusters/${clusterId}`),
        authApi.get('/hosts'),
      ]);
      setServices(c.services || []);
      setHosts(h);
      const srSvc = (c.services || []).find((s: Service) => s.role === 'schema_registry');
      if (srSvc) {
        try {
          const { data: subs } = await authApi.get<string[]>(`/clusters/${clusterId}/schema-registry/subjects`);
          setSubjects(subs);
        } catch {
          setSubjects([]);
        }
      }
    } catch (e: unknown) {
      const ax = e as { response?: { data?: { detail?: string } } };
      setError(ax.response?.data?.detail || 'Failed to load Schema Registry status');
    } finally {
      setLoading(false);
    }
  }, [clusterId]);

  useEffect(() => { fetchAll(); }, [fetchAll]);

  // Poll deploy task
  useEffect(() => {
    if (!deployTaskId) return;
    const interval = setInterval(async () => {
      try {
        const { data } = await authApi.get(`/clusters/${clusterId}/deploy/${deployTaskId}`);
        setDeployLogs(data.logs || []);
        if (data.status !== 'running') {
          clearInterval(interval);
          setDeployTaskId(null);
          setDeploying(false);
          fetchAll();
        }
      } catch {
        clearInterval(interval);
        setDeploying(false);
      }
    }, 2000);
    return () => clearInterval(interval);
  }, [deployTaskId, clusterId, fetchAll]);

  const handleDeploy = async () => {
    if (!deployHost) {
      setError('Pick a host to install Schema Registry on');
      return;
    }
    setDeploying(true);
    setDeployLogs([]);
    setError('');
    try {
      const { data } = await authApi.post(`/clusters/${clusterId}/services/schema-registry`, {
        host_id: deployHost,
        port: deployPort,
      });
      setDeployTaskId(data.task_id);
    } catch (e: unknown) {
      const ax = e as { response?: { data?: { detail?: string } } };
      setError(ax.response?.data?.detail || 'Deploy failed');
      setDeploying(false);
    }
  };

  const handleRegister = async () => {
    if (!newSubject.trim() || !newSchemaText.trim()) {
      setError('Subject name and schema body are both required');
      return;
    }
    setRegistering(true);
    setError('');
    try {
      await authApi.post(`/clusters/${clusterId}/schema-registry/subjects/${newSubject}/versions`, {
        schema: newSchemaText,
        schema_type: newSchemaType,
      });
      setShowRegister(false);
      setNewSubject('');
      setNewSchemaText('');
      fetchAll();
    } catch (e: unknown) {
      const ax = e as { response?: { data?: { detail?: string } } };
      setError(ax.response?.data?.detail || 'Failed to register schema');
    } finally {
      setRegistering(false);
    }
  };

  const viewSubject = async (name: string) => {
    try {
      const { data: versions } = await authApi.get<number[]>(`/clusters/${clusterId}/schema-registry/subjects/${name}/versions`);
      setSubjectDetail({ name, versions });
    } catch {
      setSubjectDetail({ name, versions: [] });
    }
  };

  if (loading && !services.length) {
    return <div className="flex items-center gap-2 text-sm text-gray-500"><Loader2 size={14} className="animate-spin" /> Loading…</div>;
  }

  // Hosts already in this cluster (preferred for SR co-deploy) plus all others
  const clusterHosts = hosts.filter(h => clusterHostIds?.includes(h.id));
  const otherHosts = hosts.filter(h => !clusterHostIds?.includes(h.id));

  return (
    <div className="space-y-4">
      <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-2">
        <Database size={16} className="text-blue-600" /> Schema Registry
      </h3>

      {error && (
        <div className="flex items-center gap-2 bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-sm text-red-700">
          <AlertCircle size={14} /> {error}
        </div>
      )}

      {!sr ? (
        // ── Deploy form ─────────────────────────────────
        <div className="border border-gray-200 rounded-xl p-4">
          <h4 className="text-sm font-medium text-gray-900 mb-1 flex items-center gap-2">
            <Server size={14} /> Schema Registry not yet deployed
          </h4>
          <p className="text-xs text-gray-500 mb-3">
            Tantor will install Apicurio Registry (Confluent-API-compatible) on the host you pick. Schemas are persisted in this cluster's own Kafka — no separate database.
          </p>

          <div className="space-y-3">
            <div>
              <label className="block text-xs font-medium text-gray-700 mb-1">Host</label>
              <select
                value={deployHost}
                onChange={e => setDeployHost(e.target.value)}
                className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm"
              >
                <option value="">— select a host —</option>
                {clusterHosts.length > 0 && (
                  <optgroup label="In this cluster">
                    {clusterHosts.map(h => (
                      <option key={h.id} value={h.id}>{h.hostname} ({h.ip_address})</option>
                    ))}
                  </optgroup>
                )}
                {otherHosts.length > 0 && (
                  <optgroup label="Other registered hosts">
                    {otherHosts.map(h => (
                      <option key={h.id} value={h.id}>{h.hostname} ({h.ip_address})</option>
                    ))}
                  </optgroup>
                )}
              </select>
            </div>

            <div>
              <label className="block text-xs font-medium text-gray-700 mb-1">REST port</label>
              <input
                type="number"
                value={deployPort}
                onChange={e => setDeployPort(parseInt(e.target.value) || 8085)}
                className="w-32 px-3 py-2 border border-gray-200 rounded-lg text-sm"
              />
              <span className="ml-2 text-xs text-gray-500">Confluent-compat endpoint will live at <code>:{deployPort}/apis/ccompat/v7</code></span>
            </div>

            <button
              onClick={handleDeploy}
              disabled={deploying || !deployHost}
              className="px-4 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 disabled:opacity-50 flex items-center gap-2"
            >
              {deploying ? <Loader2 size={14} className="animate-spin" /> : <Upload size={14} />}
              Deploy Schema Registry
            </button>
          </div>

          {deployLogs.length > 0 && (
            <pre className="mt-4 max-h-72 overflow-auto bg-gray-950 text-green-200 text-xs p-3 rounded-lg font-mono">
              {deployLogs.join('\n')}
            </pre>
          )}
        </div>
      ) : (
        // ── Browse subjects ─────────────────────────────
        <>
          <div className="flex items-center gap-3">
            <span className={`px-2 py-0.5 rounded text-xs font-medium ${
              sr.status === 'running' ? 'bg-green-100 text-green-700' : 'bg-amber-100 text-amber-700'
            }`}>
              {sr.status}
            </span>
            <button
              onClick={fetchAll}
              className="text-xs flex items-center gap-1 text-blue-600 hover:underline"
            >
              <RefreshCw size={12} /> Refresh
            </button>
            <button
              onClick={() => setShowRegister(true)}
              className="ml-auto px-3 py-1.5 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 flex items-center gap-1.5"
            >
              <Plus size={14} /> Register schema
            </button>
          </div>

          {subjects.length === 0 ? (
            <div className="border border-dashed border-gray-300 rounded-xl p-8 text-center text-sm text-gray-500">
              No subjects registered yet. Click "Register schema" to add one.
            </div>
          ) : (
            <div className="border border-gray-200 rounded-xl overflow-hidden">
              <ul className="divide-y divide-gray-100">
                {subjects.map(s => (
                  <li key={s} className="px-4 py-2.5 flex items-center justify-between hover:bg-gray-50">
                    <span className="font-mono text-sm">{s}</span>
                    <button
                      onClick={() => viewSubject(s)}
                      className="text-xs text-blue-600 hover:underline"
                    >
                      View versions
                    </button>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {subjectDetail && (
            <div className="border border-gray-200 rounded-xl p-4 bg-blue-50/30">
              <div className="flex items-center justify-between mb-2">
                <h4 className="font-mono text-sm font-medium">{subjectDetail.name}</h4>
                <button onClick={() => setSubjectDetail(null)} className="text-xs text-gray-500">close</button>
              </div>
              <p className="text-xs text-gray-700">
                Versions: {subjectDetail.versions.length === 0 ? '—' : subjectDetail.versions.join(', ')}
              </p>
            </div>
          )}

          {showRegister && (
            <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4" onClick={() => setShowRegister(false)}>
              <div className="bg-white rounded-xl shadow-xl max-w-2xl w-full p-6" onClick={e => e.stopPropagation()}>
                <h3 className="text-lg font-semibold mb-3 flex items-center gap-2">
                  <Plus size={18} /> Register schema
                </h3>
                <div className="space-y-3">
                  <div>
                    <label className="block text-xs font-medium text-gray-700 mb-1">Subject name</label>
                    <input
                      type="text"
                      value={newSubject}
                      onChange={e => setNewSubject(e.target.value)}
                      placeholder="orders-value"
                      className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm font-mono"
                    />
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-gray-700 mb-1">Type</label>
                    <select
                      value={newSchemaType}
                      onChange={e => setNewSchemaType(e.target.value)}
                      className="px-3 py-2 border border-gray-200 rounded-lg text-sm"
                    >
                      <option value="AVRO">AVRO</option>
                      <option value="JSON">JSON</option>
                      <option value="PROTOBUF">PROTOBUF</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-gray-700 mb-1">Schema body</label>
                    <textarea
                      value={newSchemaText}
                      onChange={e => setNewSchemaText(e.target.value)}
                      rows={10}
                      placeholder='{"type": "record", "name": "Order", "fields": [...]}'
                      className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm font-mono"
                    />
                  </div>
                </div>
                <div className="flex justify-end gap-2 mt-4">
                  <button onClick={() => setShowRegister(false)} className="px-4 py-2 text-sm border rounded-lg">Cancel</button>
                  <button
                    onClick={handleRegister}
                    disabled={registering}
                    className="px-4 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 disabled:opacity-50 flex items-center gap-1.5"
                  >
                    {registering && <Loader2 size={14} className="animate-spin" />}
                    Register
                  </button>
                </div>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
