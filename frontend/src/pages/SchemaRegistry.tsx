import { useEffect, useMemo, useState } from 'react';
import { Database, RefreshCw, Plus, Trash2, X, Check } from 'lucide-react';
import {
  getClusters,
  getRegistryHealth, getSubjects, getVersions, getSchemaVersion,
  registerSchema, deleteSubject, getGlobalCompat, setGlobalCompat,
  type SchemaVersion, type CompatibilityLevel, type SchemaType, type RegistryHealth,
} from '../lib/api';
import { isAdmin } from '../lib/auth';

const COMPAT_LEVELS: CompatibilityLevel[] = [
  'BACKWARD', 'BACKWARD_TRANSITIVE', 'FORWARD', 'FORWARD_TRANSITIVE',
  'FULL', 'FULL_TRANSITIVE', 'NONE',
];

interface ClusterOpt { id: string; name: string }

export default function SchemaRegistryPage() {
  const admin = isAdmin();
  const [clusters, setClusters] = useState<ClusterOpt[]>([]);
  const [clusterId, setClusterId] = useState('');
  const [health, setHealth] = useState<RegistryHealth | null>(null);
  const [subjects, setSubjects] = useState<string[]>([]);
  const [selectedSubject, setSelectedSubject] = useState<string | null>(null);
  const [versions, setVersions] = useState<number[]>([]);
  const [activeVersion, setActiveVersion] = useState<SchemaVersion | null>(null);
  const [compat, setCompat] = useState<CompatibilityLevel>('BACKWARD');
  const [showRegister, setShowRegister] = useState(false);
  const [error, setError] = useState('');
  const [info, setInfo] = useState('');
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    getClusters().then((cs) => {
      const opts = cs.filter((c) => c.kind !== 'external').map((c) => ({ id: c.id, name: c.name }));
      setClusters(opts);
      if (opts.length && !clusterId) setClusterId(opts[0].id);
    }).catch(() => setClusters([]));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const reload = useMemo(() => async () => {
    if (!clusterId) return;
    setLoading(true);
    setError('');
    try {
      const [h, subs, c] = await Promise.all([
        getRegistryHealth(clusterId),
        getRegistryHealth(clusterId).then((r) => r.reachable ? getSubjects(clusterId) : []),
        getRegistryHealth(clusterId).then((r) => r.reachable ? getGlobalCompat(clusterId) : null),
      ]);
      setHealth(h);
      setSubjects(subs);
      if (c) setCompat(c.compatibility);
      if (subs.length && !selectedSubject) setSelectedSubject(subs[0]);
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Failed to load registry');
    } finally {
      setLoading(false);
    }
  }, [clusterId, selectedSubject]);

  useEffect(() => { reload(); }, [reload]);

  useEffect(() => {
    if (!clusterId || !selectedSubject) {
      setVersions([]); setActiveVersion(null); return;
    }
    getVersions(clusterId, selectedSubject).then(async (vs) => {
      setVersions(vs);
      if (vs.length) {
        setActiveVersion(await getSchemaVersion(clusterId, selectedSubject, vs[vs.length - 1]));
      } else {
        setActiveVersion(null);
      }
    }).catch(() => setVersions([]));
  }, [clusterId, selectedSubject]);

  const onSelectVersion = async (v: number) => {
    if (!clusterId || !selectedSubject) return;
    setActiveVersion(await getSchemaVersion(clusterId, selectedSubject, v));
  };

  const onDeleteSubject = async (subject: string) => {
    if (!clusterId) return;
    if (!confirm(`Delete subject "${subject}" and all its versions?`)) return;
    try {
      await deleteSubject(clusterId, subject);
      setSelectedSubject(null);
      await reload();
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Delete failed');
    }
  };

  const onUpdateCompat = async (level: CompatibilityLevel) => {
    if (!clusterId) return;
    try {
      const r = await setGlobalCompat(clusterId, level);
      setCompat(r.compatibility);
      setInfo(`Compatibility set to ${r.compatibility}`);
      setTimeout(() => setInfo(''), 3000);
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Failed to update compatibility');
    }
  };

  return (
    <div className="p-8 max-w-7xl mx-auto">
      <div className="flex items-start justify-between mb-6 gap-4">
        <div>
          <h1 className="text-2xl font-semibold flex items-center gap-2">
            <Database size={24} /> Schema Registry
          </h1>
          <p className="text-sm text-gray-500 mt-1">
            Apicurio Registry, ccompat-v7 endpoint — wire-compatible with Confluent Schema Registry.
          </p>
        </div>
        <div className="flex gap-2 items-end">
          <div>
            <label className="block text-xs text-gray-500 mb-1">Cluster</label>
            <select value={clusterId} onChange={(e) => { setClusterId(e.target.value); setSelectedSubject(null); }}
              disabled={!clusters.length} className="px-3 py-2 border rounded text-sm min-w-[200px]">
              {!clusters.length && <option>No managed clusters</option>}
              {clusters.map((c) => <option key={c.id} value={c.id}>{c.name}</option>)}
            </select>
          </div>
          <button onClick={reload} disabled={loading}
            className="px-3 py-2 text-sm border rounded hover:bg-gray-50 flex items-center gap-2 disabled:opacity-50">
            <RefreshCw size={14} className={loading ? 'animate-spin' : ''} /> Refresh
          </button>
        </div>
      </div>

      {!clusterId ? (
        <div className="bg-white border rounded p-8 text-center text-gray-500">
          Deploy a managed Kafka cluster with the Schema Registry role first.
        </div>
      ) : (
        <>
          <div className="mb-4">
            {health?.reachable ? (
              <span className="text-sm text-green-700 flex items-center gap-1.5">
                <Check size={14} /> Registry connected
                {health.url && <span className="text-gray-400 font-mono text-xs">({health.url})</span>}
                {health.subject_count !== null && <span className="text-gray-500">· {health.subject_count} subject(s)</span>}
              </span>
            ) : (
              <span className="text-sm text-amber-700">
                Schema Registry not reachable on this cluster — add the schema_registry role and redeploy.
              </span>
            )}
          </div>

          {error && <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded text-sm mb-4">{error}</div>}
          {info && <div className="bg-blue-50 border border-blue-200 text-blue-700 px-4 py-3 rounded text-sm mb-4">{info}</div>}

          <div className="flex items-center gap-2 mb-4 flex-wrap">
            {admin && (
              <button onClick={() => setShowRegister(true)} disabled={!health?.reachable}
                className="px-3 py-1.5 text-sm bg-blue-600 text-white rounded flex items-center gap-1.5 hover:bg-blue-700 disabled:opacity-50">
                <Plus size={14} /> Register schema
              </button>
            )}
            <span className="text-sm text-gray-500 ml-4">Global compatibility:</span>
            <select value={compat} onChange={(e) => onUpdateCompat(e.target.value as CompatibilityLevel)}
              disabled={!admin || !health?.reachable} className="px-2 py-1 border rounded text-sm">
              {COMPAT_LEVELS.map((l) => <option key={l} value={l}>{l}</option>)}
            </select>
          </div>

          <div className="grid grid-cols-12 gap-4">
            <div className="col-span-4 bg-white border rounded">
              <div className="px-3 py-2 border-b text-xs uppercase tracking-wide text-gray-500 font-semibold">
                Subjects ({subjects.length})
              </div>
              <ul className="divide-y max-h-[600px] overflow-y-auto">
                {!subjects.length && <li className="px-4 py-6 text-center text-gray-400 text-sm">No subjects yet.</li>}
                {subjects.map((s) => (
                  <li key={s}
                    onClick={() => setSelectedSubject(s)}
                    className={`px-3 py-2 cursor-pointer text-sm flex items-center justify-between ${
                      selectedSubject === s ? 'bg-blue-50' : 'hover:bg-gray-50'
                    }`}>
                    <span className="font-mono">{s}</span>
                    {admin && (
                      <button onClick={(e) => { e.stopPropagation(); onDeleteSubject(s); }}
                        className="text-gray-400 hover:text-red-600">
                        <Trash2 size={12} />
                      </button>
                    )}
                  </li>
                ))}
              </ul>
            </div>

            <div className="col-span-8 bg-white border rounded">
              <div className="px-3 py-2 border-b flex items-center justify-between">
                <div className="text-xs uppercase tracking-wide text-gray-500 font-semibold">
                  {selectedSubject ?? 'Select a subject'}
                </div>
                {selectedSubject && versions.length > 0 && (
                  <select onChange={(e) => onSelectVersion(parseInt(e.target.value, 10))}
                    value={activeVersion?.version ?? ''} className="px-2 py-1 border rounded text-xs">
                    {versions.map((v) => <option key={v} value={v}>v{v}</option>)}
                  </select>
                )}
              </div>
              <div className="p-4">
                {!activeVersion ? (
                  <p className="text-gray-400 text-sm">No schema selected.</p>
                ) : (
                  <>
                    <div className="flex gap-4 text-xs text-gray-600 mb-3">
                      <span>id <code>{activeVersion.id}</code></span>
                      <span>version <code>{activeVersion.version}</code></span>
                      <span>type <code>{activeVersion.schema_type ?? 'AVRO'}</code></span>
                    </div>
                    <pre className="bg-gray-50 border rounded p-3 text-xs overflow-x-auto whitespace-pre-wrap break-all">
                      {(() => {
                        try { return JSON.stringify(JSON.parse(activeVersion.schema_text), null, 2); }
                        catch { return activeVersion.schema_text; }
                      })()}
                    </pre>
                  </>
                )}
              </div>
            </div>
          </div>

          {showRegister && (
            <RegisterModal
              clusterId={clusterId}
              onClose={() => setShowRegister(false)}
              onSaved={async (subject) => {
                setShowRegister(false);
                await reload();
                setSelectedSubject(subject);
              }}
            />
          )}
        </>
      )}
    </div>
  );
}

function RegisterModal({
  clusterId, onClose, onSaved,
}: { clusterId: string; onClose: () => void; onSaved: (subject: string) => void }) {
  const [subject, setSubject] = useState('');
  const [schemaText, setSchemaText] = useState(
    '{\n  "type": "record",\n  "name": "User",\n  "fields": [\n    {"name": "id", "type": "long"},\n    {"name": "name", "type": "string"}\n  ]\n}'
  );
  const [schemaType, setSchemaType] = useState<SchemaType>('AVRO');
  const [error, setError] = useState('');
  const [saving, setSaving] = useState(false);

  const submit = async () => {
    if (!subject.trim() || !schemaText.trim()) {
      setError('Subject and schema text are required');
      return;
    }
    setSaving(true);
    try {
      await registerSchema(clusterId, subject.trim(), schemaText, schemaType);
      onSaved(subject.trim());
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Register failed');
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/40 flex items-center justify-center p-4 z-50">
      <div className="bg-white rounded-lg shadow-xl max-w-2xl w-full">
        <div className="px-6 py-4 border-b flex items-center justify-between">
          <h3 className="font-semibold">Register schema</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600"><X size={18} /></button>
        </div>
        <div className="p-6 space-y-3">
          {error && <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-2 rounded text-sm">{error}</div>}
          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-2">
              <label className="block text-xs font-medium text-gray-700 mb-1">Subject</label>
              <input value={subject} onChange={(e) => setSubject(e.target.value)}
                placeholder="orders-value" className="w-full px-3 py-2 border rounded text-sm" />
              <p className="text-xs text-gray-400 mt-1">Convention: <code>&lt;topic&gt;-key</code> or <code>&lt;topic&gt;-value</code>.</p>
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-700 mb-1">Type</label>
              <select value={schemaType} onChange={(e) => setSchemaType(e.target.value as SchemaType)}
                className="w-full px-3 py-2 border rounded text-sm">
                <option>AVRO</option>
                <option>JSON</option>
                <option>PROTOBUF</option>
              </select>
            </div>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Schema</label>
            <textarea value={schemaText} onChange={(e) => setSchemaText(e.target.value)}
              rows={14} className="w-full px-3 py-2 border rounded text-xs font-mono" />
          </div>
        </div>
        <div className="px-6 py-4 border-t flex justify-end gap-2 bg-gray-50">
          <button onClick={onClose} className="px-4 py-2 text-sm border rounded hover:bg-white">Cancel</button>
          <button onClick={submit} disabled={saving}
            className="px-4 py-2 text-sm bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50">
            {saving ? 'Registering…' : 'Register'}
          </button>
        </div>
      </div>
    </div>
  );
}
