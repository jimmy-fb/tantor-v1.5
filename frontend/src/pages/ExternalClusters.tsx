import { useEffect, useState } from 'react';
import { Plug, RefreshCw, Plus, Trash2, X, Check, Wifi, AlertCircle, Loader2 } from 'lucide-react';
import {
  listExternalClusters, createExternalCluster, updateExternalCluster, deleteExternalCluster,
  testExternalUnsaved, testExternalSaved, externalListTopics,
  type ExternalCluster, type SecurityProtocol, type SaslMechanism, type ExternalConnectionTestResult,
} from '../lib/api';
import { isAdmin } from '../lib/auth';

const PROTOCOLS: SecurityProtocol[] = ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'];
const SASL_MECHANISMS: SaslMechanism[] = ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'OAUTHBEARER', 'GSSAPI'];

interface FormState {
  id?: string;
  name: string;
  bootstrap_servers: string;
  security_protocol: SecurityProtocol;
  sasl_mechanism: SaslMechanism | null;
  ssl_verify: boolean;
  sasl_username: string;
  sasl_password: string;
  ssl_ca_pem: string;
  ssl_cert_pem: string;
  ssl_key_pem: string;
}

const blankForm = (): FormState => ({
  name: '',
  bootstrap_servers: '',
  security_protocol: 'PLAINTEXT',
  sasl_mechanism: null,
  ssl_verify: true,
  sasl_username: '',
  sasl_password: '',
  ssl_ca_pem: '',
  ssl_cert_pem: '',
  ssl_key_pem: '',
});

const validateBootstrapServers = (value: string): string | null => {
  const servers = value.split(',').map((s) => s.trim()).filter(Boolean);
  if (!servers.length) {
    return 'Bootstrap servers are required';
  }

  for (const server of servers) {
    if (!server.includes(':')) {
      return `Bootstrap server "${server}" must include a port, for example ${server}:9092`;
    }
    if (server.includes(':') && server.split(':').length > 2 && !server.startsWith('[')) {
      return `Bootstrap server "${server}" must use [ipv6]:port format for IPv6 addresses`;
    }

    const [host, port] = server.split(/:(?=[^:]*$)/);
    if (server.startsWith('[') && !host.endsWith(']')) {
      return `Bootstrap server "${server}" must use [ipv6]:port format for IPv6 addresses`;
    }
    const portNumber = Number(port);
    if (!host || !port || !/^\d+$/.test(port) || portNumber < 1 || portNumber > 65535) {
      return `Bootstrap server "${server}" must include a valid port between 1 and 65535`;
    }
  }

  return null;
};

export default function ExternalClustersPage() {
  const admin = isAdmin();
  const [list, setList] = useState<ExternalCluster[]>([]);
  const [loading, setLoading] = useState(false);
  const [editing, setEditing] = useState<FormState | null>(null);
  const [error, setError] = useState('');
  const [info, setInfo] = useState('');
  const [topicsByCluster, setTopicsByCluster] = useState<Record<string, string[]>>({});
  const [testingCluster, setTestingCluster] = useState<string | null>(null);
  const [listingCluster, setListingCluster] = useState<string | null>(null);

  const reload = async () => {
    setLoading(true);
    setError('');
    try {
      const [data] = await Promise.all([
        listExternalClusters(),
        new Promise(r => setTimeout(r, 500)) // Ensure spinner is visible for at least 500ms
      ]);
      setList(data);
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Failed to load');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { reload(); }, []);

  const startNew = () => setEditing(blankForm());

  const startEdit = (c: ExternalCluster) => {
    setEditing({
      id: c.id,
      name: c.name,
      bootstrap_servers: c.bootstrap_servers ?? '',
      security_protocol: c.security_protocol,
      sasl_mechanism: c.sasl_mechanism,
      ssl_verify: c.ssl_verify,
      sasl_username: c.sasl_username ?? '',
      sasl_password: '',
      ssl_ca_pem: '',
      ssl_cert_pem: '',
      ssl_key_pem: '',
    });
  };

  const onSave = async () => {
    if (!editing) return;
    if (!editing.name.trim()) {
      setError('Display name is required');
      return;
    }
    const bootstrapError = validateBootstrapServers(editing.bootstrap_servers);
    if (bootstrapError) {
      setError(bootstrapError);
      return;
    }
    setError('');
    try {
      const secrets: Record<string, string> = {};
      if (editing.sasl_username) secrets.sasl_username = editing.sasl_username;
      if (editing.sasl_password) secrets.sasl_password = editing.sasl_password;
      if (editing.ssl_ca_pem) secrets.ssl_ca_pem = editing.ssl_ca_pem;
      if (editing.ssl_cert_pem) secrets.ssl_cert_pem = editing.ssl_cert_pem;
      if (editing.ssl_key_pem) secrets.ssl_key_pem = editing.ssl_key_pem;
      const body = {
        name: editing.name.trim(),
        bootstrap_servers: editing.bootstrap_servers.trim(),
        security_protocol: editing.security_protocol,
        sasl_mechanism: editing.sasl_mechanism,
        ssl_verify: editing.ssl_verify,
        secrets,
      };
      if (editing.id) {
        await updateExternalCluster(editing.id, body);
      } else {
        await createExternalCluster(body);
      }
      setEditing(null);
      await reload();
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Save failed');
    }
  };

  const onTestUnsaved = async () => {
    if (!editing) return;
    setInfo('');
    setError('');
    try {
      const secrets: Record<string, string> = {};
      if (editing.sasl_username) secrets.sasl_username = editing.sasl_username;
      if (editing.sasl_password) secrets.sasl_password = editing.sasl_password;
      if (editing.ssl_ca_pem) secrets.ssl_ca_pem = editing.ssl_ca_pem;
      if (editing.ssl_cert_pem) secrets.ssl_cert_pem = editing.ssl_cert_pem;
      if (editing.ssl_key_pem) secrets.ssl_key_pem = editing.ssl_key_pem;
      const r: ExternalConnectionTestResult = await testExternalUnsaved({
        bootstrap_servers: editing.bootstrap_servers.trim(),
        security_protocol: editing.security_protocol,
        sasl_mechanism: editing.sasl_mechanism,
        ssl_verify: editing.ssl_verify,
        secrets,
      });
      setInfo(r.success
        ? `✓ ${r.message}${r.cluster_id ? ` · cluster_id=${r.cluster_id}` : ''}${r.controller_id != null ? ` · controller=${r.controller_id}` : ''}`
        : `✗ ${r.message}`);
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Test failed');
    }
  };

  const onTestSaved = async (c: ExternalCluster) => {
    setTestingCluster(c.id);
    setInfo('');
    try {
      const r = await testExternalSaved(c.id);
      setInfo(r.success ? `✓ ${c.name}: ${r.message}` : `✗ ${c.name}: ${r.message}`);
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setInfo(`✗ ${c.name}: ${err.response?.data?.detail || 'failed'}`);
    } finally {
      setTestingCluster(null);
    }
  };

  const onListTopics = async (c: ExternalCluster) => {
    setListingCluster(c.id);
    try {
      const tps = await externalListTopics(c.id);
      setTopicsByCluster({ ...topicsByCluster, [c.id]: tps.map((t) => t.name) });
      setInfo(`✓ ${c.name}: ${tps.length} topic(s) loaded`);
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setInfo(`✗ ${c.name} list_topics: ${err.response?.data?.detail || 'failed'}`);
    } finally {
      setListingCluster(null);
    }
  };

  const onDelete = async (c: ExternalCluster) => {
    if (!confirm(`Remove external cluster "${c.name}"?`)) return;
    try {
      await deleteExternalCluster(c.id);
      await reload();
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Delete failed');
    }
  };

  return (
    <div className="p-8 max-w-7xl mx-auto">
      <div className="flex items-start justify-between mb-6">
        <div>
          <h1 className="text-2xl font-semibold flex items-center gap-2">
            <Plug size={24} /> External Clusters
          </h1>
          <p className="text-sm text-gray-500 mt-1">
            Connect to existing Kafka clusters Tantor didn't deploy. Supports PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
          </p>
        </div>
        <div className="flex gap-2">
          {admin && (
            <button onClick={startNew}
              className="px-3 py-2 text-sm bg-blue-600 text-white rounded flex items-center gap-1.5 hover:bg-blue-700">
              <Plus size={14} /> Connect cluster
            </button>
          )}
          <button onClick={reload} disabled={loading}
            className="px-3 py-2 text-sm border rounded flex items-center gap-2 hover:bg-gray-50 disabled:opacity-50">
            <RefreshCw size={14} className={loading ? 'animate-spin' : ''} /> Refresh
          </button>
        </div>
      </div>

      {error && <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded text-sm mb-4">{error}</div>}
      {info && <div className="bg-blue-50 border border-blue-200 text-blue-700 px-4 py-3 rounded text-sm mb-4 break-all">{info}</div>}

      <div className="bg-white border rounded overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 text-left text-xs uppercase text-gray-500">
            <tr>
              <th className="px-3 py-2">Name</th>
              <th className="px-3 py-2">Bootstrap servers</th>
              <th className="px-3 py-2 w-[140px]">Protocol</th>
              <th className="px-3 py-2 w-[140px]">SASL</th>
              <th className="px-3 py-2 w-[80px]">Verify</th>
              {admin && <th className="px-3 py-2 w-[260px]">Actions</th>}
            </tr>
          </thead>
          <tbody>
            {!list.length && (
              <tr><td colSpan={admin ? 6 : 5} className="px-3 py-8 text-center text-gray-400">No external clusters connected.</td></tr>
            )}
            {list.map((c) => (
              <tr key={c.id} className="border-t hover:bg-gray-50 align-top">
                <td className="px-3 py-2 font-medium">{c.name}</td>
                <td className="px-3 py-2 font-mono text-xs break-all">{c.bootstrap_servers}</td>
                <td className="px-3 py-2 text-xs">{c.security_protocol}</td>
                <td className="px-3 py-2 text-xs">
                  {c.sasl_mechanism ? `${c.sasl_mechanism} as ${c.sasl_username ?? '?'} ${c.sasl_password_set ? '🔐' : ''}` : '—'}
                </td>
                <td className="px-3 py-2 text-xs">{c.ssl_verify ? 'on' : 'off'}</td>
                {admin && (
                  <td className="px-3 py-2 flex gap-1 flex-wrap">
                    <button
                      onClick={() => onTestSaved(c)}
                      disabled={testingCluster === c.id}
                      className="px-2 py-1 text-xs border rounded hover:bg-gray-50 disabled:opacity-50 flex items-center gap-1"
                    >
                      {testingCluster === c.id ? <Loader2 size={12} className="animate-spin" /> : <Wifi size={12} />}
                      {testingCluster === c.id ? 'Testing…' : 'Test'}
                    </button>
                    <button
                      onClick={() => onListTopics(c)}
                      disabled={listingCluster === c.id}
                      className="px-2 py-1 text-xs border rounded hover:bg-gray-50 disabled:opacity-50 flex items-center gap-1"
                    >
                      {listingCluster === c.id && <Loader2 size={12} className="animate-spin" />}
                      {listingCluster === c.id ? 'Loading…' : 'List topics'}
                    </button>
                    <button onClick={() => startEdit(c)} className="px-2 py-1 text-xs border rounded hover:bg-gray-50">
                      Edit
                    </button>
                    <button onClick={() => onDelete(c)} className="px-2 py-1 text-xs border rounded text-red-600 hover:bg-red-50">
                      <Trash2 size={12} />
                    </button>
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {Object.keys(topicsByCluster).length > 0 && (
        <div className="mt-4 bg-white border rounded p-4">
          <h2 className="text-sm font-semibold mb-2">Recent topic listings</h2>
          {Object.entries(topicsByCluster).map(([cid, tps]) => {
            const c = list.find((x) => x.id === cid);
            return (
              <div key={cid} className="mb-3">
                <div className="text-xs text-gray-500 mb-1">{c?.name ?? cid} · {tps.length} topic(s)</div>
                <div className="flex flex-wrap gap-1">
                  {tps.length === 0 && <span className="text-xs text-gray-400">No topics</span>}
                  {tps.map((t) => (
                    <code key={t} className="text-xs bg-gray-100 border rounded px-1.5 py-0.5">{t}</code>
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {editing && (
        <ConnectModal
          form={editing}
          error={error}
          onChange={(p) => { setError(''); setEditing({ ...editing, ...p }); }}
          onClose={() => { setEditing(null); setError(''); setInfo(''); }}
          onSave={onSave}
          onTest={onTestUnsaved}
        />
      )}
    </div>
  );
}

function ConnectModal({
  form, error, onChange, onClose, onSave, onTest,
}: {
  form: FormState;
  error: string;
  onChange: (p: Partial<FormState>) => void;
  onClose: () => void;
  onSave: () => void;
  onTest: () => void;
}) {
  const isSasl = form.security_protocol.startsWith('SASL_');
  const isSsl = form.security_protocol.endsWith('SSL');
  return (
    <div className="fixed inset-0 bg-black/40 flex items-center justify-center p-4 z-50">
      <div className="bg-white rounded-lg shadow-xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        <div className="px-6 py-4 border-b flex items-center justify-between">
          <h3 className="font-semibold">{form.id ? 'Edit external cluster' : 'Connect external cluster'}</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600"><X size={18} /></button>
        </div>
        <div className="p-6 space-y-3">
          {error && (
            <div className="bg-red-50 border border-red-200 text-red-700 px-3 py-2 rounded text-sm">
              {error}
            </div>
          )}
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Display name</label>
            <input value={form.name} onChange={(e) => onChange({ name: e.target.value })}
              className="w-full px-3 py-2 border rounded text-sm" placeholder="Production prod-east" />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Bootstrap servers</label>
            <input value={form.bootstrap_servers} onChange={(e) => onChange({ bootstrap_servers: e.target.value })}
              className="w-full px-3 py-2 border rounded text-sm font-mono"
              placeholder="broker-1.example.com:9092,broker-2.example.com:9092" />
            <p className="mt-1 text-[11px] text-gray-500">Each server must include a port, for example broker.example.com:9092.</p>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs font-medium text-gray-700 mb-1">Security protocol</label>
              <select value={form.security_protocol}
                onChange={(e) => onChange({ security_protocol: e.target.value as SecurityProtocol })}
                className="w-full px-3 py-2 border rounded text-sm">
                {PROTOCOLS.map((p) => <option key={p} value={p}>{p}</option>)}
              </select>
            </div>
            {isSasl && (
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">SASL mechanism</label>
                <select value={form.sasl_mechanism ?? 'PLAIN'}
                  onChange={(e) => onChange({ sasl_mechanism: e.target.value as SaslMechanism })}
                  className="w-full px-3 py-2 border rounded text-sm">
                  {SASL_MECHANISMS.map((m) => <option key={m} value={m}>{m}</option>)}
                </select>
              </div>
            )}
          </div>

          {isSasl && (
            <div className="border-l-4 border-blue-200 pl-3 py-2 space-y-2">
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-xs font-medium text-gray-700 mb-1">SASL username</label>
                  <input value={form.sasl_username} onChange={(e) => onChange({ sasl_username: e.target.value })}
                    className="w-full px-3 py-2 border rounded text-sm" />
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-700 mb-1">
                    SASL password {form.id && <span className="text-gray-400">(leave blank to keep)</span>}
                  </label>
                  <input type="password" value={form.sasl_password} onChange={(e) => onChange({ sasl_password: e.target.value })}
                    className="w-full px-3 py-2 border rounded text-sm" />
                </div>
              </div>
            </div>
          )}

          {isSsl && (
            <div className="border-l-4 border-emerald-200 pl-3 py-2 space-y-2">
              <label className="flex items-center gap-2 text-sm text-gray-700 cursor-pointer">
                <input type="checkbox" checked={form.ssl_verify}
                  onChange={(e) => onChange({ ssl_verify: e.target.checked })} />
                Verify server certificate
              </label>
              {!form.ssl_verify && (
                <div className="text-xs text-amber-700 bg-amber-50 border border-amber-200 rounded px-2 py-1.5 flex items-center gap-1.5">
                  <AlertCircle size={12} /> SSL verification OFF — vulnerable to MITM. Use only for dev clusters.
                </div>
              )}
              <PemField
                label={`CA certificate (server) ${form.id ? '(leave blank to keep)' : '(optional)'}`}
                value={form.ssl_ca_pem}
                onChange={(v) => onChange({ ssl_ca_pem: v })}
                accept=".pem,.crt,.cer"
              />
              <PemField
                label={`Client certificate ${form.id ? '(leave blank to keep)' : '(mTLS only)'}`}
                value={form.ssl_cert_pem}
                onChange={(v) => onChange({ ssl_cert_pem: v })}
                accept=".pem,.crt,.cer"
              />
              <PemField
                label={`Client private key ${form.id ? '(leave blank to keep)' : '(mTLS only)'}`}
                value={form.ssl_key_pem}
                onChange={(v) => onChange({ ssl_key_pem: v })}
                accept=".pem,.key"
              />
              <div className="text-[11px] text-gray-500 italic">
                Tip: paste PEM content into the textarea, or click <strong>Upload</strong> to load a .pem / .crt / .key file from disk.
                JKS keystores aren't supported directly — convert with <code className="bg-gray-100 px-1 rounded">keytool -importkeystore ... -deststoretype PKCS12</code> then <code className="bg-gray-100 px-1 rounded">openssl pkcs12 -in ... -out ...pem</code>.
              </div>
            </div>
          )}
        </div>
        <div className="px-6 py-4 border-t flex justify-between bg-gray-50">
          <button onClick={onTest} className="px-4 py-2 text-sm border rounded hover:bg-white flex items-center gap-2">
            <Check size={14} /> Test connection
          </button>
          <div className="flex gap-2">
            <button onClick={onClose} className="px-4 py-2 text-sm border rounded hover:bg-white">Cancel</button>
            <button onClick={onSave} className="px-4 py-2 text-sm bg-blue-600 text-white rounded hover:bg-blue-700">
              Save
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}


function PemField({
  label, value, onChange, accept,
}: {
  label: string; value: string; onChange: (v: string) => void; accept: string;
}) {
  // PEM-aware textarea + "Upload" button. The file is read in the operator's
  // browser via FileReader so its bytes never leave their machine until they
  // hit Save (and even then are encrypted at rest by the backend's Fernet).
  const fileInputId = `pem-file-${label.replace(/[^a-z0-9]/gi, '-').toLowerCase()}`;
  const onFile = (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0];
    if (!f) return;
    const reader = new FileReader();
    reader.onload = () => {
      const text = String(reader.result || '');
      onChange(text);
    };
    reader.readAsText(f);
    // reset so the same file can be picked again
    e.target.value = '';
  };
  const isPem = value.includes('-----BEGIN');
  return (
    <div>
      <div className="flex items-center justify-between mb-1">
        <label className="block text-xs font-medium text-gray-700">{label}</label>
        <label htmlFor={fileInputId} className="text-xs text-blue-600 hover:underline cursor-pointer">
          Upload {accept.split(',')[0]} file
        </label>
        <input id={fileInputId} type="file" accept={accept} className="hidden" onChange={onFile} />
      </div>
      <textarea rows={3} value={value} onChange={(e) => onChange(e.target.value)}
        placeholder={`-----BEGIN CERTIFICATE-----\n…\n-----END CERTIFICATE-----`}
        className="w-full px-3 py-2 border rounded text-xs font-mono" />
      {value && (
        <div className="text-[11px] mt-1 flex items-center gap-2">
          <span className={isPem ? 'text-green-700' : 'text-amber-700'}>
            {isPem ? '✓ looks like PEM' : '⚠ no -----BEGIN header detected'}
          </span>
          <span className="text-gray-500">{value.length} chars</span>
        </div>
      )}
    </div>
  );
}
