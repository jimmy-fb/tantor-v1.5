import { useEffect, useState } from 'react';
import { Lock, Download, Plus, Trash2, AlertCircle, ShieldCheck, RefreshCw, Copy } from 'lucide-react';
import {
  getTlsState, setTlsState, listClientCerts, issueClientCert, revokeClientCert,
  type TLSState, type ClientCertSummary, type ClientCertBundle,
} from '../../lib/api';
import { isAdmin } from '../../lib/auth';

interface Props { clusterId: string; clusterRunning: boolean }

export default function TLSPanel({ clusterId, clusterRunning }: Props) {
  const admin = isAdmin();
  const [state, setState] = useState<TLSState | null>(null);
  const [certs, setCerts] = useState<ClientCertSummary[]>([]);
  const [error, setError] = useState('');
  const [info, setInfo] = useState('');
  const [issuing, setIssuing] = useState(false);
  const [bundle, setBundle] = useState<ClientCertBundle | null>(null);
  const [newCertCN, setNewCertCN] = useState('');
  const [newCertTtl, setNewCertTtl] = useState(365);

  const reload = async () => {
    try {
      const [s, cs] = await Promise.all([getTlsState(clusterId), listClientCerts(clusterId).catch(() => [])]);
      setState(s); setCerts(cs);
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Failed to load TLS state');
    }
  };

  useEffect(() => { reload(); }, [clusterId]);

  const toggle = async (ssl: boolean, mtls: boolean) => {
    setError(''); setInfo('');
    try {
      const next = await setTlsState(clusterId, { ssl_enabled: ssl, mtls_required: mtls });
      setState(next);
      setInfo(
        ssl
          ? 'TLS enabled. Trigger a redeploy or rolling restart for the cluster to pick up the new SSL listener.'
          : 'TLS disabled. Trigger a redeploy or rolling restart for brokers to drop the SSL listener.',
      );
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Toggle failed');
    }
  };

  const issue = async () => {
    if (!newCertCN.trim()) { setError('Common name is required'); return; }
    setIssuing(true); setError('');
    try {
      const b = await issueClientCert(clusterId, newCertCN.trim(), newCertTtl);
      setBundle(b); setNewCertCN(''); await reload();
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Issue failed');
    } finally { setIssuing(false); }
  };

  const revoke = async (cn: string) => {
    if (!confirm(`Revoke client cert "${cn}"? Existing connections using it will keep working until they reconnect.`)) return;
    try { await revokeClientCert(clusterId, cn); await reload(); }
    catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } } };
      setError(err.response?.data?.detail || 'Revoke failed');
    }
  };

  if (!state) return <div className="p-6 text-gray-400 text-sm">Loading TLS state…</div>;

  return (
    <div className="space-y-4">
      <div className="bg-white border rounded-xl p-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h3 className="font-semibold flex items-center gap-2"><Lock size={16} /> Transport Layer Security</h3>
            <p className="text-sm text-gray-500 mt-1">
              Tantor manages a per-cluster CA and per-broker keystores. SSL listener runs on port <code>{state.ssl_listener_port}</code> alongside PLAINTEXT.
            </p>
          </div>
          <button onClick={reload} className="px-3 py-1.5 text-sm border rounded hover:bg-gray-50 flex items-center gap-2">
            <RefreshCw size={14} /> Refresh
          </button>
        </div>
        {error && <div className="mt-3 bg-red-50 border border-red-200 text-red-700 px-3 py-2 rounded text-sm">{error}</div>}
        {info && <div className="mt-3 bg-blue-50 border border-blue-200 text-blue-700 px-3 py-2 rounded text-sm">{info}</div>}

        <div className="mt-4 grid grid-cols-2 gap-4">
          <Toggle
            label="Enable SSL/TLS listener"
            description="Brokers expose an SSL listener using a Tantor-managed CA."
            on={state.ssl_enabled}
            disabled={!admin}
            onChange={(v) => toggle(v, v ? state.mtls_required : false)}
          />
          <Toggle
            label="Require mTLS (client cert)"
            description="Producers/consumers must present a cert signed by this cluster CA."
            on={state.mtls_required}
            disabled={!admin || !state.ssl_enabled}
            onChange={(v) => toggle(state.ssl_enabled, v)}
          />
        </div>

        {state.ssl_enabled && !clusterRunning && (
          <div className="mt-3 bg-amber-50 border border-amber-200 text-amber-800 px-3 py-2 rounded text-sm flex items-start gap-2">
            <AlertCircle size={14} className="mt-0.5" />
            Configuration changed but brokers haven't been redeployed. Use <strong>Restart</strong> tab for a rolling restart, or <strong>Deploy</strong> to push the new keystores.
          </div>
        )}

        {state.ssl_enabled && state.ca_present && (
          <div className="mt-4 flex items-center gap-3">
            <a
              href={`/api/clusters/${clusterId}/security/tls/ca`}
              className="px-3 py-1.5 text-sm bg-gray-900 text-white rounded hover:bg-gray-800 flex items-center gap-2"
              download
            >
              <Download size={14} /> Download cluster CA cert (PEM)
            </a>
            <span className="text-xs text-gray-500">
              Producers/consumers configure <code>ssl.truststore</code> with this PEM (or its PKCS12 form).
            </span>
          </div>
        )}
      </div>

      {state.ssl_enabled && (
        <div className="bg-white border rounded-xl p-5">
          <h3 className="font-semibold flex items-center gap-2"><ShieldCheck size={16} /> Client certificates</h3>
          <p className="text-sm text-gray-500 mt-1">
            Mint a client cert + key (signed by the cluster CA) for a producer or consumer.
            {state.mtls_required ? ' mTLS is on — clients without a cert will be rejected.' : ' mTLS is off — these certs are optional.'}
          </p>

          {admin && (
            <div className="mt-3 flex items-end gap-2">
              <div className="flex-1">
                <label className="block text-xs text-gray-500 mb-1">Common name</label>
                <input
                  value={newCertCN}
                  onChange={(e) => setNewCertCN(e.target.value)}
                  placeholder="orders-producer"
                  className="w-full px-3 py-2 border rounded text-sm"
                />
              </div>
              <div className="w-32">
                <label className="block text-xs text-gray-500 mb-1">TTL (days)</label>
                <input
                  type="number" min={1} max={3650}
                  value={newCertTtl}
                  onChange={(e) => setNewCertTtl(parseInt(e.target.value || '365', 10))}
                  className="w-full px-3 py-2 border rounded text-sm"
                />
              </div>
              <button
                onClick={issue}
                disabled={issuing || !newCertCN.trim()}
                className="px-3 py-2 text-sm bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 flex items-center gap-2"
              >
                <Plus size={14} /> {issuing ? 'Issuing…' : 'Issue cert'}
              </button>
            </div>
          )}

          {bundle && <BundleDownload bundle={bundle} onClose={() => setBundle(null)} />}

          <div className="mt-4 border rounded overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 text-left text-xs uppercase text-gray-500">
                <tr>
                  <th className="px-3 py-2">Common name</th>
                  <th className="px-3 py-2 w-[180px]">Issued</th>
                  <th className="px-3 py-2 w-[180px]">Expires</th>
                  <th className="px-3 py-2 w-[120px]">Serial</th>
                  {admin && <th className="px-3 py-2 w-[80px]"></th>}
                </tr>
              </thead>
              <tbody>
                {!certs.length && (
                  <tr><td colSpan={admin ? 5 : 4} className="px-3 py-6 text-center text-gray-400">No client certs yet.</td></tr>
                )}
                {certs.map((c) => (
                  <tr key={c.common_name} className="border-t hover:bg-gray-50">
                    <td className="px-3 py-2 font-mono text-xs">{c.common_name}</td>
                    <td className="px-3 py-2 text-xs text-gray-600">{new Date(c.issued_at).toLocaleString()}</td>
                    <td className="px-3 py-2 text-xs text-gray-600">{new Date(c.expires_at).toLocaleString()}</td>
                    <td className="px-3 py-2 font-mono text-xs text-gray-500 truncate">{c.serial_number.slice(0, 16)}…</td>
                    {admin && (
                      <td className="px-3 py-2">
                        <button onClick={() => revoke(c.common_name)}
                          className="px-2 py-1 text-xs border rounded text-red-600 hover:bg-red-50">
                          <Trash2 size={12} />
                        </button>
                      </td>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

function Toggle({ label, description, on, disabled, onChange }: {
  label: string; description: string; on: boolean; disabled?: boolean; onChange: (v: boolean) => void;
}) {
  return (
    <div className={`border rounded-lg p-4 ${disabled ? 'opacity-50' : ''}`}>
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="font-medium text-sm">{label}</div>
          <div className="text-xs text-gray-500 mt-1">{description}</div>
        </div>
        <button
          type="button"
          disabled={disabled}
          onClick={() => onChange(!on)}
          className={`relative inline-flex h-6 w-11 shrink-0 items-center rounded-full transition-colors ${
            on ? 'bg-blue-600' : 'bg-gray-300'
          } ${disabled ? 'cursor-not-allowed' : ''}`}
        >
          <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${on ? 'translate-x-6' : 'translate-x-1'}`} />
        </button>
      </div>
    </div>
  );
}

function BundleDownload({ bundle, onClose }: { bundle: ClientCertBundle; onClose: () => void }) {
  const copy = (text: string) => navigator.clipboard.writeText(text);
  return (
    <div className="mt-4 border-2 border-blue-300 bg-blue-50 rounded-lg p-4">
      <div className="flex items-start justify-between mb-2">
        <div>
          <div className="font-semibold text-sm text-blue-900">Cert issued for {bundle.common_name}</div>
          <div className="text-xs text-blue-700 mt-0.5">
            Save these files now — Tantor stores the cert + key on disk but the operator-facing PEM bundle is shown only once here.
          </div>
        </div>
        <button onClick={onClose} className="text-blue-700 hover:text-blue-900 text-xs">dismiss</button>
      </div>
      <div className="grid grid-cols-2 gap-2 text-xs">
        <PemBox label="ca.pem" value={bundle.ca_pem} onCopy={() => copy(bundle.ca_pem)} />
        <PemBox label="client.crt" value={bundle.cert_pem} onCopy={() => copy(bundle.cert_pem)} />
        <PemBox label="client.key" value={bundle.key_pem} onCopy={() => copy(bundle.key_pem)} />
        <div className="bg-white border rounded p-2">
          <div className="flex items-center justify-between mb-1">
            <span className="font-mono text-xs">PKCS12 password</span>
            <button onClick={() => copy(bundle.p12_password)} className="text-blue-600 hover:underline text-xs flex items-center gap-1">
              <Copy size={12} /> copy
            </button>
          </div>
          <div className="font-mono text-xs break-all">{bundle.p12_password}</div>
        </div>
      </div>
    </div>
  );
}

function PemBox({ label, value, onCopy }: { label: string; value: string; onCopy: () => void }) {
  return (
    <div className="bg-white border rounded p-2 col-span-1">
      <div className="flex items-center justify-between mb-1">
        <span className="font-mono">{label}</span>
        <button onClick={onCopy} className="text-blue-600 hover:underline text-xs flex items-center gap-1">
          <Copy size={12} /> copy
        </button>
      </div>
      <pre className="font-mono text-[10px] max-h-32 overflow-y-auto whitespace-pre-wrap break-all">{value}</pre>
    </div>
  );
}
