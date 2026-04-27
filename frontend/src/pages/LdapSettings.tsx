import { useState, useEffect } from 'react';
import { KeyRound, Save, TestTube, RefreshCw, X, Check, Users } from 'lucide-react';
import { getLdapConfig, updateLdapConfig, testLdapConnection, syncLdapUsers } from '../lib/api';

interface LdapConfig {
  id?: string;
  enabled: boolean;
  server_url: string;
  use_ssl: boolean;
  tls_validate_cert: boolean;
  tls_ca_cert_present?: boolean;
  bind_dn: string;
  user_search_base: string;
  user_search_filter: string;
  group_search_base: string;
  admin_group_dn: string;
  monitor_group_dn: string;
  default_role: string;
  connection_timeout: number;
}

interface LdapUser {
  dn: string;
  username: string;
  display_name: string;
}

const FILTER_PRESETS = [
  { label: 'Active Directory (sAMAccountName)', value: '(sAMAccountName={username})' },
  { label: 'Active Directory (userPrincipalName)', value: '(userPrincipalName={username}@DOMAIN)' },
  { label: 'OpenLDAP (uid)', value: '(uid={username})' },
  { label: 'OpenLDAP (cn)', value: '(cn={username})' },
];

const DEFAULT_CONFIG: LdapConfig = {
  enabled: false,
  server_url: '',
  use_ssl: false,
  tls_validate_cert: true,
  tls_ca_cert_present: false,
  bind_dn: '',
  user_search_base: '',
  user_search_filter: '(sAMAccountName={username})',
  group_search_base: '',
  admin_group_dn: '',
  monitor_group_dn: '',
  default_role: 'monitor',
  connection_timeout: 10,
};

export default function LdapSettings() {
  const [config, setConfig] = useState<LdapConfig>(DEFAULT_CONFIG);
  const [bindPassword, setBindPassword] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [showTestModal, setShowTestModal] = useState(false);
  const [testUsername, setTestUsername] = useState('');
  const [testPassword, setTestPassword] = useState('');
  const [testResult, setTestResult] = useState<{ success: boolean; message: string; user_dn?: string; groups?: string[] } | null>(null);
  const [ldapUsers, setLdapUsers] = useState<LdapUser[]>([]);
  const [showUsers, setShowUsers] = useState(false);
  // CA cert is write-only — backend never returns the body, only `tls_ca_cert_present`.
  // Empty string means "no change". The "Clear stored CA" button posts the literal empty string.
  const [caCertInput, setCaCertInput] = useState('');
  const [clearCaCert, setClearCaCert] = useState(false);

  useEffect(() => {
    fetchConfig();
  }, []);

  const fetchConfig = async () => {
    try {
      const data = await getLdapConfig();
      if (data) {
        setConfig(data);
      }
    } catch {
      // No config yet, use defaults
    } finally {
      setLoading(false);
    }
  };

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!bindPassword && !config.id) {
      setError('Bind password is required for initial configuration');
      return;
    }
    setSaving(true);
    setError('');
    setSuccess('');
    try {
      const payload: Record<string, unknown> = {
        ...config,
        bind_password: bindPassword || 'UNCHANGED',
      };
      delete payload.id;
      delete payload.tls_ca_cert_present;
      // Only send tls_ca_cert when the operator wants to change it. Empty
      // string explicitly clears, undefined leaves stored value untouched.
      if (clearCaCert) {
        payload.tls_ca_cert = '';
      } else if (caCertInput.trim()) {
        payload.tls_ca_cert = caCertInput;
      } else {
        delete payload.tls_ca_cert;
      }
      const result = await updateLdapConfig(payload);
      setConfig(result);
      setBindPassword('');
      setCaCertInput('');
      setClearCaCert(false);
      setSuccess('LDAP configuration saved successfully');
      setTimeout(() => setSuccess(''), 5000);
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || 'Failed to save configuration';
      setError(msg);
    } finally {
      setSaving(false);
    }
  };

  const handleTest = async () => {
    if (!testUsername || !testPassword) return;
    setTesting(true);
    setTestResult(null);
    try {
      const result = await testLdapConnection({ username: testUsername, password: testPassword });
      setTestResult(result);
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || 'Test failed';
      setTestResult({ success: false, message: msg });
    } finally {
      setTesting(false);
    }
  };

  const handleSyncUsers = async () => {
    setSyncing(true);
    setError('');
    try {
      const result = await syncLdapUsers();
      setLdapUsers(result.users || []);
      setShowUsers(true);
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || 'Failed to sync users';
      setError(msg);
    } finally {
      setSyncing(false);
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
            <KeyRound size={24} />
            LDAP / Active Directory
          </h1>
          <p className="text-gray-500 mt-1">Configure LDAP or Active Directory authentication</p>
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={() => setShowTestModal(true)}
            disabled={!config.id}
            className="flex items-center gap-2 px-4 py-2 bg-amber-600 hover:bg-amber-700 disabled:bg-gray-300 text-white rounded-lg font-medium transition-colors"
          >
            <TestTube size={18} />
            Test Connection
          </button>
          <button
            onClick={handleSyncUsers}
            disabled={!config.id || syncing}
            className="flex items-center gap-2 px-4 py-2 bg-indigo-600 hover:bg-indigo-700 disabled:bg-gray-300 text-white rounded-lg font-medium transition-colors"
          >
            <Users size={18} />
            {syncing ? 'Syncing...' : 'Sync Users'}
          </button>
        </div>
      </div>

      {error && (
        <div className="p-3 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm flex items-center justify-between">
          {error}
          <button onClick={() => setError('')} className="text-red-400 hover:text-red-600"><X size={16} /></button>
        </div>
      )}

      {success && (
        <div className="p-3 bg-green-50 border border-green-200 rounded-lg text-green-700 text-sm flex items-center gap-2">
          <Check size={16} />
          {success}
        </div>
      )}

      <form onSubmit={handleSave} className="space-y-6">
        {/* Enable/Disable Toggle */}
        <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm">
          <div className="flex items-center justify-between">
            <div>
              <h3 className="font-semibold text-gray-900">LDAP Authentication</h3>
              <p className="text-sm text-gray-500 mt-1">
                {config.enabled
                  ? 'LDAP authentication is enabled. Users can log in with their directory credentials.'
                  : 'LDAP authentication is disabled. Only local users can log in.'}
              </p>
            </div>
            <button
              type="button"
              onClick={() => setConfig({ ...config, enabled: !config.enabled })}
              className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
                config.enabled ? 'bg-blue-600' : 'bg-gray-300'
              }`}
            >
              <span
                className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
                  config.enabled ? 'translate-x-6' : 'translate-x-1'
                }`}
              />
            </button>
          </div>
        </div>

        {/* Server Connection */}
        <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm space-y-4">
          <h3 className="font-semibold text-gray-900">Server Connection</h3>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Server URL</label>
              <input
                type="text"
                value={config.server_url}
                onChange={(e) => setConfig({ ...config, server_url: e.target.value })}
                placeholder="ldap://ad.company.com:389"
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                required
              />
              <p className="text-xs text-gray-400 mt-1">e.g. ldap://server:389 or ldaps://server:636</p>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Connection Timeout (seconds)</label>
              <input
                type="number"
                value={config.connection_timeout}
                onChange={(e) => setConfig({ ...config, connection_timeout: parseInt(e.target.value) || 10 })}
                min={1}
                max={60}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
          </div>

          <div className="flex items-center gap-3">
            <label className="flex items-center gap-2 text-sm text-gray-700 cursor-pointer">
              <input
                type="checkbox"
                checked={config.use_ssl}
                onChange={(e) => setConfig({ ...config, use_ssl: e.target.checked })}
                className="w-4 h-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500"
              />
              Use SSL/TLS (LDAPS)
            </label>
          </div>

          {config.use_ssl && (
            <div className="border-l-4 border-blue-200 pl-4 py-2 space-y-3">
              <label className="flex items-center gap-2 text-sm text-gray-700 cursor-pointer">
                <input
                  type="checkbox"
                  checked={config.tls_validate_cert}
                  onChange={(e) => setConfig({ ...config, tls_validate_cert: e.target.checked })}
                  className="w-4 h-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500"
                />
                Validate server certificate
                <span className="text-xs text-gray-500">(recommended)</span>
              </label>

              {!config.tls_validate_cert && (
                <div className="text-xs text-amber-700 bg-amber-50 border border-amber-200 rounded px-3 py-2">
                  Server-cert validation is OFF. The connection is encrypted but vulnerable to MITM —
                  use only against trusted dev directories.
                </div>
              )}

              {config.tls_validate_cert && (
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    CA Certificate (PEM){' '}
                    <span className="text-gray-400 font-normal">
                      — optional, needed for private/internal CAs
                    </span>
                  </label>
                  <textarea
                    value={caCertInput}
                    onChange={(e) => {
                      setCaCertInput(e.target.value);
                      if (e.target.value) setClearCaCert(false);
                    }}
                    rows={6}
                    placeholder={
                      config.tls_ca_cert_present
                        ? '— stored — paste new PEM to replace —'
                        : '-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----'
                    }
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg font-mono text-xs focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  />
                  <div className="flex items-center justify-between mt-1">
                    <p className="text-xs text-gray-400">
                      {config.tls_ca_cert_present
                        ? 'A CA certificate is currently stored. Paste a new one to replace it.'
                        : 'Leave blank to use the system trust store (works for public CAs).'}
                    </p>
                    {config.tls_ca_cert_present && (
                      <label className="flex items-center gap-1.5 text-xs text-red-600 cursor-pointer">
                        <input
                          type="checkbox"
                          checked={clearCaCert}
                          onChange={(e) => {
                            setClearCaCert(e.target.checked);
                            if (e.target.checked) setCaCertInput('');
                          }}
                          className="w-3 h-3"
                        />
                        Clear stored CA on save
                      </label>
                    )}
                  </div>
                </div>
              )}
            </div>
          )}

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Bind DN</label>
              <input
                type="text"
                value={config.bind_dn}
                onChange={(e) => setConfig({ ...config, bind_dn: e.target.value })}
                placeholder="cn=admin,dc=example,dc=com"
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                required
              />
              <p className="text-xs text-gray-400 mt-1">Service account DN used to search for users</p>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Bind Password {config.id && <span className="text-gray-400">(leave blank to keep current)</span>}
              </label>
              <input
                type="password"
                value={bindPassword}
                onChange={(e) => setBindPassword(e.target.value)}
                placeholder={config.id ? '********' : 'Enter password'}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                required={!config.id}
              />
            </div>
          </div>
        </div>

        {/* User Search */}
        <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm space-y-4">
          <h3 className="font-semibold text-gray-900">User Search</h3>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">User Search Base</label>
            <input
              type="text"
              value={config.user_search_base}
              onChange={(e) => setConfig({ ...config, user_search_base: e.target.value })}
              placeholder="ou=users,dc=example,dc=com"
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              required
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">User Search Filter</label>
            <div className="flex gap-2">
              <input
                type="text"
                value={config.user_search_filter}
                onChange={(e) => setConfig({ ...config, user_search_filter: e.target.value })}
                placeholder="(sAMAccountName={username})"
                className="flex-1 px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                required
              />
              <select
                value=""
                onChange={(e) => {
                  if (e.target.value) setConfig({ ...config, user_search_filter: e.target.value });
                }}
                className="px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm"
              >
                <option value="">Presets...</option>
                {FILTER_PRESETS.map((p) => (
                  <option key={p.value} value={p.value}>{p.label}</option>
                ))}
              </select>
            </div>
            <p className="text-xs text-gray-400 mt-1">Use {'{username}'} as placeholder for the login username</p>
          </div>
        </div>

        {/* Group Mapping */}
        <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm space-y-4">
          <h3 className="font-semibold text-gray-900">Group Mapping</h3>
          <p className="text-sm text-gray-500">Map LDAP groups to Tantor roles. Leave blank to assign the default role to all LDAP users.</p>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Group Search Base</label>
            <input
              type="text"
              value={config.group_search_base || ''}
              onChange={(e) => setConfig({ ...config, group_search_base: e.target.value })}
              placeholder="ou=groups,dc=example,dc=com"
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
            <p className="text-xs text-gray-400 mt-1">Required for OpenLDAP group lookups. AD uses memberOf attribute automatically.</p>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Admin Group DN</label>
              <input
                type="text"
                value={config.admin_group_dn || ''}
                onChange={(e) => setConfig({ ...config, admin_group_dn: e.target.value })}
                placeholder="cn=tantor-admins,ou=groups,dc=example,dc=com"
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
              <p className="text-xs text-gray-400 mt-1">Members get admin role</p>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Monitor Group DN</label>
              <input
                type="text"
                value={config.monitor_group_dn || ''}
                onChange={(e) => setConfig({ ...config, monitor_group_dn: e.target.value })}
                placeholder="cn=tantor-monitors,ou=groups,dc=example,dc=com"
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
              <p className="text-xs text-gray-400 mt-1">Members get monitor role</p>
            </div>
          </div>

          <div className="w-48">
            <label className="block text-sm font-medium text-gray-700 mb-1">Default Role</label>
            <select
              value={config.default_role}
              onChange={(e) => setConfig({ ...config, default_role: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              <option value="monitor">Monitor</option>
              <option value="admin">Admin</option>
            </select>
            <p className="text-xs text-gray-400 mt-1">Role when user matches no group</p>
          </div>
        </div>

        {/* Save Button */}
        <div className="flex justify-end">
          <button
            type="submit"
            disabled={saving}
            className="flex items-center gap-2 px-6 py-2.5 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white rounded-lg font-medium transition-colors"
          >
            <Save size={18} />
            {saving ? 'Saving...' : 'Save Configuration'}
          </button>
        </div>
      </form>

      {/* Test Connection Modal */}
      {showTestModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="font-semibold text-gray-900 text-lg">Test LDAP Connection</h3>
              <button onClick={() => { setShowTestModal(false); setTestResult(null); }} className="text-gray-400 hover:text-gray-600">
                <X size={20} />
              </button>
            </div>
            <p className="text-sm text-gray-500">Enter credentials of an LDAP user to test authentication.</p>

            <div className="space-y-3">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Username</label>
                <input
                  type="text"
                  value={testUsername}
                  onChange={(e) => setTestUsername(e.target.value)}
                  placeholder="jdoe"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Password</label>
                <input
                  type="password"
                  value={testPassword}
                  onChange={(e) => setTestPassword(e.target.value)}
                  placeholder="password"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
              </div>
            </div>

            {testResult && (
              <div className={`p-3 rounded-lg text-sm ${testResult.success ? 'bg-green-50 border border-green-200 text-green-700' : 'bg-red-50 border border-red-200 text-red-700'}`}>
                <p className="font-medium">{testResult.success ? 'Success' : 'Failed'}</p>
                <p className="mt-1">{testResult.message}</p>
                {testResult.user_dn && <p className="mt-1 text-xs font-mono">DN: {testResult.user_dn}</p>}
                {testResult.groups && testResult.groups.length > 0 && (
                  <div className="mt-2">
                    <p className="text-xs font-medium">Groups ({testResult.groups.length}):</p>
                    <ul className="text-xs font-mono mt-1 space-y-0.5 max-h-32 overflow-y-auto">
                      {testResult.groups.map((g, i) => <li key={i} className="truncate">{g}</li>)}
                    </ul>
                  </div>
                )}
              </div>
            )}

            <div className="flex justify-end gap-3">
              <button
                onClick={() => { setShowTestModal(false); setTestResult(null); }}
                className="px-4 py-2 bg-gray-200 hover:bg-gray-300 text-gray-700 rounded-lg font-medium transition-colors"
              >
                Close
              </button>
              <button
                onClick={handleTest}
                disabled={testing || !testUsername || !testPassword}
                className="flex items-center gap-2 px-4 py-2 bg-amber-600 hover:bg-amber-700 disabled:bg-gray-300 text-white rounded-lg font-medium transition-colors"
              >
                <RefreshCw size={16} className={testing ? 'animate-spin' : ''} />
                {testing ? 'Testing...' : 'Test'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Synced Users Panel */}
      {showUsers && (
        <div className="bg-white border border-gray-200 rounded-xl shadow-sm overflow-hidden">
          <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
            <h3 className="font-semibold text-gray-900">Discovered LDAP Users ({ldapUsers.length})</h3>
            <button onClick={() => setShowUsers(false)} className="text-gray-400 hover:text-gray-600">
              <X size={18} />
            </button>
          </div>
          {ldapUsers.length === 0 ? (
            <div className="px-6 py-8 text-center text-gray-500">
              No users found. Check your search base and filter settings.
            </div>
          ) : (
            <table className="w-full">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  <th className="text-left px-6 py-3 text-xs font-medium text-gray-500 uppercase tracking-wider">Username</th>
                  <th className="text-left px-6 py-3 text-xs font-medium text-gray-500 uppercase tracking-wider">Display Name</th>
                  <th className="text-left px-6 py-3 text-xs font-medium text-gray-500 uppercase tracking-wider">DN</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {ldapUsers.map((user, i) => (
                  <tr key={i} className="hover:bg-gray-50">
                    <td className="px-6 py-3 text-sm font-medium text-gray-900">{user.username}</td>
                    <td className="px-6 py-3 text-sm text-gray-600">{user.display_name}</td>
                    <td className="px-6 py-3 text-xs text-gray-400 font-mono truncate max-w-xs">{user.dn}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </div>
  );
}
