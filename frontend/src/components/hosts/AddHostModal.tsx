import { useState } from 'react';
import { Antenna, KeyRound, X } from 'lucide-react';
import type { HostAuthType, HostCreate } from '../../types';

interface Props {
  onSubmit: (data: HostCreate) => Promise<void>;
  onClose: () => void;
  initialIpAddress?: string;
  initialHostname?: string;
}

/**
 * Add Host modal — v1.5+ defaults to AGENT mode, which is the recommended
 * path for any new install. SSH mode stays available for environments
 * that haven't yet rolled out the agent.
 */
export default function AddHostModal({ onSubmit, onClose, initialIpAddress = '', initialHostname = '' }: Props) {
  const [form, setForm] = useState<HostCreate>({
    hostname: initialHostname,
    ip_address: initialIpAddress,
    ssh_port: 22,
    username: '',
    auth_type: 'agent',
    credential: '',
  });
  const [loading, setLoading] = useState(false);
  const isAgent = form.auth_type === 'agent';
  const isKeyAuth = form.auth_type === 'key';
  const credentialLabel = form.auth_type === 'key'
    ? 'Private Key'
    : form.auth_type === 'arcos'
      ? 'ARCOS Password / Token'
      : 'Password';

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    try {
      await onSubmit(form);
      onClose();
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-md p-6">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-lg font-semibold">Add Host</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <X size={20} />
          </button>
        </div>

        {/* Mode picker */}
        <div className="grid grid-cols-2 gap-2 mb-5">
          <button
            type="button"
            onClick={() => setForm({ ...form, auth_type: 'agent', credential: '', username: '' })}
            className={`flex items-center gap-2 px-3 py-2 text-sm rounded-lg border transition ${
              isAgent
                ? 'bg-blue-50 border-blue-500 text-blue-700'
                : 'bg-white border-gray-200 text-gray-600 hover:border-gray-300'
            }`}
          >
            <Antenna size={16} />
            <div className="text-left">
              <div className="font-medium">Agent</div>
              <div className="text-xs opacity-75">Recommended · no inbound SSH</div>
            </div>
          </button>
          <button
            type="button"
            onClick={() => setForm({ ...form, auth_type: 'password', username: form.username || 'root' })}
            className={`flex items-center gap-2 px-3 py-2 text-sm rounded-lg border transition ${
              !isAgent
                ? 'bg-amber-50 border-amber-500 text-amber-700'
                : 'bg-white border-gray-200 text-gray-600 hover:border-gray-300'
            }`}
          >
            <KeyRound size={16} />
            <div className="text-left">
              <div className="font-medium">SSH</div>
              <div className="text-xs opacity-75">Legacy</div>
            </div>
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Hostname</label>
            <input
              type="text"
              required
              placeholder="kafka-node-1"
              value={form.hostname}
              onChange={e => setForm({ ...form, hostname: e.target.value })}
              className="w-full px-3 py-2 border rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">IP Address</label>
            <input
              type="text"
              required
              placeholder="192.168.1.100"
              value={form.ip_address}
              onChange={e => setForm({ ...form, ip_address: e.target.value })}
              className="w-full px-3 py-2 border rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
          </div>

          {isAgent ? (
            <div className="rounded-lg border border-blue-100 bg-blue-50 p-3 text-sm text-blue-800">
              <div className="font-medium mb-1">Agent mode — no SSH credentials needed</div>
              <p className="text-blue-700/90 text-xs leading-relaxed">
                After clicking Add Host, you'll get a one-line installer to paste on
                the broker. The agent dials Tantor — Tantor never SSHes in.
              </p>
            </div>
          ) : (
            <>
              <div className="grid grid-cols-3 gap-3">
                <div className="col-span-2">
                  <label className="block text-sm font-medium text-gray-700 mb-1">Username</label>
                  <input
                    type="text"
                    required
                    value={form.username}
                    onChange={e => setForm({ ...form, username: e.target.value })}
                    className="w-full px-3 py-2 border rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">SSH Port</label>
                  <input
                    type="number"
                    value={form.ssh_port}
                    onChange={e => setForm({ ...form, ssh_port: Number(e.target.value) })}
                    className="w-full px-3 py-2 border rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  />
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Authentication</label>
                <select
                  value={form.auth_type}
                  onChange={e => setForm({ ...form, auth_type: e.target.value as HostAuthType })}
                  className="w-full px-3 py-2 border rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="password">Password</option>
                  <option value="key">SSH Key</option>
                  <option value="arcos">ARCOS</option>
                </select>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">{credentialLabel}</label>
                {isKeyAuth ? (
                  <textarea
                    required
                    rows={4}
                    placeholder="-----BEGIN OPENSSH PRIVATE KEY-----"
                    value={form.credential}
                    onChange={e => setForm({ ...form, credential: e.target.value })}
                    className="w-full px-3 py-2 border rounded-lg text-sm font-mono focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  />
                ) : (
                  <input
                    type="password"
                    required
                    value={form.credential}
                    onChange={e => setForm({ ...form, credential: e.target.value })}
                    className="w-full px-3 py-2 border rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  />
                )}
              </div>
            </>
          )}

          <div className="flex justify-end gap-3 pt-2">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-sm text-gray-600 hover:text-gray-800"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={loading}
              className="px-4 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 disabled:opacity-50"
            >
              {loading ? 'Adding...' : 'Add Host'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
