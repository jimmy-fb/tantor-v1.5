import { useState } from 'react';
import { X } from 'lucide-react';
import type { Host, HostAuthType } from '../../types';

interface HostUpdate {
  hostname?: string;
  ip_address?: string;
  ssh_port?: number;
  username?: string;
  auth_type?: HostAuthType;
  credential?: string;
}

interface Props {
  host: Host;
  onSubmit: (data: HostUpdate) => Promise<void>;
  onClose: () => void;
}

export default function EditHostModal({ host, onSubmit, onClose }: Props) {
  const [form, setForm] = useState({
    hostname: host.hostname,
    ip_address: host.ip_address,
    ssh_port: host.ssh_port,
    username: host.username,
    auth_type: host.auth_type,
    credential: '',
  });
  const [loading, setLoading] = useState(false);
  const isKeyAuth = form.auth_type === 'key';
  const credentialLabel = form.auth_type === 'key'
    ? 'New Private Key'
    : form.auth_type === 'arcos'
      ? 'New ARCOS Password / Token'
      : 'New Password';

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    try {
      const update: HostUpdate = {
        hostname: form.hostname,
        ip_address: form.ip_address,
        ssh_port: form.ssh_port,
        username: form.username,
        auth_type: form.auth_type,
      };
      if (form.credential) {
        update.credential = form.credential;
      }
      await onSubmit(update);
      onClose();
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-md p-6">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-lg font-semibold">Edit Host</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <X size={20} />
          </button>
        </div>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Hostname</label>
            <input
              type="text"
              required
              value={form.hostname}
              onChange={e => setForm({ ...form, hostname: e.target.value })}
              className="w-full px-3 py-2 border rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
          </div>
          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-2">
              <label className="block text-sm font-medium text-gray-700 mb-1">IP Address</label>
              <input
                type="text"
                required
                value={form.ip_address}
                onChange={e => setForm({ ...form, ip_address: e.target.value })}
                className="w-full px-3 py-2 border rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Port</label>
              <input
                type="number"
                value={form.ssh_port}
                onChange={e => setForm({ ...form, ssh_port: Number(e.target.value) })}
                className="w-full px-3 py-2 border rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>
          </div>
          <div>
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
            <label className="block text-sm font-medium text-gray-700 mb-1">
              {credentialLabel}
              <span className="text-gray-400 font-normal ml-1">(leave blank to keep current)</span>
            </label>
            {isKeyAuth ? (
              <textarea
                rows={4}
                placeholder="Leave blank to keep current key"
                value={form.credential}
                onChange={e => setForm({ ...form, credential: e.target.value })}
                className="w-full px-3 py-2 border rounded-lg text-sm font-mono focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            ) : (
              <input
                type="password"
                placeholder={form.auth_type === 'arcos' ? 'Leave blank to keep current ARCOS credential' : 'Leave blank to keep current password'}
                value={form.credential}
                onChange={e => setForm({ ...form, credential: e.target.value })}
                className="w-full px-3 py-2 border rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            )}
          </div>
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
              {loading ? 'Saving...' : 'Save Changes'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
