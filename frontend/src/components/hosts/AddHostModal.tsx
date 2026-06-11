import { useState } from 'react';
import { X } from 'lucide-react';
import type { HostAuthType, HostCreate } from '../../types';

interface Props {
  onSubmit: (data: HostCreate) => Promise<void>;
  onClose: () => void;
  initialIpAddress?: string;
  initialHostname?: string;
}

export default function AddHostModal({ onSubmit, onClose, initialIpAddress = '', initialHostname = '' }: Props) {
  const [form, setForm] = useState<HostCreate>({
    hostname: initialHostname,
    ip_address: initialIpAddress,
    ssh_port: 22,
    username: 'root',
    auth_type: 'password',
    credential: '',
  });
  const [loading, setLoading] = useState(false);
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
          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-2">
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
            </label>
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
