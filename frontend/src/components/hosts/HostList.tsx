import { useState } from 'react';
import { Wifi, Loader2, Trash2, Shield, Pencil } from 'lucide-react';
import type { Host, PrereqResult } from '../../types';
import { testHost, checkPrereqs, deleteHost, updateHost } from '../../lib/api';
import { isAdmin } from '../../lib/auth';
import PrereqResults from './PrereqResults';
import EditHostModal from './EditHostModal';
import AgentControls from './AgentControls';

interface Props {
  hosts: Host[];
  onRefresh: () => void;
}

export default function HostList({ hosts, onRefresh }: Props) {
  const [testing, setTesting] = useState<string | null>(null);
  const [checking, setChecking] = useState<string | null>(null);
  const [editingHost, setEditingHost] = useState<Host | null>(null);
  const [prereqResults, setPrereqResults] = useState<Record<string, PrereqResult>>({});
  const [testMessages, setTestMessages] = useState<Record<string, { success: boolean; message: string }>>({});

  const handleTest = async (id: string) => {
    setTesting(id);
    try {
      const result = await testHost(id);
      setTestMessages(prev => ({ ...prev, [id]: { success: result.success, message: result.message } }));
      onRefresh();
    } finally {
      setTesting(null);
    }
  };

  const handlePrereqs = async (id: string) => {
    setChecking(id);
    try {
      const result = await checkPrereqs(id);
      setPrereqResults(prev => ({ ...prev, [id]: result }));
    } finally {
      setChecking(null);
    }
  };

  const handleDelete = async (id: string) => {
    if (!confirm('Delete this host?')) return;
    await deleteHost(id);
    onRefresh();
  };

  if (hosts.length === 0) {
    return (
      <div className="text-center py-12 text-gray-500">
        <p>No hosts added yet. Click "Add Host" to get started.</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {hosts.map(host => (
        <div key={host.id} className="bg-white border rounded-xl p-5 shadow-sm">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <div className={`w-3 h-3 rounded-full ${
                host.status === 'online' ? 'bg-green-500' :
                host.status === 'offline' ? 'bg-red-500' : 'bg-gray-400'
              }`} />
              <div>
                <h3 className="font-semibold text-gray-900">{host.hostname}</h3>
                <p className="text-sm text-gray-500">
                  {host.username}@{host.ip_address}:{host.ssh_port}
                  {host.os_info && <span className="ml-2 text-gray-400">({host.os_info})</span>}
                </p>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={() => handleTest(host.id)}
                disabled={testing === host.id}
                className="flex items-center gap-1.5 px-3 py-1.5 text-sm border rounded-lg hover:bg-gray-50 disabled:opacity-50"
              >
                {testing === host.id ? <Loader2 size={14} className="animate-spin" /> : <Wifi size={14} />}
                Test
              </button>
              <button
                onClick={() => handlePrereqs(host.id)}
                disabled={checking === host.id}
                className="flex items-center gap-1.5 px-3 py-1.5 text-sm border rounded-lg hover:bg-gray-50 disabled:opacity-50"
              >
                {checking === host.id ? <Loader2 size={14} className="animate-spin" /> : <Shield size={14} />}
                Prerequisites
              </button>
              {isAdmin() && (
                <button
                  onClick={() => setEditingHost(host)}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-sm border rounded-lg hover:bg-gray-50"
                >
                  <Pencil size={14} />
                </button>
              )}
              <button
                onClick={() => handleDelete(host.id)}
                className="flex items-center gap-1.5 px-3 py-1.5 text-sm text-red-600 border border-red-200 rounded-lg hover:bg-red-50"
              >
                <Trash2 size={14} />
              </button>
            </div>
          </div>

          {testMessages[host.id] && (
            <div className={`mt-3 px-3 py-2 rounded-lg text-sm ${
              testMessages[host.id].success ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-700'
            }`}>
              {testMessages[host.id].message}
            </div>
          )}

          {prereqResults[host.id] && (
            <div className="mt-4 border-t pt-4">
              <PrereqResults result={prereqResults[host.id]} />
            </div>
          )}

          <AgentControls hostId={host.id} />
        </div>
      ))}

      {editingHost && (
        <EditHostModal
          host={editingHost}
          onSubmit={async (data) => {
            await updateHost(editingHost.id, data);
            onRefresh();
          }}
          onClose={() => setEditingHost(null)}
        />
      )}
    </div>
  );
}
