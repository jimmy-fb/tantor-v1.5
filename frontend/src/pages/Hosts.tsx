import { useState, useEffect } from 'react';
import { Copy, Plus, X } from 'lucide-react';
import type { Host } from '../types';
import { type MintAgentTokenResponse, createHost, getHosts, mintAgentToken } from '../lib/api';
import HostList from '../components/hosts/HostList';
import AddHostModal from '../components/hosts/AddHostModal';

export default function Hosts() {
  const [hosts, setHosts] = useState<Host[]>([]);
  const [showModal, setShowModal] = useState(false);
  const [pendingInstall, setPendingInstall] = useState<{ host: Host; token: MintAgentTokenResponse } | null>(null);
  const [copied, setCopied] = useState(false);

  const fetchHosts = () => {
    getHosts().then(setHosts);
  };

  useEffect(() => {
    fetchHosts();
  }, []);

  const handleCopy = async () => {
    if (!pendingInstall) return;
    await navigator.clipboard.writeText(pendingInstall.token.install_command);
    setCopied(true);
    setTimeout(() => setCopied(false), 2500);
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Hosts</h1>
          <p className="text-sm text-gray-500 mt-1">Manage your Linux servers for Kafka deployment</p>
        </div>
        <button
          onClick={() => setShowModal(true)}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700"
        >
          <Plus size={16} /> Add Host
        </button>
      </div>

      {pendingInstall && (
        <div className="mb-6 rounded-lg border border-blue-200 bg-blue-50 p-4 relative">
          <button
            onClick={() => setPendingInstall(null)}
            className="absolute top-3 right-3 text-blue-700 hover:text-blue-900"
            title="Dismiss"
          >
            <X size={16} />
          </button>
          <div className="font-medium text-blue-900">
            Host added — run this on <code className="bg-white px-1 py-0.5 rounded text-xs">{pendingInstall.host.hostname}</code> to install the agent
          </div>
          <p className="text-xs text-blue-800/80 mt-1">
            Token expires {new Date(pendingInstall.token.expires_at).toLocaleString()}. The
            agent dials Tantor — no inbound SSH required.
          </p>
          <div className="mt-3 flex items-center gap-2">
            <code className="flex-1 px-3 py-2 bg-white border border-blue-200 rounded text-xs break-all">
              {pendingInstall.token.install_command}
            </code>
            <button
              onClick={handleCopy}
              className="flex items-center gap-1 px-3 py-2 text-xs border border-blue-300 rounded hover:bg-white text-blue-700"
            >
              <Copy size={12} /> {copied ? 'Copied' : 'Copy'}
            </button>
          </div>
        </div>
      )}

      <HostList hosts={hosts} onRefresh={fetchHosts} />

      {showModal && (
        <AddHostModal
          onSubmit={async (data) => {
            const newHost = await createHost(data);
            fetchHosts();
            // For agent-mode hosts, mint a token right away and show
            // the one-line installer so the operator can paste it on
            // the broker host without any extra clicks.
            if (data.auth_type === 'agent') {
              try {
                const token = await mintAgentToken(newHost.id);
                setPendingInstall({ host: newHost, token });
              } catch {
                // Non-fatal — operator can mint manually from the host row.
              }
            }
          }}
          onClose={() => setShowModal(false)}
        />
      )}
    </div>
  );
}
