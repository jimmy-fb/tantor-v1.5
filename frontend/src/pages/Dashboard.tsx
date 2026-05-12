import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { Server, Network, Plus, Activity } from 'lucide-react';
import type { Host, Cluster } from '../types';
import { getHosts, getClusters } from '../lib/api';

export default function Dashboard() {
  const [hosts, setHosts] = useState<Host[]>([]);
  const [clusters, setClusters] = useState<Cluster[]>([]);

  useEffect(() => {
    getHosts().then(setHosts);
    getClusters().then(setClusters);
  }, []);

  const onlineHosts = hosts.filter(h => h.status === 'online').length;
  // v1.2.0 #11 — Dashboard's running-cluster count was excluding external
  // clusters (which use state="connected" instead of "running"). Count both,
  // since they're equally healthy and equally manageable from this UI.
  const runningClusters = clusters.filter(c =>
    c.state === 'running' || c.state === 'connected'
  ).length;
  const internalRunning = clusters.filter(c =>
    (c.kind || 'managed') === 'managed' && c.state === 'running'
  ).length;
  const externalConnected = clusters.filter(c =>
    c.kind === 'external' && c.state === 'connected'
  ).length;

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-8">Dashboard</h1>

      {/* Stats */}
      <div className="grid grid-cols-4 gap-4 mb-8">
        <div className="bg-white border rounded-xl p-5">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-blue-100 rounded-lg"><Server size={20} className="text-blue-600" /></div>
            <div>
              <div className="text-2xl font-bold">{hosts.length}</div>
              <div className="text-sm text-gray-500">Total Hosts</div>
            </div>
          </div>
        </div>
        <div className="bg-white border rounded-xl p-5">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-green-100 rounded-lg"><Activity size={20} className="text-green-600" /></div>
            <div>
              <div className="text-2xl font-bold">{onlineHosts}</div>
              <div className="text-sm text-gray-500">Online Hosts</div>
            </div>
          </div>
        </div>
        <div className="bg-white border rounded-xl p-5">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-purple-100 rounded-lg"><Network size={20} className="text-purple-600" /></div>
            <div>
              <div className="text-2xl font-bold">{clusters.length}</div>
              <div className="text-sm text-gray-500">Clusters</div>
            </div>
          </div>
        </div>
        <div className="bg-white border rounded-xl p-5">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-orange-100 rounded-lg"><Activity size={20} className="text-orange-600" /></div>
            <div>
              <div className="text-2xl font-bold">{runningClusters}</div>
              <div className="text-sm text-gray-500">Running Clusters</div>
              <div className="text-xs text-gray-400 mt-0.5">{internalRunning} managed · {externalConnected} external</div>
            </div>
          </div>
        </div>
      </div>

      {/* Quick actions */}
      <div className="grid grid-cols-2 gap-4 mb-8">
        <Link
          to="/hosts"
          className="flex items-center gap-4 bg-white border rounded-xl p-5 hover:border-blue-300 transition-colors"
        >
          <div className="p-3 bg-blue-50 rounded-xl"><Server size={24} className="text-blue-600" /></div>
          <div>
            <div className="font-semibold text-gray-900">Manage Hosts</div>
            <div className="text-sm text-gray-500">Add, test, and check prerequisites on Linux hosts</div>
          </div>
        </Link>
        <Link
          to="/clusters/new"
          className="flex items-center gap-4 bg-white border rounded-xl p-5 hover:border-green-300 transition-colors"
        >
          <div className="p-3 bg-green-50 rounded-xl"><Plus size={24} className="text-green-600" /></div>
          <div>
            <div className="font-semibold text-gray-900">New Cluster</div>
            <div className="text-sm text-gray-500">Deploy a new Kafka cluster with KRaft or ZooKeeper</div>
          </div>
        </Link>
      </div>

      {/* Recent clusters */}
      {clusters.length > 0 && (
        <div>
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Recent Clusters</h2>
          <div className="space-y-3">
            {clusters.slice(0, 5).map(cluster => (
              <Link
                key={cluster.id}
                to={`/clusters/${cluster.id}`}
                className="flex items-center justify-between bg-white border rounded-xl p-4 hover:border-blue-300 transition-colors"
              >
                <div className="flex items-center gap-3">
                  <Network size={18} className="text-gray-400" />
                  <div>
                    <div className="font-medium text-sm">{cluster.name}</div>
                    <div className="text-xs text-gray-400">Kafka {cluster.kafka_version} / {cluster.mode.toUpperCase()}</div>
                  </div>
                </div>
                <span className={`px-2.5 py-1 rounded-full text-xs font-medium ${
                  cluster.state === 'running' ? 'bg-green-100 text-green-700' :
                  cluster.state === 'stopped' ? 'bg-gray-100 text-gray-600' :
                  cluster.state === 'deploying' ? 'bg-blue-100 text-blue-700' :
                  cluster.state === 'error' ? 'bg-red-100 text-red-700' :
                  'bg-yellow-100 text-yellow-700'
                }`}>
                  {cluster.state}
                </span>
              </Link>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
