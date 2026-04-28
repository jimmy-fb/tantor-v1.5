import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { Network, Plus, Trash2, Plug } from 'lucide-react';
import type { Cluster } from '../types';
import { getClusters, deleteCluster } from '../lib/api';

export default function Clusters() {
  const [clusters, setClusters] = useState<Cluster[]>([]);

  const fetchClusters = () => {
    getClusters().then(setClusters);
  };

  useEffect(() => {
    fetchClusters();
  }, []);

  const handleDelete = async (id: string) => {
    if (!confirm('Delete this cluster? This does not stop running services.')) return;
    await deleteCluster(id);
    fetchClusters();
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Clusters</h1>
          <p className="text-sm text-gray-500 mt-1">Your Kafka cluster deployments</p>
        </div>
        <Link
          to="/clusters/new"
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700"
        >
          <Plus size={16} /> New Cluster
        </Link>
      </div>

      {clusters.length === 0 ? (
        <div className="text-center py-12 text-gray-500">
          <Network size={48} className="mx-auto mb-4 text-gray-300" />
          <p>No clusters yet. Create your first Kafka cluster.</p>
        </div>
      ) : (
        <div className="space-y-3">
          {clusters.map(cluster => {
            const isExternal = cluster.kind === 'external';
            const Icon = isExternal ? Plug : Network;
            return (
            <div key={cluster.id} className="flex items-center justify-between bg-white border rounded-xl p-5 shadow-sm">
              <Link to={`/clusters/${cluster.id}`} className="flex items-center gap-4 flex-1">
                <Icon size={20} className={isExternal ? 'text-purple-500' : 'text-gray-400'} />
                <div>
                  <div className="font-semibold text-gray-900 flex items-center gap-2">
                    {cluster.name}
                    {isExternal && (
                      <span className="px-2 py-0.5 rounded text-xs font-medium bg-purple-50 text-purple-700 border border-purple-200">
                        external
                      </span>
                    )}
                  </div>
                  <div className="text-sm text-gray-500">
                    {isExternal
                      ? <>Imported · Kafka {cluster.kafka_version === 'external' ? 'unknown' : cluster.kafka_version}</>
                      : <>Kafka {cluster.kafka_version} / {cluster.mode.toUpperCase()}</>
                    }
                    <span className="text-gray-300 mx-2">|</span>
                    Created {new Date(cluster.created_at).toLocaleDateString()}
                  </div>
                </div>
              </Link>
              <div className="flex items-center gap-3">
                <span className={`px-3 py-1 rounded-full text-xs font-medium ${
                  cluster.state === 'running' ? 'bg-green-100 text-green-700' :
                  cluster.state === 'connected' ? 'bg-green-100 text-green-700' :
                  cluster.state === 'stopped' ? 'bg-gray-100 text-gray-600' :
                  cluster.state === 'deploying' ? 'bg-blue-100 text-blue-700' :
                  cluster.state === 'error' ? 'bg-red-100 text-red-700' :
                  'bg-yellow-100 text-yellow-700'
                }`}>
                  {cluster.state}
                </span>
                <button
                  onClick={() => handleDelete(cluster.id)}
                  className="p-2 text-red-500 hover:bg-red-50 rounded-lg"
                >
                  <Trash2 size={16} />
                </button>
              </div>
            </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
