import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { Network, Plus, Trash2, Plug, Search, Filter, RotateCw } from 'lucide-react';
import type { Cluster } from '../types';
import { getClusters, deleteCluster, deployCluster } from '../lib/api';

const ENV_STYLES: Record<string, string> = {
  prod: 'bg-red-50 text-red-700 border-red-200',
  production: 'bg-red-50 text-red-700 border-red-200',
  staging: 'bg-amber-50 text-amber-700 border-amber-200',
  qa: 'bg-blue-50 text-blue-700 border-blue-200',
  dev: 'bg-emerald-50 text-emerald-700 border-emerald-200',
  test: 'bg-emerald-50 text-emerald-700 border-emerald-200',
};

function envBadge(env?: string) {
  if (!env) return null;
  const style = ENV_STYLES[env.toLowerCase()] || 'bg-gray-50 text-gray-700 border-gray-200';
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium border ${style}`}>{env}</span>
  );
}

export default function Clusters() {
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [search, setSearch] = useState('');
  const [envFilter, setEnvFilter] = useState<string>('');
  const [kindFilter, setKindFilter] = useState<'all' | 'managed' | 'external'>('all');
  const [retrying, setRetrying] = useState<string | null>(null);

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

  // QA #48: Retry button on errored deployments — kicks off a fresh deploy.
  const handleRetry = async (cluster: Cluster) => {
    if (!confirm(`Retry deployment for "${cluster.name}"? Tantor will re-run the playbook.`)) return;
    setRetrying(cluster.id);
    try {
      await deployCluster(cluster.id);
      fetchClusters();
    } catch (e) {
      alert('Retry failed — check the deploy log under Cluster Detail.');
    } finally {
      setRetrying(null);
    }
  };

  // Distinct env values across the cluster list — drives the dropdown.
  const envOptions = useMemo(() => {
    const set = new Set<string>();
    clusters.forEach((c) => c.environment && set.add(c.environment));
    return Array.from(set).sort();
  }, [clusters]);

  const filtered = useMemo(() => {
    const needle = search.trim().toLowerCase();
    return clusters.filter((c) => {
      if (kindFilter !== 'all' && (c.kind ?? 'managed') !== kindFilter) return false;
      if (envFilter && (c.environment ?? '') !== envFilter) return false;
      if (!needle) return true;
      return (
        c.name.toLowerCase().includes(needle) ||
        c.kafka_version.toLowerCase().includes(needle) ||
        (c.environment ?? '').toLowerCase().includes(needle)
      );
    });
  }, [clusters, search, envFilter, kindFilter]);

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

      {clusters.length > 0 && (
        <div className="bg-white border rounded-xl p-3 mb-4 flex flex-wrap gap-3 items-center">
          <div className="relative flex-1 min-w-[200px]">
            <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
            <input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search by name, version, environment"
              className="w-full pl-9 pr-3 py-2 border rounded text-sm"
            />
          </div>
          <select
            value={kindFilter}
            onChange={(e) => setKindFilter(e.target.value as typeof kindFilter)}
            className="px-3 py-2 border rounded text-sm"
          >
            <option value="all">All kinds</option>
            <option value="managed">Managed</option>
            <option value="external">External</option>
          </select>
          {envOptions.length > 0 && (
            <select
              value={envFilter}
              onChange={(e) => setEnvFilter(e.target.value)}
              className="px-3 py-2 border rounded text-sm"
            >
              <option value="">All environments</option>
              {envOptions.map((e) => <option key={e} value={e}>{e}</option>)}
            </select>
          )}
          {(search || envFilter || kindFilter !== 'all') && (
            <button
              onClick={() => { setSearch(''); setEnvFilter(''); setKindFilter('all'); }}
              className="px-3 py-2 text-sm border rounded hover:bg-gray-50 flex items-center gap-1"
            >
              <Filter size={12} /> Reset
            </button>
          )}
          <span className="text-xs text-gray-400 ml-auto">{filtered.length} of {clusters.length}</span>
        </div>
      )}

      {clusters.length === 0 ? (
        <div className="text-center py-12 text-gray-500">
          <Network size={48} className="mx-auto mb-4 text-gray-300" />
          <p>No clusters yet. Create your first Kafka cluster.</p>
        </div>
      ) : filtered.length === 0 ? (
        <div className="text-center py-12 text-gray-400 text-sm">
          No clusters match the current filters.
        </div>
      ) : (
        <div className="space-y-3">
          {filtered.map(cluster => {
            const isExternal = cluster.kind === 'external';
            const Icon = isExternal ? Plug : Network;
            const isErrored = cluster.state === 'error';
            return (
            <div key={cluster.id} className="flex items-center justify-between bg-white border rounded-xl p-5 shadow-sm">
              <Link to={`/clusters/${cluster.id}`} className="flex items-center gap-4 flex-1">
                <Icon size={20} className={isExternal ? 'text-purple-500' : 'text-gray-400'} />
                <div>
                  <div className="font-semibold text-gray-900 flex items-center gap-2 flex-wrap">
                    {cluster.name}
                    {isExternal && (
                      <span className="px-2 py-0.5 rounded text-xs font-medium bg-purple-50 text-purple-700 border border-purple-200">
                        external
                      </span>
                    )}
                    {envBadge(cluster.environment)}
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
                {isErrored && !isExternal && (
                  <button
                    onClick={() => handleRetry(cluster)}
                    disabled={retrying === cluster.id}
                    className="flex items-center gap-1 px-3 py-1.5 text-xs bg-amber-600 text-white rounded hover:bg-amber-700 disabled:opacity-50"
                    title="Retry the failed deployment"
                  >
                    <RotateCw size={12} className={retrying === cluster.id ? 'animate-spin' : ''} />
                    Retry deploy
                  </button>
                )}
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
