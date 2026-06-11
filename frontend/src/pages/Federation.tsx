import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { Globe2, Search, RefreshCw, Loader2, AlertTriangle, Edit2, X} from 'lucide-react';
import { getFederationOverview, federationTopicSearch, patchCluster, type FederationCluster, type FederationMatch } from '../lib/api';

const ENV_BADGE: Record<string, string> = {
  prod: 'bg-red-100 text-red-800 border-red-300',
  staging: 'bg-amber-50 text-amber-700 border-amber-200',
  qa: 'bg-blue-50 text-blue-700 border-blue-200',
  dev: 'bg-emerald-50 text-emerald-700 border-emerald-200',
  fdr: 'bg-purple-100 text-purple-800 border-purple-300',
};

export default function Federation() {
  const [overview, setOverview] = useState<{
    clusters: FederationCluster[]; total: number; managed: number; external: number;
  } | null>(null);
  const [loading, setLoading] = useState(true);
  const [envFilter, setEnvFilter] = useState<string>('all');
  const [editingEnv, setEditingEnv] = useState<{ clusterId: string; currentEnv: string } | null>(null);
  const [newEnv, setNewEnv] = useState('');

  const [q, setQ] = useState('');
  const [searching, setSearching] = useState(false);
  const [searchResult, setSearchResult] = useState<{
    matches: FederationMatch[]; match_count: number; skipped: Array<{ name: string; reason: string }>;
  } | null>(null);

  const fetchOverview = async () => {
    setLoading(true);
    try {
      const data = await getFederationOverview();
      setOverview(data);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchOverview(); }, []);

  const handleEnvEdit = (clusterId: string, currentEnv: string) => {
    setEditingEnv({ clusterId, currentEnv });
    setNewEnv(currentEnv || '');
  };

  const handleEnvSave = async () => {
    if (!editingEnv) return;
    try {
      await patchCluster(editingEnv.clusterId, { environment: newEnv });
      await fetchOverview();
      setEditingEnv(null);
      setNewEnv('');
    } catch (error) {
      console.error('Failed to update environment:', error);
    }
  };

  const handleEnvCancel = () => {
    setEditingEnv(null);
    setNewEnv('');
  };

  const onSearch = async () => {
    if (!q.trim()) return;
    setSearching(true);
    try {
      const r = await federationTopicSearch(q.trim());
      setSearchResult(r);
    } finally {
      setSearching(false);
    }
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <div>
          <h1 className="text-xl font-semibold flex items-center gap-2">
            <Globe2 size={20} className="text-blue-600" /> Cluster Overview
          </h1>
          <p className="text-sm text-gray-500 mt-1">
            Single pane of glass across every cluster Tantor manages — managed and external.
          </p>
        </div>
        <button onClick={fetchOverview} className="flex items-center gap-1.5 px-3 py-1.5 text-xs border rounded-lg hover:bg-gray-50">
          <RefreshCw size={13} /> Refresh
        </button>
      </div>

      {loading ? (
        <div className="flex items-center gap-2 text-sm text-gray-500">
          <Loader2 size={14} className="animate-spin" /> Loading…
        </div>
      ) : !overview ? (
        <div className="text-sm text-gray-500">Failed to load federation overview.</div>
      ) : (
        <>
          <div className="grid grid-cols-3 gap-3 mb-5">
            <Stat label="Total clusters" value={overview.total.toString()} />
            <Stat label="Managed" value={overview.managed.toString()} />
            <Stat label="External" value={overview.external.toString()} />
          </div>

          <div className="flex items-center gap-3 mb-4">
            <label className="text-sm font-medium text-gray-700">Filter by Environment:</label>
            <select
              value={envFilter}
              onChange={e => setEnvFilter(e.target.value)}
              className="px-3 py-1.5 border rounded-lg text-sm bg-white"
            >
              <option value="all">All Environments</option>
              <option value="dev">dev</option>
              <option value="qa">qa</option>
              <option value="staging">staging</option>
              <option value="prod">prod</option>
              <option value="fdr">fdr</option>
            </select>
          </div>

          <div className="border rounded-lg overflow-hidden mb-6">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 border-b">
                <tr>
                  <th className="text-left px-3 py-2 font-medium">Cluster</th>
                  <th className="text-left px-3 py-2 font-medium">Kind</th>
                  <th className="text-left px-3 py-2 font-medium">State</th>
                  <th className="text-left px-3 py-2 font-medium">Env</th>
                  <th className="text-left px-3 py-2 font-medium">Brokers</th>
                </tr>
              </thead>
              <tbody>
                {overview.clusters.length === 0 ? (
                  <tr><td colSpan={5} className="px-3 py-6 text-center text-gray-500 italic">No clusters yet</td></tr>
                ) : overview.clusters.filter(c => envFilter === 'all' || c.environment?.toLowerCase() === envFilter).map(c => (
                  <tr key={c.id} className="border-t hover:bg-gray-50">
                    <td className="px-3 py-2 font-medium">
                      <Link to={`/clusters/${c.id}`} className="text-blue-600 hover:underline">
                        {c.name}
                      </Link>
                    </td>
                    <td className="px-3 py-2">
                      <span className={`px-2 py-0.5 rounded text-xs ${c.kind === 'managed' ? 'bg-blue-50 text-blue-700' : 'bg-purple-50 text-purple-700'}`}>
                        {c.kind}
                      </span>
                    </td>
                    <td className="px-3 py-2">
                      <span className={`px-2 py-0.5 rounded text-xs ${
                        c.state === 'running' || c.state === 'connected' ? 'bg-green-100 text-green-700' :
                        c.state === 'error' ? 'bg-red-100 text-red-700' :
                        'bg-gray-100 text-gray-600'
                      }`}>{c.state}</span>
                    </td>
                    <td className="px-3 py-2">
                      {editingEnv?.clusterId === c.id ? (
                        <div className="flex items-center gap-1">
                          <select
                            value={newEnv}
                            onChange={e => setNewEnv(e.target.value)}
                            className="px-2 py-1 text-xs border rounded"
                          >
                            <option value="">—</option>
                            <option value="dev">dev</option>
                            <option value="qa">qa</option>
                            <option value="staging">staging</option>
                            <option value="prod">prod</option>
                            <option value="fdr">fdr</option>
                          </select>
                          <button onClick={handleEnvSave} className="p-1 text-green-600 hover:bg-green-50 rounded">
                            <Edit2 size={12} />
                          </button>
                          <button onClick={handleEnvCancel} className="p-1 text-red-600 hover:bg-red-50 rounded">
                            <X size={12} />
                          </button>
                        </div>
                      ) : (
                        <div className="flex items-center gap-1">
                          {c.environment ? (
                            <span className={`px-2 py-0.5 rounded text-xs border ${ENV_BADGE[c.environment.toLowerCase()] || 'bg-gray-50 text-gray-600 border-gray-200'}`}>
                              {c.environment}
                            </span>
                          ) : <span className="text-gray-400 text-xs">—</span>}
                          {c.kind === 'external' && (
                            <button
                              onClick={() => handleEnvEdit(c.id, c.environment || '')}
                              className="p-0.5 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded"
                              title="Edit environment"
                            >
                              <Edit2 size={11} />
                            </button>
                          )}
                        </div>
                      )}
                    </td>
                    <td className="px-3 py-2">{c.broker_count ?? '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="border rounded-lg p-4">
            <h3 className="text-sm font-semibold text-gray-700 mb-2 flex items-center gap-2">
              <Search size={16} /> Find a topic across all clusters
            </h3>
            <div className="flex gap-2 mb-3">
              <input
                value={q}
                onChange={e => setQ(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && onSearch()}
                placeholder="topic name (substring match)"
                className="flex-1 px-3 py-1.5 border rounded-lg text-sm"
              />
              <button
                onClick={onSearch}
                disabled={searching || !q.trim()}
                className="flex items-center gap-1.5 px-4 py-1.5 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 disabled:opacity-50"
              >
                {searching && <Loader2 size={14} className="animate-spin" />}
                Search
              </button>
            </div>
            {searchResult && (
              <div>
                <div className="text-xs text-gray-500 mb-2">
                  {searchResult.match_count} match(es)
                  {searchResult.skipped.length > 0 && (
                    <span className="ml-2 text-yellow-700">
                      <AlertTriangle size={11} className="inline mr-0.5" />
                      Skipped {searchResult.skipped.length} unreachable cluster(s)
                    </span>
                  )}
                </div>
                {searchResult.matches.length === 0 ? (
                  <div className="text-sm text-gray-500 italic">No topics matched.</div>
                ) : (
                  <table className="w-full text-sm">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="text-left px-2 py-1.5 font-medium">Topic</th>
                        <th className="text-left px-2 py-1.5 font-medium">Cluster</th>
                        <th className="text-left px-2 py-1.5 font-medium">Env</th>
                        <th className="text-left px-2 py-1.5 font-medium">Partitions</th>
                        <th className="text-left px-2 py-1.5 font-medium">RF</th>
                      </tr>
                    </thead>
                    <tbody>
                      {searchResult.matches.map((m, i) => (
                        <tr key={i} className="border-t">
                          <td className="px-2 py-1.5 font-mono text-xs">{m.topic}</td>
                          <td className="px-2 py-1.5">
                            <Link to={`/clusters/${m.cluster_id}`} className="text-blue-600 hover:underline">
                              {m.cluster_name}
                            </Link>
                            <span className="ml-1 text-xs text-gray-500">({m.cluster_kind})</span>
                          </td>
                          <td className="px-2 py-1.5 text-xs">{m.environment || '—'}</td>
                          <td className="px-2 py-1.5">{m.partitions}</td>
                          <td className="px-2 py-1.5">{m.replication_factor}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="border rounded-lg px-3 py-2 bg-white">
      <div className="text-[11px] uppercase text-gray-500 tracking-wide">{label}</div>
      <div className="text-2xl font-semibold mt-0.5 text-gray-900">{value}</div>
    </div>
  );
}
