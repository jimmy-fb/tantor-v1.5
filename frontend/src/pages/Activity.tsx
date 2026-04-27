import { useEffect, useMemo, useState } from 'react';
import { Activity as ActivityIcon, RefreshCw, Search, Filter } from 'lucide-react';
import { getActivity, getClusters, type ActivityEntry } from '../lib/api';

interface ClusterOption {
  id: string;
  name: string;
}

const PAGE_SIZE = 100;

export default function ActivityPage() {
  const [entries, setEntries] = useState<ActivityEntry[]>([]);
  const [clusters, setClusters] = useState<ClusterOption[]>([]);
  const [clusterId, setClusterId] = useState<string>('');
  const [kind, setKind] = useState<'all' | 'security' | 'config'>('all');
  const [query, setQuery] = useState('');
  const [page, setPage] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    getClusters()
      .then((cs) => setClusters(cs.map((c) => ({ id: c.id, name: c.name }))))
      .catch(() => setClusters([]));
  }, []);

  const load = useMemo(
    () => async () => {
      setLoading(true);
      setError('');
      try {
        const resp = await getActivity({
          cluster_id: clusterId || undefined,
          kind: kind === 'all' ? undefined : kind,
          q: query || undefined,
          limit: PAGE_SIZE,
          offset: page * PAGE_SIZE,
        });
        setEntries(resp.entries);
        setHasMore(resp.has_more);
      } catch (err: unknown) {
        const e = err as { response?: { data?: { detail?: string } } };
        setError(e.response?.data?.detail || 'Failed to load activity');
        setEntries([]);
        setHasMore(false);
      } finally {
        setLoading(false);
      }
    },
    [clusterId, kind, query, page],
  );

  useEffect(() => {
    load();
  }, [load]);

  const reset = () => {
    setPage(0);
    setQuery('');
    setClusterId('');
    setKind('all');
  };

  return (
    <div className="p-8 max-w-7xl mx-auto">
      <div className="flex items-start justify-between mb-6">
        <div>
          <h1 className="text-2xl font-semibold flex items-center gap-2">
            <ActivityIcon size={24} /> Activity
          </h1>
          <p className="text-sm text-gray-500 mt-1">
            Cross-cluster timeline of security actions and broker config changes.
          </p>
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="px-3 py-1.5 text-sm border rounded hover:bg-gray-50 flex items-center gap-2 disabled:opacity-50"
        >
          <RefreshCw size={14} className={loading ? 'animate-spin' : ''} /> Refresh
        </button>
      </div>

      {/* Filters */}
      <div className="bg-white border rounded-lg p-4 mb-4 flex flex-wrap gap-3 items-end">
        <div className="flex-1 min-w-[200px]">
          <label className="block text-xs text-gray-500 mb-1">Search</label>
          <div className="relative">
            <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
            <input
              value={query}
              onChange={(e) => {
                setPage(0);
                setQuery(e.target.value);
              }}
              placeholder="action, resource, actor, details"
              className="w-full pl-9 pr-3 py-2 border rounded text-sm"
            />
          </div>
        </div>

        <div>
          <label className="block text-xs text-gray-500 mb-1">Cluster</label>
          <select
            value={clusterId}
            onChange={(e) => {
              setPage(0);
              setClusterId(e.target.value);
            }}
            className="px-3 py-2 border rounded text-sm min-w-[200px]"
          >
            <option value="">All clusters</option>
            {clusters.map((c) => (
              <option key={c.id} value={c.id}>
                {c.name}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label className="block text-xs text-gray-500 mb-1">Kind</label>
          <select
            value={kind}
            onChange={(e) => {
              setPage(0);
              setKind(e.target.value as 'all' | 'security' | 'config');
            }}
            className="px-3 py-2 border rounded text-sm"
          >
            <option value="all">All</option>
            <option value="security">Security</option>
            <option value="config">Config</option>
          </select>
        </div>

        <button
          onClick={reset}
          className="px-3 py-2 text-sm border rounded hover:bg-gray-50 flex items-center gap-2"
        >
          <Filter size={14} /> Reset
        </button>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded mb-4 text-sm">
          {error}
        </div>
      )}

      {/* Table */}
      <div className="bg-white border rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 text-left text-xs uppercase text-gray-500">
            <tr>
              <th className="px-4 py-2 w-[180px]">When</th>
              <th className="px-4 py-2 w-[80px]">Kind</th>
              <th className="px-4 py-2 w-[160px]">Cluster</th>
              <th className="px-4 py-2 w-[180px]">Action</th>
              <th className="px-4 py-2">Resource</th>
              <th className="px-4 py-2 w-[120px]">Actor</th>
            </tr>
          </thead>
          <tbody>
            {entries.length === 0 && !loading && (
              <tr>
                <td colSpan={6} className="px-4 py-8 text-center text-gray-400">
                  No activity matches these filters.
                </td>
              </tr>
            )}
            {entries.map((e) => (
              <tr key={`${e.kind}-${e.id}`} className="border-t hover:bg-gray-50 align-top">
                <td className="px-4 py-2 text-xs text-gray-600 font-mono">
                  {new Date(e.occurred_at).toLocaleString()}
                </td>
                <td className="px-4 py-2">
                  <span
                    className={
                      'px-2 py-0.5 rounded text-xs font-medium ' +
                      (e.kind === 'security'
                        ? 'bg-amber-50 text-amber-700 border border-amber-200'
                        : 'bg-sky-50 text-sky-700 border border-sky-200')
                    }
                  >
                    {e.kind}
                  </span>
                </td>
                <td className="px-4 py-2 text-gray-700">{e.cluster_name || '—'}</td>
                <td className="px-4 py-2 font-mono text-xs">{e.action}</td>
                <td className="px-4 py-2">
                  <div className="font-mono text-xs">{e.resource}</div>
                  {e.details && (
                    <div className="text-xs text-gray-500 mt-0.5 break-all">{e.details}</div>
                  )}
                </td>
                <td className="px-4 py-2 text-xs text-gray-600">{e.actor || '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div className="flex justify-between items-center mt-4 text-sm text-gray-600">
        <div>
          Page {page + 1} · {entries.length} entries on this page
          {hasMore && ' · more available'}
        </div>
        <div className="flex gap-2">
          <button
            onClick={() => setPage((p) => Math.max(0, p - 1))}
            disabled={page === 0 || loading}
            className="px-3 py-1.5 border rounded disabled:opacity-50"
          >
            Previous
          </button>
          <button
            onClick={() => setPage((p) => p + 1)}
            disabled={!hasMore || loading}
            className="px-3 py-1.5 border rounded disabled:opacity-50"
          >
            Next
          </button>
        </div>
      </div>
    </div>
  );
}
