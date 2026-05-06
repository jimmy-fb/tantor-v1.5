import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { Globe2, Search, RefreshCw, Loader2, AlertTriangle, ChevronRight } from 'lucide-react';
import { getFederationOverview, federationTopicSearch } from '../lib/api';

type Cluster = {
  id: string;
  name: string;
  kind: 'managed' | 'external';
  state: string;
  environment: string;
  kafka_version: string;
  mode: string;
  broker_count: number | null;
  topic_count: number | null;
  bootstrap_servers: string | null;
};

type Match = {
  cluster_id: string;
  cluster_name: string;
  cluster_kind: 'managed' | 'external';
  environment: string;
  topic: string;
  partitions: number;
  replication_factor: number;
};

const ENV_BADGE: Record<string, string> = {
  prod: 'bg-red-50 text-red-700 border-red-200',
  staging: 'bg-amber-50 text-amber-700 border-amber-200',
  qa: 'bg-blue-50 text-blue-700 border-blue-200',
  dev: 'bg-emerald-50 text-emerald-700 border-emerald-200',
};

export default function Federation() {
  const [overview, setOverview] = useState<{
    clusters: Cluster[]; total: number; managed: number; external: number;
  } | null>(null);
  const [loading, setLoading] = useState(true);

  const [q, setQ] = useState('');
  const [searching, setSearching] = useState(false);
  const [searchResult, setSearchResult] = useState<{
    matches: Match[]; match_count: number; skipped: Array<{ name: string; reason: string }>;
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
            <Globe2 size={20} className="text-blue-600" /> Data Federation
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

          <div className="border rounded-lg overflow-hidden mb-6">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 border-b">
                <tr>
                  <th className="text-left px-3 py-2 font-medium">Cluster</th>
                  <th className="text-left px-3 py-2 font-medium">Kind</th>
                  <th className="text-left px-3 py-2 font-medium">State</th>
                  <th className="text-left px-3 py-2 font-medium">Env</th>
                  <th className="text-left px-3 py-2 font-medium">Brokers</th>
                  <th className="text-left px-3 py-2 font-medium">Topics</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {overview.clusters.length === 0 ? (
                  <tr><td colSpan={7} className="px-3 py-6 text-center text-gray-500 italic">No clusters yet</td></tr>
                ) : overview.clusters.map(c => (
                  <tr key={c.id} className="border-t hover:bg-gray-50">
                    <td className="px-3 py-2 font-medium">{c.name}</td>
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
                      {c.environment ? (
                        <span className={`px-2 py-0.5 rounded text-xs border ${ENV_BADGE[c.environment.toLowerCase()] || 'bg-gray-50 text-gray-600 border-gray-200'}`}>
                          {c.environment}
                        </span>
                      ) : <span className="text-gray-400 text-xs">—</span>}
                    </td>
                    <td className="px-3 py-2">{c.broker_count ?? '—'}</td>
                    <td className="px-3 py-2">{c.topic_count ?? <span className="text-gray-400 italic">unreachable</span>}</td>
                    <td className="px-3 py-2 text-right">
                      <Link to={`/clusters/${c.id}`} className="text-blue-600 hover:underline text-xs inline-flex items-center gap-0.5">
                        Open <ChevronRight size={12} />
                      </Link>
                    </td>
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
