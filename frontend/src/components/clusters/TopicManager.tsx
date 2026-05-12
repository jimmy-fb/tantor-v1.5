import { useState, useEffect, useCallback, useRef } from 'react';
import { Plus, Trash2, RefreshCw, ChevronDown, ChevronUp, Loader2, Search, AlertCircle, Settings, Save } from 'lucide-react';
import type { TopicInfo, TopicDetail } from '../../types';
import { getTopics, getTopic, createTopic, deleteTopic, updateTopicConfig, updateTopicPartitions } from '../../lib/api';

interface Props {
  clusterId: string;
}

export default function TopicManager({ clusterId }: Props) {
  const [topics, setTopics] = useState<TopicInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [expandedDetail, setExpandedDetail] = useState<TopicDetail | null>(null);
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<string | null>(null);
  const [search, setSearch] = useState('');

  // Create form state
  const [newName, setNewName] = useState('');
  const [newPartitions, setNewPartitions] = useState(3);
  const [newRF, setNewRF] = useState(1);

  // v1.4.3 #9 — separate visible (user-triggered) and silent
  // (background poll) fetches so the page doesn't flash back into
  // the loading state every 10s.
  const fetchTopics = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    setError(null);
    try {
      const data = await getTopics(clusterId, search || undefined);
      setTopics(data);
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      setError(msg || 'Failed to load topics. Is the broker reachable?');
      // Don't blow away the existing list on a transient poll failure.
      if (!silent) setTopics([]);
    } finally {
      if (!silent) setLoading(false);
    }
  }, [clusterId, search]);

  // Single effect for initial load + search-triggered refetch. NOT in
  // the poll loop so the spinner doesn't reappear every 10s.
  useEffect(() => {
    fetchTopics(false);
  }, [fetchTopics]);

  // Background poll — silent (no loading spinner, no list wipe). Uses
  // a ref so the interval doesn't re-fire when fetchTopics identity
  // changes (search input typing), which was the source of #9.
  const fetchRef = useRef(fetchTopics);
  fetchRef.current = fetchTopics;
  useEffect(() => {
    const interval = setInterval(() => {
      if (!document.hidden) fetchRef.current(true);
    }, 10000);
    return () => clearInterval(interval);
  }, []);

  const handleExpand = async (name: string) => {
    if (expanded === name) {
      setExpanded(null);
      setExpandedDetail(null);
      return;
    }
    setExpanded(name);
    try {
      const detail = await getTopic(clusterId, name);
      setExpandedDetail(detail);
    } catch {
      setExpandedDetail(null);
    }
  };

  const handleCreate = async () => {
    const name = newName.trim();
    if (!name) return;
    setCreating(true);
    setCreateError(null);
    try {
      await createTopic(clusterId, {
        name,
        partitions: newPartitions,
        replication_factor: newRF,
      });
      // Optimistic insert + immediate refetch + retry once after 1.5s.
      // Kafka's create_topics is synchronous on the broker but admin clients
      // sometimes show stale metadata for a beat; the retry handles that.
      setTopics(prev => prev.some(t => t.name === name)
        ? prev
        : [...prev, { name, partitions: newPartitions, replication_factor: newRF }],
      );
      setNewName('');
      setNewPartitions(3);
      setNewRF(1);
      setShowCreate(false);
      fetchTopics();
      setTimeout(() => fetchTopics(), 1500);
    } catch (e: unknown) {
      const apiErr = (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      setCreateError(apiErr || (e instanceof Error ? e.message : 'Topic creation failed'));
    } finally {
      setCreating(false);
    }
  };

  const handleDelete = async (name: string) => {
    if (!confirm(`Delete topic "${name}"? This cannot be undone.`)) return;
    setDeleting(name);
    try {
      await deleteTopic(clusterId, name);
      fetchTopics();
    } finally {
      setDeleting(null);
    }
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-gray-700">Topics ({topics.length})</h3>
        <div className="flex gap-2">
          {/* Search */}
          <div className="relative">
            <Search size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
            <input
              type="text"
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder="Search topics..."
              className="pl-8 pr-3 py-1.5 text-xs border rounded-lg w-48 focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <button
            onClick={() => fetchTopics(false)}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs border rounded-lg hover:bg-gray-50"
          >
            <RefreshCw size={13} /> Refresh
          </button>
          <button
            onClick={() => setShowCreate(!showCreate)}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-blue-600 text-white rounded-lg hover:bg-blue-700"
          >
            <Plus size={13} /> Create Topic
          </button>
        </div>
      </div>

      {/* Error banner */}
      {error && (
        <div className="flex items-center gap-2 bg-red-50 border border-red-200 rounded-lg px-4 py-3 mb-4 text-sm text-red-700">
          <AlertCircle size={16} />
          <span>{error}</span>
          <button onClick={() => fetchTopics(false)} className="ml-auto text-xs underline hover:no-underline">Retry</button>
        </div>
      )}

      {/* Create form */}
      {showCreate && (
        <div className="bg-blue-50 border border-blue-200 rounded-xl p-4 mb-4">
          <h4 className="text-sm font-medium text-gray-800 mb-3">Create New Topic</h4>
          <div className="grid grid-cols-3 gap-3">
            <div>
              <label className="block text-xs text-gray-600 mb-1">Topic Name</label>
              <input
                type="text"
                value={newName}
                onChange={e => setNewName(e.target.value)}
                placeholder="my-topic"
                className="w-full px-2.5 py-1.5 border rounded-lg text-sm"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-600 mb-1">Partitions</label>
              <input
                type="number"
                min={1}
                value={newPartitions}
                onChange={e => setNewPartitions(Number(e.target.value))}
                className="w-full px-2.5 py-1.5 border rounded-lg text-sm"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-600 mb-1">Replication Factor</label>
              <input
                type="number"
                min={1}
                value={newRF}
                onChange={e => setNewRF(Number(e.target.value))}
                className="w-full px-2.5 py-1.5 border rounded-lg text-sm"
              />
            </div>
          </div>
          {createError && (
            <div className="mt-3 text-sm text-red-700 bg-red-50 border border-red-200 rounded p-2 px-3">
              {createError}
            </div>
          )}
          <div className="flex gap-2 mt-3">
            <button
              onClick={handleCreate}
              disabled={creating || !newName.trim()}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50"
            >
              {creating ? <Loader2 size={13} className="animate-spin" /> : null}
              Create
            </button>
            <button
              onClick={() => setShowCreate(false)}
              className="px-3 py-1.5 text-xs border rounded-lg hover:bg-gray-50"
            >
              Cancel
            </button>
          </div>
        </div>
      )}

      {loading ? (
        <div className="flex items-center justify-center gap-2 py-8 text-gray-400 text-sm">
          <Loader2 size={16} className="animate-spin" /> Loading topics...
        </div>
      ) : topics.length === 0 && !error ? (
        <div className="text-center py-8 text-gray-400 text-sm">
          {search ? `No topics matching "${search}"` : 'No topics found. Create one to get started.'}
        </div>
      ) : (
        <div className="bg-white border rounded-xl overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-gray-500 bg-gray-50">
                <th className="px-4 py-2.5 font-medium">Name</th>
                <th className="px-4 py-2.5 font-medium">Partitions</th>
                <th className="px-4 py-2.5 font-medium">Replication</th>
                <th className="px-4 py-2.5 font-medium w-20"></th>
              </tr>
            </thead>
            <tbody>
              {topics.map(topic => (
                <TopicRow
                  key={topic.name}
                  topic={topic}
                  clusterId={clusterId}
                  expanded={expanded === topic.name}
                  expandedDetail={expanded === topic.name ? expandedDetail : null}
                  deleting={deleting === topic.name}
                  onExpand={() => handleExpand(topic.name)}
                  onDelete={() => handleDelete(topic.name)}
                  onRefresh={fetchTopics}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

const RETENTION_PRESETS = [
  { label: '1 hour', value: '3600000' },
  { label: '1 day', value: '86400000' },
  { label: '7 days', value: '604800000' },
  { label: '30 days', value: '2592000000' },
  { label: 'Custom', value: 'custom' },
];

function formatRetention(ms: string): string {
  const n = parseInt(ms, 10);
  if (isNaN(n)) return ms;
  if (n < 60000) return `${n} ms`;
  if (n < 3600000) return `${Math.round(n / 60000)} min`;
  if (n < 86400000) return `${Math.round(n / 3600000)} hr`;
  return `${Math.round(n / 86400000)} day(s)`;
}

function TopicRow({
  topic, clusterId, expanded, expandedDetail, deleting, onExpand, onDelete, onRefresh,
}: {
  topic: TopicInfo;
  clusterId: string;
  expanded: boolean;
  expandedDetail: TopicDetail | null;
  deleting: boolean;
  onExpand: () => void;
  onDelete: () => void;
  onRefresh: () => void;
}) {
  const [showSettings, setShowSettings] = useState(false);
  const [retentionPreset, setRetentionPreset] = useState('');
  const [retentionCustom, setRetentionCustom] = useState('');
  const [newPartitionCount, setNewPartitionCount] = useState<number>(0);
  const [savingConfig, setSavingConfig] = useState(false);
  const [savingPartitions, setSavingPartitions] = useState(false);
  const [settingsMsg, setSettingsMsg] = useState<{ type: 'ok' | 'err'; text: string } | null>(null);

  // Initialize form values from topic detail when settings are opened
  useEffect(() => {
    if (showSettings && expandedDetail) {
      const currentRetention = expandedDetail.configs?.['retention.ms'] || '';
      const matchingPreset = RETENTION_PRESETS.find(p => p.value === currentRetention);
      if (matchingPreset) {
        setRetentionPreset(currentRetention);
        setRetentionCustom('');
      } else if (currentRetention) {
        setRetentionPreset('custom');
        setRetentionCustom(currentRetention);
      } else {
        setRetentionPreset('');
        setRetentionCustom('');
      }
      setNewPartitionCount(expandedDetail.partitions || 0);
    }
  }, [showSettings, expandedDetail]);

  const handleSaveConfig = async () => {
    const retVal = retentionPreset === 'custom' ? retentionCustom : retentionPreset;
    if (!retVal) return;
    setSavingConfig(true);
    setSettingsMsg(null);
    try {
      await updateTopicConfig(clusterId, topic.name, { 'retention.ms': retVal });
      setSettingsMsg({ type: 'ok', text: 'Retention updated successfully' });
      onRefresh();
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      setSettingsMsg({ type: 'err', text: msg || 'Failed to update config' });
    } finally {
      setSavingConfig(false);
    }
  };

  const handleSavePartitions = async () => {
    if (!newPartitionCount || newPartitionCount <= (expandedDetail?.partitions || 0)) return;
    setSavingPartitions(true);
    setSettingsMsg(null);
    try {
      await updateTopicPartitions(clusterId, topic.name, newPartitionCount);
      setSettingsMsg({ type: 'ok', text: `Partitions increased to ${newPartitionCount}` });
      onRefresh();
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      setSettingsMsg({ type: 'err', text: msg || 'Failed to update partitions' });
    } finally {
      setSavingPartitions(false);
    }
  };

  return (
    <>
      <tr className="border-b last:border-0 hover:bg-gray-50 cursor-pointer" onClick={onExpand}>
        <td className="px-4 py-2.5 font-mono text-gray-900">{topic.name}</td>
        <td className="px-4 py-2.5 text-gray-600">{topic.partitions}</td>
        <td className="px-4 py-2.5 text-gray-600">{topic.replication_factor}</td>
        <td className="px-4 py-2.5">
          <div className="flex items-center gap-2">
            <button
              onClick={e => { e.stopPropagation(); onDelete(); }}
              disabled={deleting}
              className="p-1 text-gray-400 hover:text-red-600 rounded"
            >
              {deleting ? <Loader2 size={14} className="animate-spin" /> : <Trash2 size={14} />}
            </button>
            {expanded ? <ChevronUp size={14} className="text-gray-400" /> : <ChevronDown size={14} className="text-gray-400" />}
          </div>
        </td>
      </tr>
      {expanded && expandedDetail && (
        <tr>
          <td colSpan={4} className="bg-gray-50 px-4 py-3">
            <div className="text-xs text-gray-600 space-y-3">
              {/* Configs */}
              {expandedDetail.configs && Object.keys(expandedDetail.configs).length > 0 && (
                <div>
                  <h5 className="font-semibold text-gray-700 mb-1">Configuration</h5>
                  <div className="grid grid-cols-2 gap-1 font-mono">
                    {Object.entries(expandedDetail.configs).map(([k, v]) => (
                      <div key={k}>
                        <span className="text-gray-500">{k}=</span>
                        {v}
                        {k === 'retention.ms' && <span className="text-gray-400 ml-1">({formatRetention(String(v))})</span>}
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {/* Partition details with ISR color coding */}
              <div>
                <h5 className="font-semibold text-gray-700 mb-1">Partition Details</h5>
                <table className="w-full">
                  <thead>
                    <tr className="text-left text-gray-500">
                      <th className="pr-4 pb-1 font-medium">Partition</th>
                      <th className="pr-4 pb-1 font-medium">Leader</th>
                      <th className="pr-4 pb-1 font-medium">Replicas</th>
                      <th className="pr-4 pb-1 font-medium">ISR</th>
                    </tr>
                  </thead>
                  <tbody>
                    {expandedDetail.partition_details.map((p, i) => {
                      const replicas = Array.isArray(p.replicas) ? p.replicas as number[] : [];
                      const isr = Array.isArray(p.isr) ? p.isr as number[] : [];
                      const isrFull = replicas.length > 0 && isr.length === replicas.length;
                      const isrEmpty = replicas.length > 0 && isr.length === 0;
                      const isrColor = isrFull
                        ? 'text-green-600'
                        : isrEmpty
                          ? 'text-red-600 font-semibold'
                          : 'text-yellow-600 font-semibold';
                      return (
                        <tr key={i} className="font-mono">
                          <td className="pr-4 py-0.5">{String(p.partition ?? i)}</td>
                          <td className="pr-4 py-0.5">{String(p.leader ?? '-')}</td>
                          <td className="pr-4 py-0.5">{replicas.length > 0 ? replicas.join(', ') : String(p.replicas ?? '-')}</td>
                          <td className={`pr-4 py-0.5 ${isrColor}`}>
                            {isr.length > 0 ? isr.join(', ') : String(p.isr ?? '-')}
                            {!isrFull && replicas.length > 0 && (
                              <span className="ml-1 text-[10px]">({isr.length}/{replicas.length})</span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              {/* Edit Settings toggle */}
              <div>
                <button
                  onClick={() => { setShowSettings(!showSettings); setSettingsMsg(null); }}
                  className="flex items-center gap-1.5 text-xs text-blue-600 hover:text-blue-800 font-medium"
                >
                  <Settings size={13} />
                  {showSettings ? 'Hide Settings' : 'Edit Settings'}
                </button>
              </div>

              {/* Edit Settings panel */}
              {showSettings && (
                <div className="bg-white border border-blue-200 rounded-lg p-3 space-y-3">
                  {settingsMsg && (
                    <div className={`text-xs px-2 py-1 rounded ${settingsMsg.type === 'ok' ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-700'}`}>
                      {settingsMsg.text}
                    </div>
                  )}

                  {/* Retention config */}
                  <div>
                    <label className="block text-xs font-medium text-gray-700 mb-1">Retention Period</label>
                    <div className="flex items-center gap-2">
                      <select
                        value={retentionPreset}
                        onChange={e => { setRetentionPreset(e.target.value); if (e.target.value !== 'custom') setRetentionCustom(''); }}
                        className="px-2 py-1.5 border rounded text-xs"
                      >
                        <option value="">-- select --</option>
                        {RETENTION_PRESETS.map(p => (
                          <option key={p.value} value={p.value}>{p.label}</option>
                        ))}
                      </select>
                      {retentionPreset === 'custom' && (
                        <input
                          type="text"
                          value={retentionCustom}
                          onChange={e => setRetentionCustom(e.target.value)}
                          placeholder="e.g. 172800000"
                          className="px-2 py-1.5 border rounded text-xs w-36"
                        />
                      )}
                      <button
                        onClick={handleSaveConfig}
                        disabled={savingConfig || (!retentionPreset || (retentionPreset === 'custom' && !retentionCustom))}
                        className="flex items-center gap-1 px-2.5 py-1.5 text-xs bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
                      >
                        {savingConfig ? <Loader2 size={12} className="animate-spin" /> : <Save size={12} />}
                        Save
                      </button>
                    </div>
                  </div>

                  {/* Partition count */}
                  <div>
                    <label className="block text-xs font-medium text-gray-700 mb-1">
                      Partition Count (current: {expandedDetail.partitions})
                    </label>
                    <div className="flex items-center gap-2">
                      <input
                        type="number"
                        min={expandedDetail.partitions + 1}
                        value={newPartitionCount}
                        onChange={e => setNewPartitionCount(Number(e.target.value))}
                        className="px-2 py-1.5 border rounded text-xs w-24"
                      />
                      <button
                        onClick={handleSavePartitions}
                        disabled={savingPartitions || newPartitionCount <= expandedDetail.partitions}
                        className="flex items-center gap-1 px-2.5 py-1.5 text-xs bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
                      >
                        {savingPartitions ? <Loader2 size={12} className="animate-spin" /> : <Save size={12} />}
                        Save
                      </button>
                    </div>
                    <p className="text-[10px] text-gray-400 mt-0.5">Partitions can only be increased, never decreased.</p>
                  </div>
                </div>
              )}
            </div>
          </td>
        </tr>
      )}
    </>
  );
}
