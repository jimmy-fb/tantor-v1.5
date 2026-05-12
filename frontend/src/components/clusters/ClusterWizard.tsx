import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { ChevronRight, ChevronLeft, Check, Server, Loader2, AlertTriangle } from 'lucide-react';
import type { Host, KafkaVersionInfo, ClusterCreate, ServiceAssignment, ClusterConfig } from '../../types';
import { getHosts, createCluster, getKafkaVersions, getClusters, preflightPorts } from '../../lib/api';

const ROLES = [
  { id: 'broker_controller', label: 'Broker + Controller', description: 'Combined KRaft broker and controller (recommended for small clusters)', color: 'bg-blue-100 text-blue-800 border-blue-200' },
  { id: 'broker', label: 'Broker', description: 'Kafka broker only (data plane)', color: 'bg-green-100 text-green-800 border-green-200' },
  { id: 'controller', label: 'Controller', description: 'KRaft controller only (metadata)', color: 'bg-purple-100 text-purple-800 border-purple-200' },
  { id: 'ksqldb', label: 'ksqlDB', description: 'Stream processing SQL engine', color: 'bg-orange-100 text-orange-800 border-orange-200' },
  { id: 'kafka_connect', label: 'Kafka Connect', description: 'Data integration framework', color: 'bg-teal-100 text-teal-800 border-teal-200' },
  { id: 'zookeeper', label: 'ZooKeeper', description: 'Legacy consensus (Kafka < 4.0 only)', color: 'bg-gray-100 text-gray-800 border-gray-200' },
];

// Roles that are mutually exclusive per host
const EXCLUSIVE_GROUPS: Record<string, string[]> = {
  broker_controller: ['broker', 'controller'],
  broker: ['broker_controller'],
  controller: ['broker_controller'],
};

// Parse major version from a Kafka version string like "3.7.0"
function getMajorVersion(version: string): number {
  const parts = version.split('.');
  return parseInt(parts[0], 10) || 0;
}

export default function ClusterWizard() {
  const navigate = useNavigate();
  const [step, setStep] = useState(0);
  const [hosts, setHosts] = useState<Host[]>([]);
  const [versions, setVersions] = useState<KafkaVersionInfo[]>([]);
  const [versionsLoading, setVersionsLoading] = useState(true);
  const [loading, setLoading] = useState(false);
  const [existingClusters, setExistingClusters] = useState<{ name: string; ports: number[] }[]>([]);

  // Step 1: Cluster basics
  const [name, setName] = useState('');
  const [kafkaVersion, setKafkaVersion] = useState('');
  const [mode, setMode] = useState<'kraft' | 'zookeeper'>('kraft');
  // QA #51: env tag — short label like "dev"/"qa"/"prod" or anything operator picks
  const [environment, setEnvironment] = useState('');

  // Step 2: Role assignment — multi-role per host
  const [assignments, setAssignments] = useState<Record<string, string[]>>({});

  // Step 3: Configuration
  const [config, setConfig] = useState<ClusterConfig>({
    replication_factor: 3,
    num_partitions: 3,
    log_dirs: '/var/lib/kafka/data',
    listener_port: 9092,
    controller_port: 9093,
    heap_size: '1G',
    ksqldb_port: 8088,
    connect_port: 8083,
    connect_rest_port: 8083,
  });

  // Validation warnings
  const [nameError, setNameError] = useState('');
  const [portError, setPortError] = useState('');

  // v1.4.2 — port preflight against the selected hosts.
  const [portCheckLoading, setPortCheckLoading] = useState(false);
  const [portCheckResult, setPortCheckResult] = useState<{
    ok: boolean;
    conflicts: Array<{ host_ip: string; port: number; label: string; process: string }>;
    ssh_failures: Array<{ host_ip: string; error: string }>;
  } | null>(null);

  useEffect(() => {
    getHosts().then(setHosts);
    getClusters().then(clusters => {
      setExistingClusters(clusters.map(c => ({
        name: c.name,
        ports: [], // We'd need config_json to get ports, but name check is most important
      })));
    }).catch(() => {});
    setVersionsLoading(true);
    getKafkaVersions()
      .then(data => {
        setVersions(data);
        const available = data.filter(v => v.available);
        if (available.length > 0 && !kafkaVersion) {
          setKafkaVersion(available[0].version);
        } else if (data.length > 0 && !kafkaVersion) {
          setKafkaVersion(data[0].version);
        }
      })
      .catch(() => setVersions([]))
      .finally(() => setVersionsLoading(false));
  }, []);

  // Auto-force KRaft mode for Kafka 4.x+
  useEffect(() => {
    if (kafkaVersion && getMajorVersion(kafkaVersion) >= 4) {
      setMode('kraft');
    }
  }, [kafkaVersion]);

  // Validate cluster name inline (#17)
  useEffect(() => {
    if (!name.trim()) {
      setNameError('');
      return;
    }
    const duplicate = existingClusters.find(c => c.name.toLowerCase() === name.trim().toLowerCase());
    if (duplicate) {
      setNameError(`A cluster named "${duplicate.name}" already exists`);
    } else {
      setNameError('');
    }
  }, [name, existingClusters]);

  // Validate port inline (#24)
  useEffect(() => {
    if (config.listener_port < 1024) {
      setPortError('Ports below 1024 require root access and may conflict with system services');
    } else if (config.listener_port > 65535) {
      setPortError('Port must be between 1024 and 65535');
    } else {
      setPortError('');
    }
  }, [config.listener_port]);

  const handleAssign = (hostId: string, role: string) => {
    setAssignments(prev => {
      const current = prev[hostId] || [];
      if (current.includes(role)) {
        const next = current.filter(r => r !== role);
        if (next.length === 0) {
          const copy = { ...prev };
          delete copy[hostId];
          return copy;
        }
        return { ...prev, [hostId]: next };
      } else {
        const exclusions = EXCLUSIVE_GROUPS[role] || [];
        const filtered = current.filter(r => !exclusions.includes(r));
        return { ...prev, [hostId]: [...filtered, role] };
      }
    });
  };

  const buildServices = (): ServiceAssignment[] => {
    let brokerNodeId = 1;
    let controllerNodeId = 101;
    const services: ServiceAssignment[] = [];
    for (const [hostId, roles] of Object.entries(assignments)) {
      for (const role of roles) {
        let nodeId: number;
        if (role === 'controller') {
          nodeId = controllerNodeId++;
        } else if (role === 'broker_controller') {
          // Combined role uses broker range
          nodeId = brokerNodeId++;
        } else if (role === 'broker') {
          nodeId = brokerNodeId++;
        } else {
          // ksqldb, kafka_connect, zookeeper — use broker range
          nodeId = brokerNodeId++;
        }
        services.push({ host_id: hostId, role, node_id: nodeId });
      }
    }
    return services;
  };

  const handleCreate = async () => {
    setLoading(true);
    try {
      const data: ClusterCreate = {
        name,
        kafka_version: kafkaVersion,
        mode,
        services: buildServices(),
        config,
        environment: environment.trim().toLowerCase(),
      };
      const cluster = await createCluster(data);
      navigate(`/clusters/${cluster.id}`);
    } finally {
      setLoading(false);
    }
  };

  const assignedRoles = Object.values(assignments).flat();
  const hasBroker = assignedRoles.some(r => r === 'broker' || r === 'broker_controller');
  const availableVersions = versions.filter(v => v.available);
  const selectedVersion = versions.find(v => v.version === kafkaVersion);
  const isKafka4Plus = kafkaVersion ? getMajorVersion(kafkaVersion) >= 4 : false;

  // Compute broker count for replication factor validation (#22)
  const brokerCount = assignedRoles.filter(r => r === 'broker' || r === 'broker_controller').length;
  const rfExceedsBrokers = config.replication_factor > brokerCount && brokerCount > 0;

  // Check if any offline host is selected (#23)
  const offlineHostSelected = Object.keys(assignments).some(hostId => {
    const host = hosts.find(h => h.id === hostId);
    return host && host.status !== 'online';
  });

  // (#29) advertised.listeners hostname looks DNS-resolvable.
  // We can't actually resolve from the browser, but we can warn when an
  // entry isn't an IP literal AND looks like a private hostname (no dot,
  // looks like `kafka1` instead of `kafka-1.prod.example.com`).
  const ipLiteralPattern = /^(\d{1,3}\.){3}\d{1,3}$/;
  const fqdnRiskyHosts = Object.keys(assignments).map(hostId => {
    const host = hosts.find(h => h.id === hostId);
    if (!host) return null;
    if (ipLiteralPattern.test(host.ip_address)) return null;        // IP literals are fine
    if (host.ip_address.includes('.')) return null;                  // FQDN-ish is fine
    return host.hostname;                                            // bare hostname → risky
  }).filter(Boolean) as string[];

  // (#30) listener.security.protocol.map sanity: every listener name in
  // `listeners=` must have a matching protocol map entry. Tantor renders
  // these centrally so the only way a user can break this is by editing
  // server.properties post-deploy — just surface a friendly note.

  // Available roles based on mode
  const availableRoles = ROLES.filter(r => {
    if (mode === 'kraft') return r.id !== 'zookeeper';
    // ZooKeeper mode: hide controller and broker_controller
    return r.id !== 'controller' && r.id !== 'broker_controller';
  });

  const steps = [
    {
      title: 'Cluster Basics',
      content: (
        <div className="space-y-6">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Cluster Name</label>
            <input
              type="text"
              value={name}
              onChange={e => setName(e.target.value)}
              placeholder="my-kafka-cluster"
              className={`w-full px-3 py-2 border rounded-lg text-sm focus:ring-2 focus:ring-blue-500 ${nameError ? 'border-red-400' : ''}`}
            />
            {nameError && (
              <p className="flex items-center gap-1 text-xs text-red-600 mt-1">
                <AlertTriangle size={12} /> {nameError}
              </p>
            )}
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Kafka Version</label>
            {versionsLoading ? (
              <div className="flex items-center gap-2 text-sm text-gray-400 py-2">
                <Loader2 size={14} className="animate-spin" /> Loading available versions...
              </div>
            ) : versions.length === 0 ? (
              <div className="text-sm text-red-500 py-2">
                No versions found. Upload a Kafka binary on the{' '}
                <a href="/versions" className="text-blue-600 underline">Kafka Versions</a> page.
              </div>
            ) : (
              <>
                <select
                  value={kafkaVersion}
                  onChange={e => setKafkaVersion(e.target.value)}
                  className="w-full px-3 py-2 border rounded-lg text-sm focus:ring-2 focus:ring-blue-500"
                >
                  {availableVersions.length > 0 && (
                    <optgroup label="Available (downloaded)">
                      {availableVersions.map(v => (
                        <option key={v.version} value={v.version}>
                          {v.version} ({v.size_mb} MB)
                          {v.release_date ? ` - Released ${v.release_date}` : ''}
                        </option>
                      ))}
                    </optgroup>
                  )}
                  {versions.filter(v => !v.available).length > 0 && (
                    <optgroup label="Not Downloaded (upload required)">
                      {versions.filter(v => !v.available).map(v => (
                        <option key={v.version} value={v.version} disabled>
                          {v.version} - Not available (upload binary first)
                        </option>
                      ))}
                    </optgroup>
                  )}
                </select>
                {selectedVersion && selectedVersion.features && (
                  <div className="mt-2 text-xs text-gray-500">
                    <span className="font-medium">Features:</span>{' '}
                    {selectedVersion.features.slice(0, 3).join(', ')}
                    {selectedVersion.features.length > 3 && ` +${selectedVersion.features.length - 3} more`}
                  </div>
                )}
              </>
            )}
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Environment <span className="text-gray-400 text-xs">(optional)</span></label>
            <div className="flex gap-2 flex-wrap">
              {['dev', 'qa', 'staging', 'prod'].map((e) => (
                <button
                  key={e}
                  type="button"
                  onClick={() => setEnvironment(e === environment ? '' : e)}
                  className={`px-3 py-1.5 text-sm rounded border ${
                    environment === e ? 'bg-blue-50 border-blue-400 text-blue-700' : 'bg-white border-gray-300 text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  {e}
                </button>
              ))}
              <input
                value={environment}
                onChange={(e) => setEnvironment(e.target.value)}
                placeholder="custom tag (e.g. us-east-1)"
                className="flex-1 min-w-[200px] px-3 py-1.5 text-sm border rounded"
              />
            </div>
            <p className="text-xs text-gray-400 mt-1">Used for filtering on the Clusters list. Lowercased.</p>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Consensus Mode</label>
            <div className="grid grid-cols-2 gap-3">
              <button
                onClick={() => setMode('kraft')}
                className={`p-4 border-2 rounded-xl text-left transition-colors ${
                  mode === 'kraft' ? 'border-blue-500 bg-blue-50' : 'border-gray-200 hover:border-gray-300'
                }`}
              >
                <div className="font-semibold text-sm">KRaft</div>
                <div className="text-xs text-gray-500 mt-1">Recommended. Built-in Raft consensus, no ZooKeeper needed.</div>
              </button>
              <button
                onClick={() => !isKafka4Plus && setMode('zookeeper')}
                disabled={isKafka4Plus}
                className={`p-4 border-2 rounded-xl text-left transition-colors ${
                  isKafka4Plus ? 'opacity-50 cursor-not-allowed border-gray-200 bg-gray-50' :
                  mode === 'zookeeper' ? 'border-blue-500 bg-blue-50' : 'border-gray-200 hover:border-gray-300'
                }`}
              >
                <div className="font-semibold text-sm">ZooKeeper</div>
                <div className="text-xs text-gray-500 mt-1">
                  {isKafka4Plus
                    ? 'Not available for Kafka 4.x+. ZooKeeper was removed in Kafka 4.0.'
                    : 'Legacy mode. Requires separate ZooKeeper ensemble.'}
                </div>
              </button>
            </div>
          </div>
        </div>
      ),
      valid: name.trim().length > 0 && kafkaVersion.length > 0 && !nameError,
    },
    {
      title: 'Assign Roles',
      content: (
        <div>
          {hosts.length === 0 ? (
            <div className="text-center py-8 text-gray-500">
              No hosts available. <a href="/hosts" className="text-blue-600 underline">Add hosts first</a>.
            </div>
          ) : (
            <div className="space-y-4">
              <p className="text-sm text-gray-600 mb-4">
                Assign one or more roles to each host. A single host can run multiple services (e.g. Broker+Controller and ksqlDB).
                {mode === 'kraft' && (
                  <span className="block mt-1 text-xs text-gray-400">
                    Node IDs: Brokers start at 1, standalone Controllers start at 101.
                  </span>
                )}
              </p>

              {offlineHostSelected && (
                <div className="flex items-start gap-2 p-3 bg-amber-50 border border-amber-200 rounded-lg text-amber-800 text-sm">
                  <AlertTriangle size={16} className="mt-0.5 flex-shrink-0" />
                  <span>One or more selected hosts are offline. Deployment will fail for offline hosts.</span>
                </div>
              )}

              {fqdnRiskyHosts.length > 0 && (
                <div className="flex items-start gap-2 p-3 bg-amber-50 border border-amber-200 rounded-lg text-amber-800 text-sm">
                  <AlertTriangle size={16} className="mt-0.5 flex-shrink-0" />
                  <span>
                    Host(s) <strong>{fqdnRiskyHosts.join(', ')}</strong> use a bare hostname (no dots,
                    not an IP literal). Tantor will set <code>advertised.listeners</code> to that name
                    — internal brokers resolve it via OS hostname lookup, but external clients in
                    containers / Kubernetes / other VMs may not. Use an FQDN or IP for cross-environment
                    safety.
                  </span>
                </div>
              )}

              {hosts.map(host => {
                const hostRoles = assignments[host.id] || [];
                const isOffline = host.status !== 'online';
                return (
                  <div key={host.id} className={`border rounded-xl p-4 ${isOffline ? 'opacity-60 bg-gray-50' : ''}`}>
                    <div className="flex items-center gap-3 mb-3">
                      <Server size={16} className="text-gray-400" />
                      <div>
                        <span className="font-medium text-sm">{host.hostname}</span>
                        <span className="text-xs text-gray-400 ml-2">{host.ip_address}</span>
                      </div>
                      {hostRoles.length > 0 && (
                        <span className="ml-auto text-xs px-2 py-0.5 rounded-full bg-blue-50 text-blue-600 font-medium">
                          {hostRoles.length} role{hostRoles.length > 1 ? 's' : ''}
                        </span>
                      )}
                      <span className={`${hostRoles.length > 0 ? '' : 'ml-auto'} text-xs px-2 py-0.5 rounded-full ${
                        host.status === 'online' ? 'bg-green-100 text-green-700' :
                        'bg-red-100 text-red-600'
                      }`}>
                        {host.status}
                      </span>
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {availableRoles.map(role => (
                          <button
                            key={role.id}
                            onClick={() => handleAssign(host.id, role.id)}
                            title={isOffline ? `${host.hostname} is offline — deployment will fail` : role.description}
                            className={`px-3 py-1.5 text-xs rounded-lg border transition-all ${
                              hostRoles.includes(role.id)
                                ? role.color + ' ring-2 ring-offset-1 ring-blue-400'
                                : isOffline
                                  ? 'border-gray-200 text-gray-400 cursor-pointer'
                                  : 'border-gray-200 text-gray-600 hover:border-gray-400'
                            }`}
                          >
                            {role.label}
                            {isOffline && hostRoles.includes(role.id) && ' (offline)'}
                          </button>
                        ))}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      ),
      valid: hasBroker,
    },
    {
      title: 'Configuration',
      content: (
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Replication Factor</label>
              <input
                type="number" min={1} max={10}
                value={config.replication_factor}
                onChange={e => setConfig({ ...config, replication_factor: Number(e.target.value) })}
                className={`w-full px-3 py-2 border rounded-lg text-sm ${rfExceedsBrokers ? 'border-red-400' : ''}`}
              />
              {rfExceedsBrokers && (
                <p className="flex items-center gap-1 text-xs text-red-600 mt-1">
                  <AlertTriangle size={12} />
                  Replication factor ({config.replication_factor}) exceeds broker count ({brokerCount}). Deployment will fail.
                </p>
              )}
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Default Partitions</label>
              <input
                type="number" min={1} max={100}
                value={config.num_partitions}
                onChange={e => setConfig({ ...config, num_partitions: Number(e.target.value) })}
                className="w-full px-3 py-2 border rounded-lg text-sm"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Broker Port</label>
              <input
                type="number"
                value={config.listener_port}
                onChange={e => setConfig({ ...config, listener_port: Number(e.target.value) })}
                className={`w-full px-3 py-2 border rounded-lg text-sm ${portError ? 'border-amber-400' : ''}`}
              />
              {portError && (
                <p className="flex items-center gap-1 text-xs text-amber-600 mt-1">
                  <AlertTriangle size={12} /> {portError}
                </p>
              )}
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                {mode === 'zookeeper' ? 'ZooKeeper Port' : 'Controller Port'}
              </label>
              <input
                type="number"
                value={mode === 'zookeeper' ? (config.controller_port === 9093 ? 2181 : config.controller_port) : config.controller_port}
                onChange={e => setConfig({ ...config, controller_port: Number(e.target.value) })}
                className="w-full px-3 py-2 border rounded-lg text-sm"
              />
              <p className="text-xs text-gray-400 mt-1">
                {mode === 'zookeeper' ? 'Default: 2181' : 'Default: 9093'}
              </p>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Log Directory</label>
              <input
                type="text"
                value={config.log_dirs}
                onChange={e => setConfig({ ...config, log_dirs: e.target.value })}
                className="w-full px-3 py-2 border rounded-lg text-sm font-mono"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Heap Size</label>
              <select
                value={config.heap_size}
                onChange={e => setConfig({ ...config, heap_size: e.target.value })}
                className="w-full px-3 py-2 border rounded-lg text-sm"
              >
                <option value="512M">512 MB</option>
                <option value="1G">1 GB</option>
                <option value="2G">2 GB</option>
                <option value="4G">4 GB</option>
                <option value="6G">6 GB</option>
              </select>
            </div>
            {assignedRoles.includes('ksqldb') && (
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">ksqlDB Port</label>
                <input
                  type="number"
                  value={config.ksqldb_port}
                  onChange={e => setConfig({ ...config, ksqldb_port: Number(e.target.value) })}
                  className="w-full px-3 py-2 border rounded-lg text-sm"
                />
              </div>
            )}
            {assignedRoles.includes('kafka_connect') && (
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Connect REST Port</label>
                <input
                  type="number"
                  value={config.connect_rest_port}
                  onChange={e => setConfig({ ...config, connect_rest_port: Number(e.target.value) })}
                  className="w-full px-3 py-2 border rounded-lg text-sm"
                />
              </div>
            )}
          </div>

          {/* v1.4.2 — Check ports on selected hosts before submit */}
          <div className="border border-gray-200 rounded-xl p-4 bg-gray-50">
            <div className="flex items-center justify-between mb-2">
              <div>
                <div className="text-sm font-medium text-gray-900">Port availability</div>
                <div className="text-xs text-gray-500 mt-0.5">
                  Run a quick SSH check against your selected hosts to make sure these ports are free.
                </div>
              </div>
              <button
                type="button"
                disabled={portCheckLoading || Object.keys(assignments).length === 0}
                onClick={async () => {
                  setPortCheckLoading(true);
                  setPortCheckResult(null);
                  try {
                    const ports = [config.listener_port, config.controller_port];
                    if (assignedRoles.includes('schema_registry')) ports.push(config.schema_registry_port || 8085);
                    if (assignedRoles.includes('ksqldb')) ports.push(config.ksqldb_port);
                    if (assignedRoles.includes('kafka_connect')) ports.push(config.connect_rest_port);
                    const hostIds = Object.keys(assignments).filter(h => assignments[h].length > 0);
                    const r = await preflightPorts(hostIds, ports);
                    setPortCheckResult(r);
                  } catch (e: unknown) {
                    const ax = e as { response?: { data?: { detail?: string } } };
                    setPortCheckResult({ ok: false, conflicts: [], ssh_failures: [{
                      host_ip: 'preflight', error: ax.response?.data?.detail || 'preflight failed',
                    }] });
                  } finally {
                    setPortCheckLoading(false);
                  }
                }}
                className="px-3 py-1.5 bg-white border border-gray-300 rounded-lg text-sm hover:bg-gray-100 disabled:opacity-50 flex items-center gap-1.5"
              >
                {portCheckLoading ? <Loader2 size={14} className="animate-spin" /> : <Server size={14} />}
                Check ports
              </button>
            </div>
            {portCheckResult && (
              portCheckResult.ok ? (
                <div className="text-sm text-green-700 flex items-center gap-2 mt-2">
                  <Check size={14} /> All ports free on selected hosts.
                </div>
              ) : (
                <div className="text-sm space-y-1 mt-2">
                  {portCheckResult.conflicts.map((c, i) => (
                    <div key={i} className="flex items-start gap-2 text-amber-800 bg-amber-50 border border-amber-200 rounded px-3 py-2">
                      <AlertTriangle size={14} className="mt-0.5 shrink-0" />
                      <div>
                        <div className="font-mono text-xs">
                          {c.host_ip}:{c.port} ({c.label}) is in use
                        </div>
                        <div className="text-xs text-amber-700 mt-0.5">
                          held by: <span className="font-mono">{c.process}</span>
                        </div>
                      </div>
                    </div>
                  ))}
                  {portCheckResult.ssh_failures.map((s, i) => (
                    <div key={`s${i}`} className="flex items-start gap-2 text-gray-700 bg-gray-100 border border-gray-200 rounded px-3 py-2">
                      <AlertTriangle size={14} className="mt-0.5 shrink-0" />
                      <div className="text-xs">
                        Couldn't SSH to {s.host_ip}: {s.error}
                      </div>
                    </div>
                  ))}
                </div>
              )
            )}
          </div>
        </div>
      ),
      valid: !rfExceedsBrokers,
    },
    {
      title: 'Review & Create',
      content: (
        <div className="space-y-6">
          {/* Warnings */}
          {(offlineHostSelected || rfExceedsBrokers) && (
            <div className="space-y-2">
              {offlineHostSelected && (
                <div className="flex items-start gap-2 p-3 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">
                  <AlertTriangle size={16} className="mt-0.5 flex-shrink-0" />
                  <span>One or more selected hosts are offline. Deployment will fail.</span>
                </div>
              )}
              {rfExceedsBrokers && (
                <div className="flex items-start gap-2 p-3 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">
                  <AlertTriangle size={16} className="mt-0.5 flex-shrink-0" />
                  <span>Replication factor ({config.replication_factor}) exceeds broker count ({brokerCount}).</span>
                </div>
              )}
            </div>
          )}
          <div className="bg-gray-50 rounded-xl p-5">
            <h3 className="font-semibold text-sm text-gray-800 mb-3">Cluster Summary</h3>
            <dl className="grid grid-cols-2 gap-2 text-sm">
              <dt className="text-gray-500">Name</dt><dd className="font-medium">{name}</dd>
              <dt className="text-gray-500">Kafka Version</dt><dd className="font-medium">{kafkaVersion}</dd>
              <dt className="text-gray-500">Mode</dt><dd className="font-medium uppercase">{mode}</dd>
              <dt className="text-gray-500">Replication Factor</dt><dd className="font-medium">{config.replication_factor}</dd>
              <dt className="text-gray-500">Partitions</dt><dd className="font-medium">{config.num_partitions}</dd>
            </dl>
          </div>
          <div>
            <h3 className="font-semibold text-sm text-gray-800 mb-3">Service Assignments</h3>
            <div className="space-y-2">
              {buildServices().map((svc, i) => {
                const host = hosts.find(h => h.id === svc.host_id);
                const roleInfo = ROLES.find(r => r.id === svc.role);
                return (
                  <div key={i} className="flex items-center gap-3 text-sm">
                    <span className={`px-2 py-0.5 rounded text-xs border ${roleInfo?.color}`}>
                      {roleInfo?.label}
                    </span>
                    <span className="text-gray-400 text-xs">ID: {svc.node_id}</span>
                    <span className="text-gray-600">{host?.hostname}</span>
                    <span className="text-gray-400">({host?.ip_address})</span>
                    {host?.status !== 'online' && (
                      <span className="text-xs text-red-500 font-medium">OFFLINE</span>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      ),
      valid: !rfExceedsBrokers,
    },
  ];

  return (
    <div>
      {/* Step indicators */}
      <div className="flex items-center gap-2 mb-8">
        {steps.map((s, i) => (
          <div key={i} className="flex items-center gap-2">
            <button
              onClick={() => i < step && setStep(i)}
              className={`flex items-center gap-2 px-3 py-1.5 rounded-full text-sm transition-colors ${
                i === step ? 'bg-blue-600 text-white' :
                i < step ? 'bg-blue-100 text-blue-700 cursor-pointer' :
                'bg-gray-100 text-gray-400'
              }`}
            >
              {i < step ? <Check size={14} /> : <span className="w-5 text-center">{i + 1}</span>}
              {s.title}
            </button>
            {i < steps.length - 1 && <ChevronRight size={16} className="text-gray-300" />}
          </div>
        ))}
      </div>

      {/* Current step content */}
      <div className="bg-white border rounded-xl p-6 mb-6">
        {steps[step].content}
      </div>

      {/* Navigation */}
      <div className="flex justify-between">
        <button
          onClick={() => setStep(s => s - 1)}
          disabled={step === 0}
          className="flex items-center gap-1 px-4 py-2 text-sm border rounded-lg hover:bg-gray-50 disabled:opacity-30"
        >
          <ChevronLeft size={16} /> Back
        </button>
        {step < steps.length - 1 ? (
          <button
            onClick={() => setStep(s => s + 1)}
            disabled={!steps[step].valid}
            className="flex items-center gap-1 px-4 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
          >
            Next <ChevronRight size={16} />
          </button>
        ) : (
          <button
            onClick={handleCreate}
            disabled={loading || rfExceedsBrokers}
            className="flex items-center gap-1 px-6 py-2 text-sm bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50"
          >
            {loading ? 'Creating...' : 'Create Cluster'}
          </button>
        )}
      </div>
    </div>
  );
}
