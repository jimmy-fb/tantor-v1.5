import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { ChevronRight, ChevronLeft, Check, Server, Loader2, AlertTriangle, FolderOpen, Info } from 'lucide-react';
import type { Host, KafkaVersionInfo, ClusterCreate, ServiceAssignment, ClusterConfig, InitialAcl } from '../../types';
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

// Fields used in the Advanced Configuration accordion that are not part of
// ClusterConfig (which is owned by the backend types). They are merged into
// the config payload at submit time so the backend receives them — but
// TypeScript won't complain about ClusterConfig not knowing about them.
interface AdvancedConfig {
  retention_hours: number;
  cpu_quota: string;
  memory_max: string;
  jvm_performance_opts: string;
  jmx_port: number | undefined;
  gc_logging_enabled: boolean;
}

const DEFAULT_ADVANCED: AdvancedConfig = {
  retention_hours: 168,
  cpu_quota: '',
  memory_max: '',
  jvm_performance_opts: '',
  jmx_port: undefined,
  gc_logging_enabled: false,
};

// Parse major version from a Kafka version string like "3.7.0"
function getMajorVersion(version: string): number {
  const parts = version.split('.');
  return parseInt(parts[0], 10) || 0;
}

// Validate a path the same way the backend does — absolute, no traversal, safe chars.
// Returns an error string or empty string if valid.
function validateDeployPath(value: string, label: string): string {
  if (!value.trim()) return ''; // empty = auto-derive, always valid
  const path = value.trim();
  if (!path.startsWith('/')) return `${label} must be an absolute path (start with /).`;
  if (path.split('/').includes('..')) return `${label} must not contain ".." path traversal.`;
  if (!/^\/[A-Za-z0-9/_\-.]{1,510}$/.test(path))
    return `${label} contains invalid characters. Use only letters, numbers, /, -, _, .`;
  return '';
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
  const [environment, setEnvironment] = useState('');

  // Step 2: Role assignment — multi-role per host
  const [assignments, setAssignments] = useState<Record<string, string[]>>({});

  // Step 3: Core configuration (fields that exist on ClusterConfig)
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
    kafka_install_dir: '',
    kafka_data_dir: '',
  });

  // Step 3: Advanced configuration (fields NOT on ClusterConfig — merged at submit)
  const [advanced, setAdvanced] = useState<AdvancedConfig>(DEFAULT_ADVANCED);

  // Validation state
  const [nameError, setNameError] = useState('');
  const [portError, setPortError] = useState('');
  const [installDirError, setInstallDirError] = useState('');
  const [dataDirError, setDataDirError] = useState('');

  // Step 4: Initial ACLs
  const [initialAcls, setInitialAcls] = useState<InitialAcl[]>([]);

  // v1.4.2 — port preflight against the selected hosts.
  const [portCheckLoading, setPortCheckLoading] = useState(false);
  const [portCheckResult, setPortCheckResult] = useState<{
    ok: boolean;
    conflicts: Array<{ host_ip: string; port: number; label: string; process: string }>;
    ssh_failures: Array<{ host_ip: string; error: string }>;
  } | null>(null);

  useEffect(() => {
    getHosts().then(setHosts);
    getClusters()
      .then(clusters => setExistingClusters(clusters.map(c => ({ name: c.name, ports: [] }))))
      .catch(() => {});
    setVersionsLoading(true);
    getKafkaVersions()
      .then(data => {
        setVersions(data);
        const available = data.filter(v => v.available);
        if (available.length > 0) {
          setKafkaVersion(v => v || available[0].version);
        } else if (data.length > 0) {
          setKafkaVersion(v => v || data[0].version);
        }
      })
      .catch(() => setVersions([]))
      .finally(() => setVersionsLoading(false));
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Auto-force KRaft mode for Kafka 4.x+
  useEffect(() => {
    if (kafkaVersion && getMajorVersion(kafkaVersion) >= 4) {
      setMode('kraft');
    }
  }, [kafkaVersion]);

  // Validate cluster name inline (#17)
  useEffect(() => {
    if (!name.trim()) { setNameError(''); return; }
    const duplicate = existingClusters.find(c => c.name.toLowerCase() === name.trim().toLowerCase());
    setNameError(duplicate ? `A cluster named "${duplicate.name}" already exists` : '');
  }, [name, existingClusters]);

  // Validate listener port inline (#24)
  useEffect(() => {
    if (config.listener_port < 1024)
      setPortError('Ports below 1024 require root access and may conflict with system services');
    else if (config.listener_port > 65535)
      setPortError('Port must be between 1024 and 65535');
    else
      setPortError('');
  }, [config.listener_port]);

  // Validate custom deploy paths (v1.4.5)
  useEffect(() => {
    setInstallDirError(validateDeployPath(config.kafka_install_dir || '', 'Install Directory'));
  }, [config.kafka_install_dir]);

  useEffect(() => {
    setDataDirError(validateDeployPath(config.kafka_data_dir || '', 'Data Directory'));
  }, [config.kafka_data_dir]);

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
    let ancillaryNodeId = 201; // ksqldb, kafka_connect, zookeeper
    const services: ServiceAssignment[] = [];
    for (const [hostId, roles] of Object.entries(assignments)) {
      for (const role of roles) {
        let nodeId: number;
        if (role === 'controller') nodeId = controllerNodeId++;
        else if (role === 'broker_controller' || role === 'broker') nodeId = brokerNodeId++;
        else nodeId = ancillaryNodeId++;
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
        config: {
          ...config,
          // Merge advanced fields into the payload — the backend accepts them
          // even though they're not part of the strict ClusterConfig TS type.
          ...(advanced as unknown as Partial<ClusterConfig>),
          // Normalise empty strings to undefined so the Pydantic validator
          // doesn't see an empty string as an invalid path.
          kafka_install_dir: config.kafka_install_dir?.trim() || undefined,
          kafka_data_dir: config.kafka_data_dir?.trim() || undefined,
          // Strip optional advanced fields that are empty / default
          ...(advanced.cpu_quota.trim() ? {} : { cpu_quota: undefined }),
          ...(advanced.memory_max.trim() ? {} : { memory_max: undefined }),
          ...(advanced.jvm_performance_opts.trim() ? {} : { jvm_performance_opts: undefined }),
          ...(advanced.jmx_port ? {} : { jmx_port: undefined }),
        },
        environment: environment.trim().toLowerCase(),
        initial_acls: initialAcls.filter(
          a => a.principal.trim() && a.resource_name.trim() && a.operations.length > 0
        ),
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

  const brokerCount = assignedRoles.filter(r => r === 'broker' || r === 'broker_controller').length;
  const rfExceedsBrokers = config.replication_factor > brokerCount && brokerCount > 0;

  const offlineHostSelected = Object.keys(assignments).some(hostId => {
    const host = hosts.find(h => h.id === hostId);
    return host && host.status !== 'online';
  });

  const ipLiteralPattern = /^(\d{1,3}\.){3}\d{1,3}$/;
  const fqdnRiskyHosts = Object.keys(assignments).map(hostId => {
    const host = hosts.find(h => h.id === hostId);
    if (!host) return null;
    if (ipLiteralPattern.test(host.ip_address)) return null;
    if (host.ip_address.includes('.')) return null;
    return host.hostname;
  }).filter(Boolean) as string[];

  const availableRoles = ROLES.filter(r => {
    if (mode === 'kraft') return r.id !== 'zookeeper';
    return r.id !== 'controller' && r.id !== 'broker_controller';
  });

  const pathsValid = !installDirError && !dataDirError;
  const step3Valid = !rfExceedsBrokers && pathsValid;
  const hasCustomPaths = !!(config.kafka_install_dir?.trim() || config.kafka_data_dir?.trim());

  const steps = [
    // ── Step 1: Cluster Basics ──────────────────────────────────────────
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
                {selectedVersion?.features && (
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
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Environment <span className="text-gray-400 text-xs">(optional)</span>
            </label>
            <div className="flex gap-2 flex-wrap">
              {['dev', 'qa', 'staging', 'prod', 'fdr'].map((e) => (
                <button
                  key={e}
                  type="button"
                  onClick={() => setEnvironment(e === environment ? '' : e)}
                  className={`px-3 py-1.5 text-sm rounded border ${
                    environment === e
                      ? 'bg-blue-50 border-blue-400 text-blue-700'
                      : 'bg-white border-gray-300 text-gray-600 hover:bg-gray-50'
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
                  isKafka4Plus
                    ? 'opacity-50 cursor-not-allowed border-gray-200 bg-gray-50'
                    : mode === 'zookeeper'
                    ? 'border-blue-500 bg-blue-50'
                    : 'border-gray-200 hover:border-gray-300'
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

    // ── Step 2: Assign Roles ───────────────────────────────────────────
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
                        host.status === 'online' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-600'
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

    // ── Step 3: Configuration ──────────────────────────────────────────
    {
      title: 'Configuration',
      content: (
        <div className="space-y-6">
          {/* ── Core settings ── */}
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
              <p className="text-xs text-gray-400 mt-1">{mode === 'zookeeper' ? 'Default: 2181' : 'Default: 9093'}</p>
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

          {/* ── Deploy Paths (v1.4.5) ─────────────────────────────────── */}
          <div className="border border-gray-200 rounded-xl overflow-hidden">
            <div className="flex items-center gap-2 px-4 py-3 bg-gray-50 border-b border-gray-200">
              <FolderOpen size={15} className="text-gray-500" />
              <span className="text-sm font-medium text-gray-800">Deploy Paths</span>
              <span className="ml-1 text-xs text-gray-400">(optional — leave blank for auto)</span>
            </div>

            <div className="p-4 space-y-4">
              <div className="flex items-start gap-2 text-xs text-blue-700 bg-blue-50 border border-blue-100 rounded-lg px-3 py-2.5">
                <Info size={13} className="mt-0.5 flex-shrink-0" />
                <span>
                  By default Tantor derives unique paths from the cluster ID, e.g.{' '}
                  <code className="font-mono bg-blue-100 px-1 rounded">/opt/kafka-prod-a1b2c3d4</code>, so
                  multiple clusters on the same host never collide. Override only when your infrastructure
                  policy requires specific mount points (e.g. a dedicated NVMe at{' '}
                  <code className="font-mono bg-blue-100 px-1 rounded">/data/kafka</code>).
                  Must be an absolute path.
                </span>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Log Directory</label>
                <input
                  type="text"
                  value={config.log_dirs}
                  onChange={e => setConfig({ ...config, log_dirs: e.target.value })}
                  className="w-full px-3 py-2 border rounded-lg text-sm font-mono"
                />
                <p className="text-xs text-gray-400 mt-1">
                  Kafka <code>log.dirs</code> — where message log segments are stored. Synced automatically to Data Directory when using auto-derive.
                </p>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Install Directory</label>
                  <input
                    type="text"
                    value={config.kafka_install_dir || ''}
                    onChange={e => setConfig({ ...config, kafka_install_dir: e.target.value })}
                    placeholder="/opt/kafka-{name}-{id}"
                    className={`w-full px-3 py-2 border rounded-lg text-sm font-mono ${
                      installDirError ? 'border-red-400 bg-red-50' : ''
                    }`}
                  />
                  {installDirError ? (
                    <p className="flex items-center gap-1 text-xs text-red-600 mt-1">
                      <AlertTriangle size={11} /> {installDirError}
                    </p>
                  ) : (
                    <p className="text-xs text-gray-400 mt-1">Where Kafka binaries are extracted on broker hosts.</p>
                  )}
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Data Directory</label>
                  <input
                    type="text"
                    value={config.kafka_data_dir || ''}
                    onChange={e => setConfig({ ...config, kafka_data_dir: e.target.value })}
                    placeholder="/var/lib/kafka-{name}-{id}/data"
                    className={`w-full px-3 py-2 border rounded-lg text-sm font-mono ${
                      dataDirError ? 'border-red-400 bg-red-50' : ''
                    }`}
                  />
                  {dataDirError ? (
                    <p className="flex items-center gap-1 text-xs text-red-600 mt-1">
                      <AlertTriangle size={11} /> {dataDirError}
                    </p>
                  ) : (
                    <p className="text-xs text-gray-400 mt-1">Where Kafka log segments and KRaft metadata are persisted.</p>
                  )}
                </div>
              </div>

              {hasCustomPaths && config.log_dirs === '/var/lib/kafka/data' && (
                <div className="flex items-start gap-2 text-xs text-amber-700 bg-amber-50 border border-amber-200 rounded-lg px-3 py-2.5">
                  <AlertTriangle size={13} className="mt-0.5 flex-shrink-0" />
                  <span>
                    You've set a custom Data Directory but <strong>Log Directory</strong> is still the
                    default <code className="font-mono">/var/lib/kafka/data</code>. Tantor syncs these
                    automatically when using auto-derive, but with custom paths you should update Log
                    Directory to match — otherwise brokers will write segments to the old path.
                  </span>
                </div>
              )}
            </div>
          </div>

          {/* ── Advanced Configuration Accordion ── */}
          <details className="border border-gray-200 rounded-xl overflow-hidden bg-white [&_summary::-webkit-details-marker]:hidden group">
            <summary className="flex items-center justify-between p-4 cursor-pointer hover:bg-gray-50 focus:outline-none">
              <div>
                <div className="text-sm font-medium text-gray-900">Advanced Configuration</div>
                <div className="text-xs text-gray-500 mt-0.5">Tune resources, retention, and heap sizing.</div>
              </div>
              <svg className="w-5 h-5 text-gray-400 group-open:rotate-180 transition-transform" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
              </svg>
            </summary>
            <div className="p-4 border-t border-gray-100 bg-gray-50">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
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
                  <p className="text-xs text-gray-500 mt-1">Global JVM heap limit for all Kafka components.</p>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Retention (Hours)</label>
                  <input
                    type="number"
                    value={advanced.retention_hours}
                    onChange={e => setAdvanced({ ...advanced, retention_hours: Number(e.target.value) })}
                    className="w-full px-3 py-2 border rounded-lg text-sm"
                  />
                  <p className="text-xs text-gray-500 mt-1">log.retention.hours (Default: 168 / 7 days)</p>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">CPU Quota (Systemd)</label>
                  <input
                    type="text"
                    value={advanced.cpu_quota}
                    placeholder="e.g. 200%"
                    onChange={e => setAdvanced({ ...advanced, cpu_quota: e.target.value })}
                    className="w-full px-3 py-2 border rounded-lg text-sm"
                  />
                  <p className="text-xs text-gray-500 mt-1">CPUQuota limit (e.g. 200% = 2 cores). Leave blank for unlimited.</p>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Max Memory (Systemd)</label>
                  <input
                    type="text"
                    value={advanced.memory_max}
                    placeholder="e.g. 16G"
                    onChange={e => setAdvanced({ ...advanced, memory_max: e.target.value })}
                    className="w-full px-3 py-2 border rounded-lg text-sm"
                  />
                  <p className="text-xs text-gray-500 mt-1">MemoryMax cgroup limit. Leave blank for unlimited.</p>
                </div>
              </div>

              <div className="mt-6 border-t border-gray-200 pt-4">
                <div className="text-sm font-medium text-gray-900 mb-4">JVM Troubleshooting & Tuning</div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="md:col-span-2">
                    <label className="block text-sm font-medium text-gray-700 mb-1">Custom JVM Performance Options</label>
                    <input
                      type="text"
                      value={advanced.jvm_performance_opts}
                      placeholder="e.g. -javaagent:/opt/datadog/agent.jar -XX:+HeapDumpOnOutOfMemoryError"
                      onChange={e => setAdvanced({ ...advanced, jvm_performance_opts: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg text-sm"
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      Extra flags to pass to the JVM. Do NOT pass -Xmx or -Xms here; use the Heap Size dropdown above.
                    </p>
                    {advanced.jvm_performance_opts.includes('-Xmx') && (
                      <p className="text-xs text-red-600 mt-1">
                        Warning: Passing -Xmx here conflicts with the Heap Size setting and may cause startup failures.
                      </p>
                    )}
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">JMX Port</label>
                    <input
                      type="number"
                      value={advanced.jmx_port ?? ''}
                      placeholder="e.g. 9999"
                      onChange={e => setAdvanced({ ...advanced, jmx_port: e.target.value ? Number(e.target.value) : undefined })}
                      className="w-full px-3 py-2 border rounded-lg text-sm"
                    />
                    <p className="text-xs text-gray-500 mt-1">Port for remote JMX profiling. Leave blank to disable.</p>
                  </div>

                  <div className="flex flex-col justify-center">
                    <label className="flex items-center space-x-2 mt-4 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={advanced.gc_logging_enabled}
                        onChange={e => setAdvanced({ ...advanced, gc_logging_enabled: e.target.checked })}
                        className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                      />
                      <span className="text-sm font-medium text-gray-700">Enable GC Logging</span>
                    </label>
                    <p className="text-xs text-gray-500 mt-1 ml-6">
                      Writes detailed garbage collection logs to /var/log/kafka/gc-*.log
                    </p>
                  </div>
                </div>
              </div>
            </div>
          </details>

          {/* ── Port preflight (v1.4.2) ── */}
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
                    if (assignedRoles.includes('ksqldb')) ports.push(config.ksqldb_port);
                    if (assignedRoles.includes('kafka_connect')) ports.push(config.connect_rest_port);
                    const hostIds = Object.keys(assignments).filter(h => assignments[h].length > 0);
                    const r = await preflightPorts(hostIds, ports);
                    setPortCheckResult(r);
                  } catch (e: unknown) {
                    const ax = e as { response?: { data?: { detail?: string } } };
                    setPortCheckResult({
                      ok: false,
                      conflicts: [],
                      ssh_failures: [{ host_ip: 'preflight', error: ax.response?.data?.detail ?? 'preflight failed' }],
                    });
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
                        <div className="font-mono text-xs">{c.host_ip}:{c.port} ({c.label}) is in use</div>
                        <div className="text-xs text-amber-700 mt-0.5">held by: <span className="font-mono">{c.process}</span></div>
                      </div>
                    </div>
                  ))}
                  {portCheckResult.ssh_failures.map((s, i) => (
                    <div key={`s${i}`} className="flex items-start gap-2 text-gray-700 bg-gray-100 border border-gray-200 rounded px-3 py-2">
                      <AlertTriangle size={14} className="mt-0.5 shrink-0" />
                      <div className="text-xs">Couldn't SSH to {s.host_ip}: {s.error}</div>
                    </div>
                  ))}
                </div>
              )
            )}
          </div>
        </div>
      ),
      valid: step3Valid,
    },

    // ── Step 4: ACLs (optional) ──────────────────────────────────────────
    {
      title: 'ACLs',
      content: (() => {
        const ACL_OPERATIONS = ['Read', 'Write', 'Create', 'Delete', 'Alter', 'Describe', 'All'];
        const ACL_RESOURCE_TYPES = ['topic', 'group', 'cluster', 'transactional-id'];

        const addAcl = () => setInitialAcls(prev => [...prev, {
          principal: '',
          resource_type: 'topic',
          resource_name: '',
          pattern_type: 'literal',
          operations: ['Read'],
          permission_type: 'Allow',
          host: '*',
        }]);

        const removeAcl = (i: number) =>
          setInitialAcls(prev => prev.filter((_, idx) => idx !== i));

        const updateAcl = (i: number, patch: Partial<InitialAcl>) =>
          setInitialAcls(prev => prev.map((a, idx) => idx === i ? { ...a, ...patch } : a));

        const toggleOp = (i: number, op: string) => {
          const current = initialAcls[i].operations;
          const next = current.includes(op)
            ? current.filter(o => o !== op)
            : [...current, op];
          updateAcl(i, { operations: next });
        };

        return (
          <div className="space-y-4">
            <div className="flex items-start gap-2 text-xs text-blue-700 bg-blue-50 border border-blue-100 rounded-lg px-3 py-2.5">
              <Info size={13} className="mt-0.5 flex-shrink-0" />
              <span>
                ACLs defined here are applied immediately after the broker comes up —
                no need to visit the Security tab after deploy. Leave blank to skip.
                The broker must have authentication enabled (<code>SASL</code>) for ACLs to take effect.
              </span>
            </div>

            {initialAcls.length === 0 && (
              <div className="text-center py-10 text-gray-400 text-sm border border-dashed rounded-xl">
                No ACLs defined. Click "Add ACL Rule" to pre-seed access controls.
              </div>
            )}

            {initialAcls.map((acl, i) => (
              <div key={i} className="border rounded-xl p-4 space-y-3 bg-white">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-semibold text-gray-600 uppercase tracking-wide">ACL Rule {i + 1}</span>
                  <button
                    type="button"
                    onClick={() => removeAcl(i)}
                    className="text-xs text-red-500 hover:text-red-700 px-2 py-0.5 rounded border border-red-200 hover:border-red-400"
                  >
                    Remove
                  </button>
                </div>

                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="block text-xs font-medium text-gray-600 mb-1">Principal</label>
                    <input
                      type="text"
                      value={acl.principal}
                      onChange={e => updateAcl(i, { principal: e.target.value })}
                      placeholder="User:myapp or myapp"
                      className="w-full px-2.5 py-1.5 border rounded-lg text-sm font-mono"
                    />
                    <p className="text-xs text-gray-400 mt-0.5">
                      Bare name auto-prefixed with <code>User:</code>
                    </p>
                  </div>

                  <div>
                    <label className="block text-xs font-medium text-gray-600 mb-1">Permission</label>
                    <div className="flex gap-2">
                      {['Allow', 'Deny'].map(pt => (
                        <button
                          key={pt}
                          type="button"
                          onClick={() => updateAcl(i, { permission_type: pt })}
                          className={`flex-1 py-1.5 text-xs rounded-lg border font-medium ${
                            acl.permission_type === pt
                              ? pt === 'Allow'
                                ? 'bg-green-50 border-green-400 text-green-700'
                                : 'bg-red-50 border-red-400 text-red-700'
                              : 'border-gray-200 text-gray-500 hover:border-gray-400'
                          }`}
                        >
                          {pt}
                        </button>
                      ))}
                    </div>
                  </div>

                  <div>
                    <label className="block text-xs font-medium text-gray-600 mb-1">Resource Type</label>
                    <select
                      value={acl.resource_type}
                      onChange={e => updateAcl(i, { resource_type: e.target.value })}
                      className="w-full px-2.5 py-1.5 border rounded-lg text-sm"
                    >
                      {ACL_RESOURCE_TYPES.map(rt => (
                        <option key={rt} value={rt}>{rt}</option>
                      ))}
                    </select>
                  </div>

                  <div>
                    <label className="block text-xs font-medium text-gray-600 mb-1">Resource Name</label>
                    <input
                      type="text"
                      value={acl.resource_name}
                      onChange={e => updateAcl(i, { resource_name: e.target.value })}
                      placeholder={acl.resource_type === 'cluster' ? 'kafka-cluster' : '* or specific name'}
                      className="w-full px-2.5 py-1.5 border rounded-lg text-sm font-mono"
                    />
                  </div>

                  <div>
                    <label className="block text-xs font-medium text-gray-600 mb-1">Pattern</label>
                    <div className="flex gap-2">
                      {['literal', 'prefixed'].map(pt => (
                        <button
                          key={pt}
                          type="button"
                          onClick={() => updateAcl(i, { pattern_type: pt })}
                          className={`flex-1 py-1.5 text-xs rounded-lg border ${
                            acl.pattern_type === pt
                              ? 'bg-blue-50 border-blue-400 text-blue-700'
                              : 'border-gray-200 text-gray-500 hover:border-gray-400'
                          }`}
                        >
                          {pt}
                        </button>
                      ))}
                    </div>
                  </div>

                  <div>
                    <label className="block text-xs font-medium text-gray-600 mb-1">Host Filter</label>
                    <input
                      type="text"
                      value={acl.host}
                      onChange={e => updateAcl(i, { host: e.target.value })}
                      placeholder="* (any host)"
                      className="w-full px-2.5 py-1.5 border rounded-lg text-sm font-mono"
                    />
                  </div>
                </div>

                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1.5">Operations</label>
                  <div className="flex flex-wrap gap-1.5">
                    {ACL_OPERATIONS.map(op => (
                      <button
                        key={op}
                        type="button"
                        onClick={() => toggleOp(i, op)}
                        className={`px-2.5 py-1 text-xs rounded-lg border transition-all ${
                          acl.operations.includes(op)
                            ? 'bg-blue-600 text-white border-blue-600'
                            : 'border-gray-200 text-gray-600 hover:border-gray-400'
                        }`}
                      >
                        {op}
                      </button>
                    ))}
                  </div>
                  {acl.operations.length === 0 && (
                    <p className="text-xs text-red-500 mt-1">Select at least one operation</p>
                  )}
                </div>
              </div>
            ))}

            <button
              type="button"
              onClick={addAcl}
              className="w-full py-2.5 border-2 border-dashed border-gray-300 rounded-xl text-sm text-gray-500 hover:border-blue-400 hover:text-blue-600 transition-colors"
            >
              + Add ACL Rule
            </button>
          </div>
        );
      })(),
      valid: initialAcls.every(a => a.operations.length > 0),
    },

    // ── Step 5: Review & Create ────────────────────────────────────────
    {
      title: 'Review & Create',
      content: (
        <div className="space-y-6">
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
            <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
              <dt className="text-gray-500">Name</dt>
              <dd className="font-medium">{name}</dd>

              <dt className="text-gray-500">Kafka Version</dt>
              <dd className="font-medium">{kafkaVersion}</dd>

              <dt className="text-gray-500">Mode</dt>
              <dd className="font-medium uppercase">{mode}</dd>

              {environment && (
                <>
                  <dt className="text-gray-500">Environment</dt>
                  <dd className="font-medium">{environment}</dd>
                </>
              )}

              <dt className="text-gray-500">Replication Factor</dt>
              <dd className="font-medium">{config.replication_factor}</dd>

              <dt className="text-gray-500">Default Partitions</dt>
              <dd className="font-medium">{config.num_partitions}</dd>

              <dt className="text-gray-500">Broker Port</dt>
              <dd className="font-medium font-mono">{config.listener_port}</dd>

              <dt className="text-gray-500">Controller Port</dt>
              <dd className="font-medium font-mono">{config.controller_port}</dd>

              <dt className="text-gray-500">Heap Size</dt>
              <dd className="font-medium">{config.heap_size}</dd>

              {advanced.retention_hours !== 168 && (
                <>
                  <dt className="text-gray-500">Retention</dt>
                  <dd className="font-medium">{advanced.retention_hours}h</dd>
                </>
              )}
              {advanced.cpu_quota && (
                <>
                  <dt className="text-gray-500">CPU Quota</dt>
                  <dd className="font-medium font-mono">{advanced.cpu_quota}</dd>
                </>
              )}
              {advanced.memory_max && (
                <>
                  <dt className="text-gray-500">Max Memory</dt>
                  <dd className="font-medium font-mono">{advanced.memory_max}</dd>
                </>
              )}
              {advanced.jmx_port && (
                <>
                  <dt className="text-gray-500">JMX Port</dt>
                  <dd className="font-medium font-mono">{advanced.jmx_port}</dd>
                </>
              )}
              {advanced.gc_logging_enabled && (
                <>
                  <dt className="text-gray-500">GC Logging</dt>
                  <dd className="font-medium text-green-700">Enabled</dd>
                </>
              )}
            </dl>
          </div>

          <div className="bg-gray-50 rounded-xl p-5">
            <div className="flex items-center gap-2 mb-3">
              <FolderOpen size={14} className="text-gray-500" />
              <h3 className="font-semibold text-sm text-gray-800">Deploy Paths</h3>
            </div>
            <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-sm">
              <dt className="text-gray-500 whitespace-nowrap">Install Directory</dt>
              <dd className="font-mono text-xs">
                {config.kafka_install_dir?.trim()
                  ? <span className="text-gray-900">{config.kafka_install_dir.trim()}</span>
                  : <span className="text-gray-400 font-sans text-xs italic">auto — /opt/kafka-{name || 'cluster'}-{'<id>'}</span>
                }
              </dd>

              <dt className="text-gray-500 whitespace-nowrap">Data Directory</dt>
              <dd className="font-mono text-xs">
                {config.kafka_data_dir?.trim()
                  ? <span className="text-gray-900">{config.kafka_data_dir.trim()}</span>
                  : <span className="text-gray-400 font-sans text-xs italic">auto — /var/lib/kafka-{name || 'cluster'}-{'<id>'}/data</span>
                }
              </dd>

              <dt className="text-gray-500 whitespace-nowrap">Log Directory</dt>
              <dd className="font-mono text-xs text-gray-900">{config.log_dirs}</dd>
            </dl>
          </div>

          {initialAcls.length > 0 && (
            <div className="bg-gray-50 rounded-xl p-5">
              <h3 className="font-semibold text-sm text-gray-800 mb-3">
                Initial ACLs
                <span className="ml-2 text-xs font-normal text-gray-400">applied after broker starts</span>
              </h3>
              <div className="space-y-1.5">
                {initialAcls
                  .filter(a => a.principal.trim() && a.resource_name.trim() && a.operations.length > 0)
                  .map((acl, i) => (
                    <div key={i} className="flex items-center gap-2 text-xs font-mono bg-white border rounded-lg px-3 py-2">
                      <span className={`px-1.5 py-0.5 rounded text-xs font-sans font-medium ${
                        acl.permission_type === 'Allow' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
                      }`}>
                        {acl.permission_type}
                      </span>
                      <span className="text-blue-700">{acl.principal || '(no principal)'}</span>
                      <span className="text-gray-400">→</span>
                      <span className="text-gray-700">{acl.resource_type}:{acl.resource_name}</span>
                      <span className="text-gray-400 font-sans">[{acl.operations.join(', ')}]</span>
                    </div>
                  ))}
                {initialAcls.filter(a => !a.principal.trim() || !a.resource_name.trim() || a.operations.length === 0).length > 0 && (
                  <p className="text-xs text-amber-600 mt-1">
                    ⚠ Incomplete ACL rows (missing principal, resource, or operations) will be skipped.
                  </p>
                )}
              </div>
            </div>
          )}

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
                i === step
                  ? 'bg-blue-600 text-white'
                  : i < step
                  ? 'bg-blue-100 text-blue-700 cursor-pointer'
                  : 'bg-gray-100 text-gray-400'
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
            {loading ? <><Loader2 size={14} className="animate-spin" /> Creating...</> : 'Create Cluster'}
          </button>
        )}
      </div>
    </div>
  );
}
