import { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import {
  Play, Square, RefreshCw, Rocket, Loader2,
  List, Users, Send, Plug, Monitor, ScrollText, Download, Shield,
  ShieldCheck, PlusCircle, Trash2, AlertTriangle, CheckCircle, XCircle, Database,
  Settings, RotateCw, ArrowUpCircle, Shuffle, TrendingUp,
} from 'lucide-react';
import type { ClusterDetail as ClusterDetailType, ServiceStatus, ValidationStep, Host } from '../types';
import {
  getCluster, deployCluster, startCluster, stopCluster, getClusterStatus,
  validateCluster, getHosts, addServices, removeService, listDeploymentTasks,
} from '../lib/api';
import TopicManager from '../components/clusters/TopicManager';
import ConsumerGroups from '../components/clusters/ConsumerGroups';
import ProduceMessage from '../components/clusters/ProduceMessage';
import ConsumeMessages from '../components/clusters/ConsumeMessages';
import ConnectManager from '../components/clusters/ConnectManager';
import SecurityManager from '../components/clusters/SecurityManager';
import TLSPanel from '../components/clusters/TLSPanel';
import KsqlManager from '../components/clusters/KsqlManager';
import ClusterSchemaRegistry from '../components/clusters/ClusterSchemaRegistry';
import ServiceLogs from '../components/clusters/ServiceLogs';
import BrokerConfigManager from '../components/clusters/BrokerConfigManager';
import RollingRestart from '../components/clusters/RollingRestart';
import UpgradeManager from '../components/clusters/UpgradeManager';
import PartitionRebalance from '../components/clusters/PartitionRebalance';
import { DeployProgress } from '../components/clusters/DeployProgress';
import CapacityForecast from '../components/clusters/CapacityForecast';
import ClusterMonitoring from '../components/clusters/ClusterMonitoring';
import ExternalLifecycle from '../components/clusters/ExternalLifecycle';

const ROLE_LABELS: Record<string, string> = {
  broker: 'Broker',
  controller: 'Controller',
  broker_controller: 'Broker + Controller',
  zookeeper: 'ZooKeeper',
  ksqldb: 'ksqlDB',
  kafka_connect: 'Kafka Connect',
};

const ROLE_COLORS: Record<string, string> = {
  broker: 'bg-green-100 text-green-800',
  controller: 'bg-purple-100 text-purple-800',
  broker_controller: 'bg-blue-100 text-blue-800',
  zookeeper: 'bg-gray-100 text-gray-800',
  ksqldb: 'bg-orange-100 text-orange-800',
  kafka_connect: 'bg-teal-100 text-teal-800',
};

const ROLES = [
  { id: 'broker_controller', label: 'Broker + Controller' },
  { id: 'broker', label: 'Broker' },
  { id: 'controller', label: 'Controller' },
  { id: 'ksqldb', label: 'ksqlDB' },
  { id: 'kafka_connect', label: 'Kafka Connect' },
];

type Tab = 'overview' | 'topics' | 'consumers' | 'produce' | 'consume' | 'connect' | 'security' | 'ksqldb' | 'validate' | 'service-logs' | 'config' | 'restart' | 'upgrade' | 'rebalance' | 'capacity' | 'monitoring' | 'lifecycle' | 'schema-registry';

export default function ClusterDetail() {
  const { id } = useParams<{ id: string }>();
  const [detail, setDetail] = useState<ClusterDetailType | null>(null);
  const [liveStatus, setLiveStatus] = useState<ServiceStatus[]>([]);
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<Tab>('overview');

  // Validation state
  const [validating, setValidating] = useState(false);
  const [validationSteps, setValidationSteps] = useState<ValidationStep[]>([]);
  const [validationSuccess, setValidationSuccess] = useState<boolean | null>(null);

  // Add node state
  const [showAddNode, setShowAddNode] = useState(false);
  const [allHosts, setAllHosts] = useState<Host[]>([]);
  const [addHostId, setAddHostId] = useState('');
  const [addRole, setAddRole] = useState('broker');
  const [addNodeId, setAddNodeId] = useState(100);
  const [addingNode, setAddingNode] = useState(false);

  // Remove node state
  const [removingService, setRemovingService] = useState<string | null>(null);
  const [removeError, setRemoveError] = useState<string | null>(null);

  const fetchDetail = () => {
    if (id) getCluster(id).then(setDetail);
  };

  useEffect(() => {
    fetchDetail();
    // customer issue #5: cluster detail header (state badge, version, services)
    // was stale until manual reload. Poll every 15s while the page is
    // visible so the operator never sees an out-of-date status.
    const interval = setInterval(() => {
      if (!document.hidden) fetchDetail();
    }, 15000);
    return () => clearInterval(interval);
  }, [id]);

  const [activeDeployTaskId, setActiveDeployTaskId] = useState<string | null>(null);

  // On page load, if the cluster is mid-deploy or in an error state, surface
  // the most recent deploy task so the user can read the log without having
  // to know the task_id.
  useEffect(() => {
    if (!id || !detail) return;
    const s = (detail.cluster.state || '').toLowerCase();
    if (s !== 'deploying' && s !== 'error') return;
    if (activeDeployTaskId) return;
    listDeploymentTasks(id).then(tasks => {
      if (tasks && tasks.length > 0) setActiveDeployTaskId(tasks[0].task_id);
    }).catch(() => { /* best-effort */ });
  }, [id, detail, activeDeployTaskId]);

  const handleDeploy = async () => {
    if (!id) return;
    setActionLoading('deploy');
    try {
      const task = await deployCluster(id);
      setActiveDeployTaskId(task.task_id);
      fetchDetail();
    } finally {
      setActionLoading(null);
    }
  };

  const handleStart = async () => {
    if (!id) return;
    setActionLoading('start');
    try {
      await startCluster(id);
      fetchDetail();
    } finally {
      setActionLoading(null);
    }
  };

  const handleStop = async () => {
    if (!id) return;
    setActionLoading('stop');
    try {
      await stopCluster(id);
      fetchDetail();
    } finally {
      setActionLoading(null);
    }
  };

  const handleRefreshStatus = async () => {
    if (!id) return;
    setActionLoading('status');
    try {
      const statuses = await getClusterStatus(id);
      setLiveStatus(statuses);
    } finally {
      setActionLoading(null);
    }
  };

  // Validation
  const handleValidate = async () => {
    if (!id) return;
    setValidating(true);
    setValidationSteps([]);
    setValidationSuccess(null);
    try {
      const result = await validateCluster(id);
      setValidationSteps(result.steps);
      setValidationSuccess(result.success);
    } catch {
      setValidationSuccess(false);
      setValidationSteps([{ step: 'error', success: false, message: 'Validation failed — is the cluster running?' }]);
    } finally {
      setValidating(false);
    }
  };

  // Add node
  const handleShowAddNode = () => {
    setShowAddNode(true);
    getHosts().then(setAllHosts);
  };

  const handleAddNode = async () => {
    if (!id || !addHostId) return;
    setAddingNode(true);
    try {
      await addServices(id, [{ host_id: addHostId, role: addRole, node_id: addNodeId }]);
      setShowAddNode(false);
      setAddHostId('');
      fetchDetail();
    } finally {
      setAddingNode(false);
    }
  };

  // Remove node
  const handleRemoveService = async (serviceId: string, force: boolean = false) => {
    if (!id) return;
    setRemovingService(serviceId);
    setRemoveError(null);
    try {
      await removeService(id, serviceId, force);
      fetchDetail();
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      setRemoveError(msg || 'Failed to remove service');
    } finally {
      setRemovingService(null);
    }
  };

  if (!detail) {
    return <div className="flex items-center justify-center gap-2 py-12 text-gray-500"><Loader2 size={20} className="animate-spin" /> Loading cluster...</div>;
  }

  const { cluster, services } = detail;
  const isRunning = cluster.state === 'running';
  const hasConnect = services.some(s => s.role === 'kafka_connect');
  const hasKsqldb = services.some(s => s.role === 'ksqldb');
  const isExternal = cluster.kind === 'external';

  const tabs: Array<{ id: Tab; label: string; icon: React.ReactNode; requiresRunning?: boolean; requiresConnect?: boolean; requiresKsqldb?: boolean; managedOnly?: boolean; externalOnly?: boolean }> = [
    { id: 'overview', label: 'Overview', icon: <Monitor size={14} /> },
    { id: 'topics', label: 'Topics', icon: <List size={14} />, requiresRunning: true },
    { id: 'consume', label: 'Consume', icon: <Download size={14} />, requiresRunning: true },
    { id: 'produce', label: 'Produce', icon: <Send size={14} />, requiresRunning: true },
    { id: 'consumers', label: 'Groups', icon: <Users size={14} />, requiresRunning: true },
    { id: 'connect', label: 'Connect', icon: <Plug size={14} />, requiresRunning: true, requiresConnect: true, managedOnly: true },
    { id: 'ksqldb', label: 'ksqlDB', icon: <Database size={14} />, requiresRunning: true, requiresKsqldb: true, managedOnly: true },
    // v1.4.0 #2 — Schema Registry per-cluster: deploy / browse subjects / register schemas.
    { id: 'schema-registry', label: 'Schema Registry', icon: <Database size={14} />, requiresRunning: true, managedOnly: true },
    // SCRAM users + ACLs work via kafka-python on external clusters too
    // (no SSH required); the TLS/mTLS sub-panel is hidden inside the
    // SecurityManager component when cluster.kind=external.
    { id: 'security', label: 'Security', icon: <Shield size={14} />, requiresRunning: true },
    { id: 'validate', label: 'Validate', icon: <ShieldCheck size={14} />, requiresRunning: true },
    // Broker config dispatches through kafka-python's describe/alter_configs
    // for external clusters; the audit-log + rollback rows persist locally.
    { id: 'config', label: 'Config', icon: <Settings size={14} />, requiresRunning: true },
    { id: 'rebalance', label: 'Rebalance', icon: <Shuffle size={14} />, requiresRunning: true, managedOnly: true },
    { id: 'restart', label: 'Restart', icon: <RotateCw size={14} />, requiresRunning: true, managedOnly: true },
    { id: 'upgrade', label: 'Upgrade', icon: <ArrowUpCircle size={14} />, managedOnly: true },
    { id: 'monitoring', label: 'Monitoring', icon: <Monitor size={14} /> },
    { id: 'lifecycle', label: 'Lifecycle', icon: <Play size={14} />, externalOnly: true },
    { id: 'capacity', label: 'Capacity', icon: <TrendingUp size={14} />, requiresRunning: true },
    { id: 'service-logs', label: 'Service Logs', icon: <ScrollText size={14} />, requiresRunning: true, managedOnly: true },
  ];

  // External clusters: treat 'connected' state as 'running' for tab visibility,
  // and hide tabs that require SSH access to broker hosts (managedOnly).
  const externalIsLive = isExternal && (cluster.state === 'connected' || cluster.state === 'running');
  const visibleTabs = tabs.filter(t => {
    if (t.managedOnly && isExternal) return false;
    if (t.externalOnly && !isExternal) return false;
    if (t.requiresRunning && !isRunning && !externalIsLive) return false;
    if (t.requiresConnect && !hasConnect) return false;
    if (t.requiresKsqldb && !hasKsqldb) return false;
    return true;
  });

  // Hosts already in cluster
  const clusterHostIds = new Set(services.map(s => s.host_id));

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            {cluster.name}
            {isExternal && (
              <span className="px-2 py-0.5 rounded text-xs font-medium bg-purple-50 text-purple-700 border border-purple-200">
                external
              </span>
            )}
          </h1>
          <p className="text-sm text-gray-500 mt-1">
            {isExternal
              ? <>Imported cluster — Kafka {cluster.kafka_version === 'external' ? 'unknown' : cluster.kafka_version} · via bootstrap servers</>
              : <>Kafka {cluster.kafka_version} / {cluster.mode.toUpperCase()}</>
            }
            <span className={`ml-3 px-2.5 py-0.5 rounded-full text-xs font-medium ${
              cluster.state === 'running' || cluster.state === 'connected' ? 'bg-green-100 text-green-700' :
              cluster.state === 'stopped' ? 'bg-gray-100 text-gray-600' :
              cluster.state === 'deploying' ? 'bg-blue-100 text-blue-700' :
              cluster.state === 'error' ? 'bg-red-100 text-red-700' :
              'bg-yellow-100 text-yellow-700'
            }`}>
              {cluster.state}
            </span>
          </p>
        </div>
        <div className="flex items-center gap-2">
          {cluster.state === 'configured' && !isExternal && (
            <button
              onClick={handleDeploy}
              disabled={actionLoading !== null}
              className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white text-sm rounded-lg hover:bg-green-700 disabled:opacity-50"
            >
              {actionLoading === 'deploy' ? <Loader2 size={16} className="animate-spin" /> : <Rocket size={16} />}
              Deploy
            </button>
          )}
          {!isExternal && (cluster.state === 'running' || cluster.state === 'stopped' || cluster.state === 'error') && (
            <>
              <button onClick={handleStart} disabled={actionLoading !== null}
                className="flex items-center gap-2 px-3 py-2 bg-green-600 text-white text-sm rounded-lg hover:bg-green-700 disabled:opacity-50">
                {actionLoading === 'start' ? <Loader2 size={16} className="animate-spin" /> : <Play size={16} />} Start
              </button>
              <button onClick={handleStop} disabled={actionLoading !== null}
                className="flex items-center gap-2 px-3 py-2 bg-red-600 text-white text-sm rounded-lg hover:bg-red-700 disabled:opacity-50">
                {actionLoading === 'stop' ? <Loader2 size={16} className="animate-spin" /> : <Square size={16} />} Stop
              </button>
              <button onClick={handleRefreshStatus} disabled={actionLoading !== null}
                className="flex items-center gap-2 px-3 py-2 border text-sm rounded-lg hover:bg-gray-50 disabled:opacity-50">
                {actionLoading === 'status' ? <Loader2 size={16} className="animate-spin" /> : <RefreshCw size={16} />} Refresh
              </button>
            </>
          )}
        </div>
      </div>

      {/* Deploy progress + log viewer — visible while a deploy is running and
          stays visible after a failure so the operator can read the full
          ansible log without leaving the page. */}
      {id && activeDeployTaskId && (
        <DeployProgress
          clusterId={id}
          taskId={activeDeployTaskId}
          onFinished={() => fetchDetail()}
        />
      )}

      {/* Tab bar */}
      <div className="flex border-b mb-6 overflow-x-auto">
        {visibleTabs.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors whitespace-nowrap ${
              activeTab === tab.id
                ? 'border-blue-600 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            {tab.icon} {tab.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {activeTab === 'overview' && (
        <>
          {/* Node management toolbar */}
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-sm font-semibold text-gray-700">Services ({services.length})</h2>
            <button
              onClick={handleShowAddNode}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-blue-600 text-white rounded-lg hover:bg-blue-700"
            >
              <PlusCircle size={13} /> Add Node
            </button>
          </div>

          {/* Remove error */}
          {removeError && (
            <div className="flex items-start gap-2 bg-yellow-50 border border-yellow-200 rounded-lg px-4 py-3 mb-4 text-sm text-yellow-800">
              <AlertTriangle size={16} className="mt-0.5 shrink-0" />
              <div>
                <p>{removeError}</p>
                {removeError.includes('force=true') && removingService === null && (
                  <p className="text-xs text-yellow-600 mt-1">
                    This is a safety check. If you understand the risk, you can force remove.
                  </p>
                )}
              </div>
              <button onClick={() => setRemoveError(null)} className="ml-auto text-yellow-500 hover:text-yellow-700"><XCircle size={14} /></button>
            </div>
          )}

          {/* Add node form */}
          {showAddNode && (
            <div className="bg-blue-50 border border-blue-200 rounded-xl p-4 mb-4">
              <h4 className="text-sm font-medium text-gray-800 mb-3">Add Node to Cluster</h4>
              <div className="grid grid-cols-3 gap-3">
                <div>
                  <label className="block text-xs text-gray-600 mb-1">Host</label>
                  <select value={addHostId} onChange={e => setAddHostId(e.target.value)}
                    className="w-full px-2.5 py-1.5 border rounded-lg text-sm">
                    <option value="">Select host...</option>
                    {allHosts.map(h => (
                      <option key={h.id} value={h.id}>
                        {h.hostname} ({h.ip_address}) {clusterHostIds.has(h.id) ? '[already in cluster]' : ''}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-xs text-gray-600 mb-1">Role</label>
                  <select value={addRole} onChange={e => setAddRole(e.target.value)}
                    className="w-full px-2.5 py-1.5 border rounded-lg text-sm">
                    {ROLES.map(r => <option key={r.id} value={r.id}>{r.label}</option>)}
                  </select>
                </div>
                <div>
                  <label className="block text-xs text-gray-600 mb-1">Node ID</label>
                  <input type="number" min={1} value={addNodeId}
                    onChange={e => setAddNodeId(Number(e.target.value))}
                    className="w-full px-2.5 py-1.5 border rounded-lg text-sm" />
                </div>
              </div>
              <div className="flex gap-2 mt-3">
                <button onClick={handleAddNode} disabled={addingNode || !addHostId}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50">
                  {addingNode ? <Loader2 size={13} className="animate-spin" /> : <PlusCircle size={13} />} Add
                </button>
                <button onClick={() => setShowAddNode(false)}
                  className="px-3 py-1.5 text-xs border rounded-lg hover:bg-gray-50">Cancel</button>
              </div>
            </div>
          )}

          {/* Services Table */}
          <div className="bg-white border rounded-xl overflow-hidden mb-6">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left text-gray-500 bg-gray-50">
                  <th className="px-5 py-3 font-medium">Role</th>
                  <th className="px-5 py-3 font-medium">Node ID</th>
                  <th className="px-5 py-3 font-medium">Host</th>
                  <th className="px-5 py-3 font-medium">Status</th>
                  <th className="px-5 py-3 font-medium w-16"></th>
                </tr>
              </thead>
              <tbody>
                {services.map(svc => {
                  const live = liveStatus.find(s => s.service_id === svc.id);
                  const currentStatus = live?.status || svc.status;
                  return (
                    <tr key={svc.id} className="border-b last:border-0">
                      <td className="px-5 py-3">
                        <span className={`px-2 py-0.5 rounded text-xs font-medium ${ROLE_COLORS[svc.role] || 'bg-gray-100'}`}>
                          {ROLE_LABELS[svc.role] || svc.role}
                        </span>
                      </td>
                      <td className="px-5 py-3 text-gray-600">{svc.node_id}</td>
                      <td className="px-5 py-3 text-gray-600">
                        {live ? `${live.hostname || live.host}` : svc.host_id.slice(0, 8)}
                      </td>
                      <td className="px-5 py-3">
                        <span className={`inline-flex items-center gap-1.5 text-xs font-medium ${
                          currentStatus === 'running' ? 'text-green-600' :
                          currentStatus === 'stopped' ? 'text-gray-500' :
                          currentStatus === 'installing' ? 'text-blue-600' :
                          currentStatus === 'error' ? 'text-red-600' : 'text-yellow-600'
                        }`}>
                          <span className={`w-1.5 h-1.5 rounded-full ${
                            currentStatus === 'running' ? 'bg-green-500' :
                            currentStatus === 'stopped' ? 'bg-gray-400' :
                            currentStatus === 'installing' ? 'bg-blue-500 animate-pulse' :
                            currentStatus === 'error' ? 'bg-red-500' : 'bg-yellow-500'
                          }`} />
                          {currentStatus}
                        </span>
                      </td>
                      <td className="px-5 py-3">
                        <button
                          onClick={() => {
                            if (confirm(`Remove ${ROLE_LABELS[svc.role] || svc.role} (node ${svc.node_id})? It will be stopped and removed.`)) {
                              handleRemoveService(svc.id);
                            }
                          }}
                          disabled={removingService !== null}
                          className="p-1 text-gray-400 hover:text-red-600 rounded"
                          title="Remove node"
                        >
                          {removingService === svc.id ? <Loader2 size={14} className="animate-spin" /> : <Trash2 size={14} />}
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </>
      )}

      {activeTab === 'topics' && id && <TopicManager clusterId={id} />}
      {activeTab === 'consume' && id && <ConsumeMessages clusterId={id} />}
      {activeTab === 'produce' && id && <ProduceMessage clusterId={id} />}
      {activeTab === 'consumers' && id && <ConsumerGroups clusterId={id} />}
      {activeTab === 'connect' && id && <ConnectManager clusterId={id} />}
      {activeTab === 'security' && id && (
        <div className="space-y-6">
          {!isExternal && <TLSPanel clusterId={id} clusterRunning={isRunning} />}
          <SecurityManager clusterId={id} isExternal={isExternal} />
        </div>
      )}
      {activeTab === 'ksqldb' && id && <KsqlManager clusterId={id} />}
      {activeTab === 'schema-registry' && id && (
        <ClusterSchemaRegistry clusterId={id} clusterHostIds={Array.from(clusterHostIds)} />
      )}
      {activeTab === 'config' && id && <BrokerConfigManager clusterId={id} />}
      {activeTab === 'restart' && id && <RollingRestart clusterId={id} />}
      {activeTab === 'rebalance' && id && <PartitionRebalance clusterId={id} />}
      {activeTab === 'upgrade' && id && <UpgradeManager clusterId={id} currentVersion={cluster.kafka_version} />}
      {activeTab === 'capacity' && id && <CapacityForecast clusterId={id} />}
      {activeTab === 'monitoring' && id && <ClusterMonitoring clusterId={id} isExternal={isExternal} />}
      {activeTab === 'lifecycle' && id && isExternal && <ExternalLifecycle clusterId={id} />}

      {activeTab === 'validate' && id && (
        <div>
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="text-sm font-semibold text-gray-700">Kafka Cluster Validation</h3>
              <p className="text-xs text-gray-500 mt-1">
                Tests broker connectivity, topic creation, message produce/consume round-trip
              </p>
            </div>
            <button
              onClick={handleValidate}
              disabled={validating}
              className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white text-sm rounded-lg hover:bg-green-700 disabled:opacity-50"
            >
              {validating ? <Loader2 size={16} className="animate-spin" /> : <ShieldCheck size={16} />}
              {validating ? 'Validating...' : 'Run Validation'}
            </button>
          </div>

          {/* Validation Results */}
          {validationSteps.length > 0 && (
            <div className="space-y-3">
              {/* Overall result */}
              <div className={`flex items-center gap-3 px-4 py-3 rounded-xl border ${
                validationSuccess === true ? 'bg-green-50 border-green-200' :
                validationSuccess === false ? 'bg-red-50 border-red-200' :
                'bg-gray-50 border-gray-200'
              }`}>
                {validationSuccess === true ? (
                  <CheckCircle size={20} className="text-green-600" />
                ) : validationSuccess === false ? (
                  <XCircle size={20} className="text-red-600" />
                ) : null}
                <span className={`text-sm font-medium ${
                  validationSuccess === true ? 'text-green-700' :
                  validationSuccess === false ? 'text-red-700' : 'text-gray-700'
                }`}>
                  {validationSuccess === true ? 'All validation steps passed' :
                   validationSuccess === false ? 'Validation failed' : 'Validating...'}
                </span>
              </div>

              {/* Step by step */}
              {validationSteps.map((step, i) => (
                <div key={i} className="bg-white border rounded-lg px-4 py-3">
                  <div className="flex items-center gap-3">
                    {step.success ? (
                      <CheckCircle size={16} className="text-green-500 shrink-0" />
                    ) : (
                      <XCircle size={16} className="text-red-500 shrink-0" />
                    )}
                    <div className="flex-1 min-w-0">
                      <span className="text-sm font-medium text-gray-800 capitalize">
                        {step.step.replace(/_/g, ' ')}
                      </span>
                      <p className="text-xs text-gray-500 mt-0.5">{step.message}</p>
                    </div>
                  </div>
                  {/* Show consumed messages data if present */}
                  {step.data && step.step === 'consume_message' && Array.isArray(step.data) && (
                    <div className="mt-2 ml-7 space-y-1">
                      {(step.data as Array<Record<string, unknown>>).slice(0, 3).map((msg, j) => (
                        <div key={j} className="text-xs font-mono bg-gray-50 rounded px-2 py-1 flex items-center gap-3">
                          {msg.partition !== null && <span className="text-purple-600">P{String(msg.partition)}</span>}
                          {msg.offset !== null && <span className="text-gray-400">@{String(msg.offset)}</span>}
                          <span className="text-gray-700 truncate">{String(msg.value ?? '')}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}

          {validationSteps.length === 0 && !validating && (
            <div className="text-center py-12 text-gray-400 text-sm">
              Click "Run Validation" to test your Kafka cluster end-to-end.
            </div>
          )}
        </div>
      )}

      {activeTab === 'service-logs' && id && (
        <ServiceLogs clusterId={id} services={services} />
      )}

    </div>
  );
}
