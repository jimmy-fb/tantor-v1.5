import { useState, useEffect } from 'react';
import {
  Plus, Trash2, RefreshCw, Pause, Play, RotateCw,
  Loader2, Plug, ChevronDown, ChevronUp, Database,
} from 'lucide-react';
import type { ConnectorStatus, ConnectorPluginInfo } from '../../types';
import {
  getConnectors, createConnector, deleteConnector,
  pauseConnector, resumeConnector, restartConnector, getConnectPlugins,
} from '../../lib/api';
import CdcWizard from './CdcWizard';

interface Props {
  clusterId: string;
}

const CONNECTOR_STATE_COLORS: Record<string, string> = {
  RUNNING: 'bg-green-100 text-green-700',
  PAUSED: 'bg-yellow-100 text-yellow-700',
  FAILED: 'bg-red-100 text-red-700',
  UNASSIGNED: 'bg-gray-100 text-gray-600',
};

export default function ConnectManager({ clusterId }: Props) {
  const [connectors, setConnectors] = useState<ConnectorStatus[]>([]);
  const [plugins, setPlugins] = useState<ConnectorPluginInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [showDeploy, setShowDeploy] = useState(false);
  const [showCdcWizard, setShowCdcWizard] = useState(false);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [actionLoading, setActionLoading] = useState<string | null>(null);

  // Deploy form
  const [newName, setNewName] = useState('');
  const [newConfig, setNewConfig] = useState('{\n  "connector.class": "",\n  "tasks.max": "1"\n}');
  const [deploying, setDeploying] = useState(false);
  const [deployError, setDeployError] = useState<string | null>(null);

  const fetchConnectors = async () => {
    setLoading(true);
    try {
      const data = await getConnectors(clusterId);
      setConnectors(data);
    } catch {
      setConnectors([]);
    } finally {
      setLoading(false);
    }
  };

  const fetchPlugins = async () => {
    try {
      const data = await getConnectPlugins(clusterId);
      setPlugins(data);
    } catch {
      setPlugins([]);
    }
  };

  useEffect(() => {
    fetchConnectors();
    fetchPlugins();
  }, [clusterId]);

  const handleDeploy = async () => {
    if (!newName.trim()) return;
    setDeploying(true);
    setDeployError(null);
    try {
      const configObj = JSON.parse(newConfig);
      await createConnector(clusterId, { name: newName.trim(), config: configObj });
      setNewName('');
      setNewConfig('{\n  "connector.class": "",\n  "tasks.max": "1"\n}');
      setShowDeploy(false);
      fetchConnectors();
    } catch (e: unknown) {
      if (e instanceof SyntaxError) {
        setDeployError('Invalid JSON configuration');
      } else {
        setDeployError('Failed to deploy connector');
      }
    } finally {
      setDeploying(false);
    }
  };

  const handleAction = async (name: string, action: 'pause' | 'resume' | 'restart' | 'delete') => {
    setActionLoading(`${name}-${action}`);
    try {
      switch (action) {
        case 'pause': await pauseConnector(clusterId, name); break;
        case 'resume': await resumeConnector(clusterId, name); break;
        case 'restart': await restartConnector(clusterId, name); break;
        case 'delete':
          if (!confirm(`Delete connector "${name}"?`)) { setActionLoading(null); return; }
          await deleteConnector(clusterId, name);
          break;
      }
      fetchConnectors();
    } finally {
      setActionLoading(null);
    }
  };

  const getConnectorState = (c: ConnectorStatus): string => {
    return String(c.connector?.state ?? 'UNKNOWN');
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-gray-700">
          Connectors ({connectors.length})
        </h3>
        <div className="flex gap-2">
          <button
            onClick={fetchConnectors}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs border rounded-lg hover:bg-gray-50"
          >
            <RefreshCw size={13} /> Refresh
          </button>
          <button
            onClick={() => setShowCdcWizard(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs border border-blue-300 bg-white text-blue-700 rounded-lg hover:bg-blue-50"
            title="Pre-curated Debezium MySQL/Postgres/Mongo/SQL Server templates"
          >
            <Database size={13} /> CDC quickstart
          </button>
          <button
            onClick={() => setShowDeploy(!showDeploy)}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-blue-600 text-white rounded-lg hover:bg-blue-700"
          >
            <Plus size={13} /> Deploy Connector
          </button>
        </div>
      </div>

      {showCdcWizard && (
        <CdcWizard
          clusterId={clusterId}
          onClose={() => setShowCdcWizard(false)}
          onCreated={() => fetchConnectors()}
        />
      )}

      {/* Deploy form */}
      {showDeploy && (
        <div className="bg-blue-50 border border-blue-200 rounded-xl p-4 mb-4">
          <h4 className="text-sm font-medium text-gray-800 mb-3">Deploy New Connector</h4>
          <div className="space-y-3">
            <div>
              <label className="block text-xs text-gray-600 mb-1">Connector Name</label>
              <input
                type="text"
                value={newName}
                onChange={e => setNewName(e.target.value)}
                placeholder="my-source-connector"
                className="w-full px-2.5 py-1.5 border rounded-lg text-sm"
              />
            </div>

            {plugins.length > 0 && (
              <div>
                <label className="block text-xs text-gray-600 mb-1">Available Plugins</label>
                <div className="flex flex-wrap gap-1.5">
                  {plugins.map((p, i) => (
                    <span key={i} className="px-2 py-0.5 bg-white border rounded text-xs text-gray-600 font-mono">
                      {p.class_name.split('.').pop()} <span className="text-gray-400">({p.type})</span>
                    </span>
                  ))}
                </div>
              </div>
            )}

            <div>
              <label className="block text-xs text-gray-600 mb-1">Configuration (JSON)</label>
              <textarea
                value={newConfig}
                onChange={e => setNewConfig(e.target.value)}
                rows={8}
                className="w-full px-2.5 py-1.5 border rounded-lg text-sm font-mono resize-y"
              />
            </div>

            {deployError && (
              <p className="text-xs text-red-600">{deployError}</p>
            )}

            <div className="flex gap-2">
              <button
                onClick={handleDeploy}
                disabled={deploying || !newName.trim()}
                className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50"
              >
                {deploying ? <Loader2 size={13} className="animate-spin" /> : <Plug size={13} />}
                Deploy
              </button>
              <button
                onClick={() => { setShowDeploy(false); setDeployError(null); }}
                className="px-3 py-1.5 text-xs border rounded-lg hover:bg-gray-50"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {loading ? (
        <div className="text-center py-8 text-gray-400 text-sm">Loading connectors...</div>
      ) : connectors.length === 0 ? (
        <div className="text-center py-8">
          <Plug size={36} className="mx-auto text-gray-300 mb-2" />
          <p className="text-gray-400 text-sm">No connectors deployed.</p>
        </div>
      ) : (
        <div className="space-y-2">
          {connectors.map(conn => {
            const state = getConnectorState(conn);
            const isExpanded = expanded === conn.name;
            return (
              <div key={conn.name} className="bg-white border rounded-xl overflow-hidden">
                <div className="flex items-center gap-4 px-4 py-3">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="font-mono text-sm font-medium text-gray-900">{conn.name}</span>
                      <span className={`px-2 py-0.5 rounded text-xs font-medium ${CONNECTOR_STATE_COLORS[state] || 'bg-gray-100 text-gray-600'}`}>
                        {state}
                      </span>
                      <span className="text-xs text-gray-400">{conn.type}</span>
                    </div>
                    <div className="text-xs text-gray-400 mt-0.5">
                      {conn.tasks.length} task{conn.tasks.length !== 1 ? 's' : ''}
                    </div>
                  </div>

                  <div className="flex items-center gap-1">
                    {state === 'RUNNING' && (
                      <button
                        onClick={() => handleAction(conn.name, 'pause')}
                        disabled={actionLoading !== null}
                        className="p-1.5 text-gray-400 hover:text-yellow-600 rounded hover:bg-yellow-50"
                        title="Pause"
                      >
                        {actionLoading === `${conn.name}-pause` ? <Loader2 size={14} className="animate-spin" /> : <Pause size={14} />}
                      </button>
                    )}
                    {state === 'PAUSED' && (
                      <button
                        onClick={() => handleAction(conn.name, 'resume')}
                        disabled={actionLoading !== null}
                        className="p-1.5 text-gray-400 hover:text-green-600 rounded hover:bg-green-50"
                        title="Resume"
                      >
                        {actionLoading === `${conn.name}-resume` ? <Loader2 size={14} className="animate-spin" /> : <Play size={14} />}
                      </button>
                    )}
                    <button
                      onClick={() => handleAction(conn.name, 'restart')}
                      disabled={actionLoading !== null}
                      className="p-1.5 text-gray-400 hover:text-blue-600 rounded hover:bg-blue-50"
                      title="Restart"
                    >
                      {actionLoading === `${conn.name}-restart` ? <Loader2 size={14} className="animate-spin" /> : <RotateCw size={14} />}
                    </button>
                    <button
                      onClick={() => handleAction(conn.name, 'delete')}
                      disabled={actionLoading !== null}
                      className="p-1.5 text-gray-400 hover:text-red-600 rounded hover:bg-red-50"
                      title="Delete"
                    >
                      {actionLoading === `${conn.name}-delete` ? <Loader2 size={14} className="animate-spin" /> : <Trash2 size={14} />}
                    </button>
                    <button
                      onClick={() => setExpanded(isExpanded ? null : conn.name)}
                      className="p-1.5 text-gray-400 hover:text-gray-600 rounded"
                    >
                      {isExpanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                    </button>
                  </div>
                </div>

                {isExpanded && (
                  <div className="border-t bg-gray-50 px-4 py-3">
                    <h5 className="text-xs font-semibold text-gray-700 mb-2">Tasks</h5>
                    {conn.tasks.length === 0 ? (
                      <p className="text-xs text-gray-400 italic">No tasks</p>
                    ) : (
                      <div className="space-y-1">
                        {conn.tasks.map((task, i) => (
                          <div key={i} className="flex items-center gap-3 text-xs font-mono">
                            <span className="text-gray-500">Task {String(task.id ?? i)}</span>
                            <span className={`px-1.5 py-0.5 rounded ${
                              CONNECTOR_STATE_COLORS[String(task.state ?? '')] || 'bg-gray-100 text-gray-600'
                            }`}>
                              {String(task.state ?? 'UNKNOWN')}
                            </span>
                            {task.worker_id ? <span className="text-gray-400">on {String(task.worker_id)}</span> : null}
                            {task.trace ? (
                              <span className="text-red-500 truncate max-w-md" title={String(task.trace)}>
                                {String(task.trace).split('\n')[0]}
                              </span>
                            ) : null}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
