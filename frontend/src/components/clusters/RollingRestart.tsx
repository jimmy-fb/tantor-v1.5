import { useState, useEffect, useRef, useCallback } from 'react';
import {
  RefreshCw, CheckCircle, XCircle, AlertTriangle,
  Play, Loader2, Server, Activity, ChevronDown, ChevronUp,
} from 'lucide-react';
import axios from 'axios';
import { getAccessToken } from '../../lib/auth';

const authApi = axios.create({ baseURL: '/api' });
authApi.interceptors.request.use((config) => {
  const token = getAccessToken();
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

interface Props {
  clusterId: string;
  isExternal?: boolean;
}

interface BrokerCheck {
  broker_id: number;
  host: string;
  healthy: boolean;
  message: string;
}

interface PreCheckResult {
  cluster_name: string;
  broker_count: number;
  all_healthy: boolean;
  checks: BrokerCheck[];
}

interface RestartProgress {
  current: number;
  total: number;
  current_broker: string | null;
}

interface RestartTask {
  status: 'running' | 'completed' | 'error';
  logs: string[];
  progress: RestartProgress;
}

type RestartScope = 'brokers' | 'controllers' | 'all';

const SCOPE_OPTIONS: { value: RestartScope; label: string; description: string }[] = [
  { value: 'brokers', label: 'Brokers Only', description: 'Restart Kafka broker nodes one at a time' },
  { value: 'controllers', label: 'Controllers', description: 'Restart KRaft controller nodes one at a time' },
  { value: 'all', label: 'All Services', description: 'Restart all services: controllers, brokers, then extras' },
];

export default function RollingRestart({ clusterId, isExternal = false }: Props) {
  const [preCheck, setPreCheck] = useState<PreCheckResult | null>(null);
  const [preCheckLoading, setPreCheckLoading] = useState(false);
  const [preCheckError, setPreCheckError] = useState('');

  const [scope, setScope] = useState<RestartScope>('brokers');
  const [showConfirm, setShowConfirm] = useState(false);
  const [starting, setStarting] = useState(false);

  const [, setTaskId] = useState<string | null>(null);
  const [task, setTask] = useState<RestartTask | null>(null);
  const [showLogs, setShowLogs] = useState(true);

  const logContainerRef = useRef<HTMLDivElement>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Auto-scroll logs
  useEffect(() => {
    if (logContainerRef.current) {
      logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
    }
  }, [task?.logs]);

  // Cleanup polling on unmount
  useEffect(() => {
    return () => {
      if (pollRef.current) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
    };
  }, []);

  const runPreCheck = useCallback(async () => {
    setPreCheckLoading(true);
    setPreCheckError('');
    try {
      const preCheckUrl = isExternal
        ? `/rolling-restart/external/${clusterId}/pre-check`
        : `/rolling-restart/clusters/${clusterId}/pre-check`;
      const res = await authApi.get(preCheckUrl);
      setPreCheck(res.data);
    } catch (err: unknown) {
      if (axios.isAxiosError(err)) {
        setPreCheckError(err.response?.data?.detail || 'Failed to run pre-check');
      } else {
        setPreCheckError('Failed to run pre-check');
      }
    } finally {
      setPreCheckLoading(false);
    }
  }, [clusterId, isExternal]);

  // Run pre-check on mount
  useEffect(() => {
    runPreCheck();
  }, [runPreCheck]);

  const pollTask = useCallback(async (id: string) => {
    try {
      const res = await authApi.get(`/rolling-restart/tasks/${id}`);
      const data: RestartTask = res.data;
      setTask(data);
      if (data.status !== 'running') {
        if (pollRef.current) {
          clearInterval(pollRef.current);
          pollRef.current = null;
        }
      }
    } catch {
      // Silently ignore poll errors
    }
  }, []);

  const startRestart = async () => {
    setStarting(true);
    setShowConfirm(false);
    try {
      const startUrl = isExternal
        ? `/rolling-restart/external/${clusterId}`
        : `/rolling-restart/clusters/${clusterId}`;
      const res = await authApi.post(startUrl, isExternal ? {} : { scope });
      const id = res.data.task_id;
      setTaskId(id);
      setTask({ status: 'running', logs: [], progress: { current: 0, total: 0, current_broker: null } });
      setShowLogs(true);

      // Start polling
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = setInterval(() => pollTask(id), 2000);
    } catch (err: unknown) {
      if (axios.isAxiosError(err)) {
        setPreCheckError(err.response?.data?.detail || 'Failed to start rolling restart');
      } else {
        setPreCheckError('Failed to start rolling restart');
      }
    } finally {
      setStarting(false);
    }
  };

  const resetState = () => {
    setTaskId(null);
    setTask(null);
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
    runPreCheck();
  };

  const isRunning = task?.status === 'running';
  const isCompleted = task?.status === 'completed';
  const isError = task?.status === 'error';
  const progressPct = task?.progress?.total
    ? Math.round((task.progress.current / task.progress.total) * 100)
    : 0;

  const getLogLineColor = (line: string) => {
    if (line.includes('\u2717') || line.includes('ERROR') || line.includes('failed')) return 'text-red-400';
    if (line.includes('\u2713')) return 'text-green-400';
    if (line.includes('\u26A0') || line.includes('WARNING') || line.includes('timeout')) return 'text-yellow-400';
    if (line.includes('\u2501')) return 'text-blue-400';
    return 'text-gray-300';
  };

  return (
    <div className="space-y-6">
      {/* Pre-restart Health Check */}
      <div className="bg-white rounded-xl border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <Activity size={20} className="text-blue-600" />
            <h3 className="text-lg font-semibold text-gray-900">Pre-Restart Health Check</h3>
          </div>
          <button
            onClick={runPreCheck}
            disabled={preCheckLoading || isRunning}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-100 hover:bg-gray-200 disabled:bg-gray-50 disabled:text-gray-400 text-gray-700 rounded-lg text-sm font-medium transition-colors"
          >
            <RefreshCw size={14} className={preCheckLoading ? 'animate-spin' : ''} />
            Refresh
          </button>
        </div>

        {preCheckError && (
          <div className="flex items-center gap-2 p-3 bg-red-50 border border-red-200 rounded-lg mb-4">
            <XCircle size={16} className="text-red-500 shrink-0" />
            <span className="text-sm text-red-700">{preCheckError}</span>
          </div>
        )}

        {preCheckLoading && !preCheck && (
          <div className="flex items-center justify-center py-8">
            <Loader2 size={24} className="animate-spin text-blue-500" />
            <span className="ml-2 text-gray-500">Running health checks...</span>
          </div>
        )}

        {preCheck && (
          <div className="space-y-3">
            <div className="flex items-center gap-3 text-sm">
              <span className="text-gray-500">Cluster:</span>
              <span className="font-medium text-gray-900">{preCheck.cluster_name}</span>
              <span className="text-gray-400">|</span>
              <span className="text-gray-500">Brokers:</span>
              <span className="font-medium text-gray-900">{preCheck.broker_count}</span>
              <span className="text-gray-400">|</span>
              <span className="text-gray-500">Status:</span>
              {preCheck.all_healthy ? (
                <span className="flex items-center gap-1 text-green-600 font-medium">
                  <CheckCircle size={14} /> All Healthy
                </span>
              ) : (
                <span className="flex items-center gap-1 text-yellow-600 font-medium">
                  <AlertTriangle size={14} /> Issues Detected
                </span>
              )}
            </div>

            {preCheck.checks.length > 0 && (
              <div className="grid gap-2">
                {preCheck.checks.map((check, idx) => (
                  <div
                    key={idx}
                    className={`flex items-center gap-3 px-3 py-2 rounded-lg text-sm ${
                      check.healthy
                        ? 'bg-green-50 border border-green-200'
                        : 'bg-red-50 border border-red-200'
                    }`}
                  >
                    {check.healthy ? (
                      <CheckCircle size={16} className="text-green-500 shrink-0" />
                    ) : (
                      <XCircle size={16} className="text-red-500 shrink-0" />
                    )}
                    <Server size={14} className="text-gray-400 shrink-0" />
                    <span className="font-mono text-gray-700">Node {check.broker_id}</span>
                    <span className="text-gray-400">({check.host})</span>
                    <span className="flex-1" />
                    <span className={check.healthy ? 'text-green-700' : 'text-red-700'}>
                      {check.message}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Restart Configuration & Trigger */}
      {!task && (
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <div className="flex items-center gap-2 mb-4">
            <RefreshCw size={20} className="text-blue-600" />
            <h3 className="text-lg font-semibold text-gray-900">Rolling Restart</h3>
          </div>

          {/* Scope selector — hidden for external clusters (brokers only) */}
          {!isExternal && (
          <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-2">Restart Scope</label>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
              {SCOPE_OPTIONS.map((opt) => (
                <button
                  key={opt.value}
                  onClick={() => setScope(opt.value)}
                  className={`text-left p-3 rounded-lg border-2 transition-colors ${
                    scope === opt.value
                      ? 'border-blue-500 bg-blue-50'
                      : 'border-gray-200 hover:border-gray-300 bg-white'
                  }`}
                >
                  <div className={`text-sm font-medium ${scope === opt.value ? 'text-blue-700' : 'text-gray-900'}`}>
                    {opt.label}
                  </div>
                  <div className="text-xs text-gray-500 mt-0.5">{opt.description}</div>
                </button>
              ))}
            </div>
          </div>
          )}

          {/* Start button */}
          <div className="flex items-center gap-3">
            <button
              onClick={() => setShowConfirm(true)}
              disabled={starting}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-300 text-white rounded-lg font-medium transition-colors"
            >
              <Play size={16} />
              Start Rolling Restart
            </button>
            {preCheck && !preCheck.all_healthy && (
              <span className="flex items-center gap-1 text-sm text-yellow-600">
                <AlertTriangle size={14} />
                Some brokers are unhealthy. Proceed with caution.
              </span>
            )}
          </div>

          {/* Confirmation Dialog */}
          {showConfirm && (
            <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
              <div className="bg-white rounded-xl shadow-xl max-w-md w-full mx-4 p-6">
                <div className="flex items-center gap-2 mb-3">
                  <AlertTriangle size={20} className="text-yellow-500" />
                  <h4 className="text-lg font-semibold text-gray-900">Confirm Rolling Restart</h4>
                </div>
                <p className="text-sm text-gray-600 mb-2">
                  This will restart services one at a time with health checks between each restart.
                </p>
                <div className="bg-gray-50 rounded-lg p-3 mb-4">
                  <div className="text-sm">
                    <span className="text-gray-500">Scope: </span>
                    <span className="font-medium text-gray-900">
                      {SCOPE_OPTIONS.find((o) => o.value === scope)?.label}
                    </span>
                  </div>
                  {preCheck && (
                    <div className="text-sm mt-1">
                      <span className="text-gray-500">Cluster: </span>
                      <span className="font-medium text-gray-900">{preCheck.cluster_name}</span>
                    </div>
                  )}
                </div>
                <div className="flex items-center justify-end gap-3">
                  <button
                    onClick={() => setShowConfirm(false)}
                    className="px-4 py-2 bg-gray-100 hover:bg-gray-200 text-gray-700 rounded-lg text-sm font-medium transition-colors"
                  >
                    Cancel
                  </button>
                  <button
                    onClick={startRestart}
                    disabled={starting}
                    className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-300 text-white rounded-lg text-sm font-medium transition-colors"
                  >
                    {starting ? <Loader2 size={14} className="animate-spin" /> : <Play size={14} />}
                    Confirm Restart
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Progress & Logs */}
      {task && (
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          {/* Status Header */}
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-3">
              {isRunning && (
                <>
                  <Loader2 size={20} className="animate-spin text-blue-500" />
                  <h3 className="text-lg font-semibold text-gray-900">Rolling Restart In Progress</h3>
                </>
              )}
              {isCompleted && (
                <>
                  <CheckCircle size={20} className="text-green-500" />
                  <h3 className="text-lg font-semibold text-green-700">Rolling Restart Completed</h3>
                </>
              )}
              {isError && (
                <>
                  <XCircle size={20} className="text-red-500" />
                  <h3 className="text-lg font-semibold text-red-700">Rolling Restart Failed</h3>
                </>
              )}
            </div>
            {!isRunning && (
              <button
                onClick={resetState}
                className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-100 hover:bg-gray-200 text-gray-700 rounded-lg text-sm font-medium transition-colors"
              >
                <RefreshCw size={14} />
                New Restart
              </button>
            )}
          </div>

          {/* Progress bar */}
          {task.progress.total > 0 && (
            <div className="mb-4">
              <div className="flex items-center justify-between text-sm mb-1">
                <span className="text-gray-600">
                  {task.progress.current} of {task.progress.total} services
                </span>
                <span className="font-medium text-gray-900">{progressPct}%</span>
              </div>
              <div className="h-2.5 bg-gray-200 rounded-full overflow-hidden">
                <div
                  className={`h-full rounded-full transition-all duration-500 ${
                    isError ? 'bg-red-500' : isCompleted ? 'bg-green-500' : 'bg-blue-500'
                  }`}
                  style={{ width: `${progressPct}%` }}
                />
              </div>
              {isRunning && task.progress.current_broker && (
                <div className="flex items-center gap-2 mt-2 text-sm text-blue-600">
                  <Server size={14} />
                  <span>Currently restarting: {task.progress.current_broker}</span>
                </div>
              )}
            </div>
          )}

          {/* Log output */}
          <div>
            <button
              onClick={() => setShowLogs(!showLogs)}
              className="flex items-center gap-1.5 text-sm font-medium text-gray-600 hover:text-gray-900 mb-2"
            >
              {showLogs ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
              {showLogs ? 'Hide' : 'Show'} Logs ({task.logs.length} lines)
            </button>
            {showLogs && (
              <div
                ref={logContainerRef}
                className="bg-gray-900 rounded-xl p-4 h-[400px] overflow-y-auto font-mono text-xs leading-relaxed"
              >
                {task.logs.length === 0 ? (
                  <div className="flex items-center justify-center h-full text-gray-500">
                    <span>Waiting for output...</span>
                  </div>
                ) : (
                  task.logs.map((line, i) => (
                    <div key={i} className={`${getLogLineColor(line)} hover:bg-gray-800/50 px-1 rounded`}>
                      {line || '\u00A0'}
                    </div>
                  ))
                )}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
