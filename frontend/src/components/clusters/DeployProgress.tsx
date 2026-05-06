import { useEffect, useRef, useState } from 'react';
import { CheckCircle2, XCircle, Loader2, AlertTriangle } from 'lucide-react';
import { getDeploymentStatus } from '../../lib/api';
import type { DeploymentTask } from '../../types';

type Props = {
  clusterId: string;
  taskId: string | null;
  onFinished?: (status: string) => void;
};

/**
 * Live view of a cluster deploy task. Polls `/clusters/{id}/deploy/{task_id}`
 * every 2s while running, freezes on terminal status, and surfaces the FULL
 * log buffer + error message so a failed deploy is diagnosable from the UI
 * (no SSH'ing to the server to read journalctl).
 */
export function DeployProgress({ clusterId, taskId, onFinished }: Props) {
  const [task, setTask] = useState<DeploymentTask | null>(null);
  const [showLogs, setShowLogs] = useState(true);
  const logEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!taskId) return;
    let cancelled = false;
    let interval: ReturnType<typeof setInterval> | null = null;

    const tick = async () => {
      try {
        const t = await getDeploymentStatus(clusterId, taskId);
        if (cancelled) return;
        setTask(t);
        const terminal = ['completed', 'failed', 'error', 'completed_with_errors'].some(s =>
          t.status?.toLowerCase().startsWith(s)
        );
        if (terminal && interval) {
          clearInterval(interval);
          onFinished?.(t.status);
        }
      } catch {
        // best-effort; the next tick will retry
      }
    };
    tick();
    interval = setInterval(tick, 2000);
    return () => { cancelled = true; if (interval) clearInterval(interval); };
  }, [clusterId, taskId, onFinished]);

  useEffect(() => {
    if (showLogs && logEndRef.current) {
      logEndRef.current.scrollIntoView({ behavior: 'auto', block: 'end' });
    }
  }, [task?.logs?.length, showLogs]);

  if (!taskId || !task) return null;

  const status = (task.status || '').toLowerCase();
  const isError = status.includes('error') || status === 'failed';
  const isRunning = status === 'running' || status === 'pending';
  const isDone = status === 'completed' || status === 'succeeded';
  const isPartial = status === 'completed_with_errors';

  const StatusIcon = isError || isPartial ? XCircle :
                     isDone ? CheckCircle2 :
                     isRunning ? Loader2 : AlertTriangle;
  const statusColor = isError || isPartial ? 'text-red-600' :
                      isDone ? 'text-green-600' :
                      'text-blue-600';
  const bannerBg = isError || isPartial ? 'bg-red-50 border-red-200' :
                   isDone ? 'bg-green-50 border-green-200' :
                   'bg-blue-50 border-blue-200';

  return (
    <div className={`mb-4 rounded-lg border ${bannerBg}`}>
      <div className="flex items-center gap-3 px-4 py-3">
        <StatusIcon size={20} className={`${statusColor} ${isRunning ? 'animate-spin' : ''}`} />
        <div className="flex-1 min-w-0">
          <div className="text-sm font-medium text-gray-900">
            Deploy: <span className={statusColor}>{task.status}</span>
            {task.current_step && (
              <span className="text-gray-600 font-normal ml-2">— {task.current_step}</span>
            )}
          </div>
          {(isError || isPartial) && task.error_message && (
            <div className="text-xs text-red-700 mt-1 font-mono break-words">
              {task.error_message}
            </div>
          )}
        </div>
        <button
          onClick={() => setShowLogs(s => !s)}
          className="text-xs text-gray-600 hover:text-gray-900 px-2 py-1 border border-gray-300 rounded"
        >
          {showLogs ? 'Hide' : 'Show'} logs ({task.logs?.length ?? 0})
        </button>
      </div>
      {showLogs && (
        <div className="border-t border-gray-200 bg-gray-900 text-gray-100 max-h-80 overflow-y-auto px-4 py-3 font-mono text-xs leading-relaxed rounded-b-lg">
          {(!task.logs || task.logs.length === 0) ? (
            <div className="text-gray-400 italic">No log lines yet…</div>
          ) : (
            task.logs.map((line, i) => (
              <div
                key={i}
                className={
                  /error|failed|fatal/i.test(line) ? 'text-red-300' :
                  /warn/i.test(line)              ? 'text-yellow-300' :
                  /✓|success|completed|ok/i.test(line) ? 'text-green-300' :
                  ''
                }
              >
                {line}
              </div>
            ))
          )}
          <div ref={logEndRef} />
        </div>
      )}
    </div>
  );
}
