import { useEffect, useState } from 'react';
import { Antenna, Copy, KeyRound, Loader2, Power, X } from 'lucide-react';
import {
  type AgentRow,
  type MintAgentTokenResponse,
  listAgents,
  mintAgentToken,
  revokeAgent,
} from '../../lib/api';
import { isAdmin } from '../../lib/auth';

interface Props {
  hostId: string;
}

/**
 * AgentControls — collapsible row on the Host detail showing the
 * tantor-agent status for this host and offering admin-only token /
 * revoke actions. Lives next to the SSH-based "Test connection" /
 * "Prerequisites" buttons so operators see both transports side-by-side.
 *
 * See docs/AGENT_PROTOCOL.md.
 */
export default function AgentControls({ hostId }: Props) {
  const [agent, setAgent] = useState<AgentRow | null>(null);
  const [loading, setLoading] = useState(false);
  const [minting, setMinting] = useState(false);
  const [revoking, setRevoking] = useState(false);
  const [token, setToken] = useState<MintAgentTokenResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const refresh = async () => {
    setLoading(true);
    try {
      const rows = await listAgents();
      setAgent(rows.find((a) => a.host_id === hostId) ?? null);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refresh();
    // Live-poll while the dialog is visible so operators see the green
    // "connected" badge appear within ~15s of starting the agent.
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hostId]);

  const handleMint = async () => {
    setError(null);
    setMinting(true);
    try {
      const resp = await mintAgentToken(hostId);
      setToken(resp);
      await refresh();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setMinting(false);
    }
  };

  const handleRevoke = async () => {
    if (!confirm('Revoke this agent? The host\'s agent will lose its connection and any future reconnects will be rejected until a new token is minted.')) return;
    setError(null);
    setRevoking(true);
    try {
      await revokeAgent(hostId);
      setToken(null);
      await refresh();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRevoking(false);
    }
  };

  const handleCopy = async () => {
    if (!token) return;
    await navigator.clipboard.writeText(token.registration_token);
    setCopied(true);
    setTimeout(() => setCopied(false), 2500);
  };

  const stateLabel = (() => {
    if (loading && !agent) return 'loading…';
    if (!agent) return 'no agent configured';
    if (!agent.is_active) return 'revoked';
    if (agent.connected) return 'connected';
    if (agent.has_pending_token) return 'awaiting first connect';
    return 'configured but disconnected';
  })();

  const stateColor = (() => {
    if (!agent || !agent.is_active) return 'text-gray-500';
    if (agent.connected) return 'text-green-700';
    if (agent.has_pending_token) return 'text-amber-700';
    return 'text-red-600';
  })();

  return (
    <div className="mt-4 border-t pt-4">
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <Antenna size={16} className={stateColor} />
          <div>
            <div className="text-sm font-medium">
              Agent: <span className={stateColor}>{stateLabel}</span>
            </div>
            <div className="text-xs text-gray-500">
              {agent?.agent_version && <>v{agent.agent_version} · </>}
              {agent?.os_family && <>{agent.os_family}{agent.os_version ? ` ${agent.os_version}` : ''} · </>}
              {agent?.features?.length ? `caps: ${agent.features.join(', ')}` : 'reverse-tunnel WSS, no inbound SSH needed'}
            </div>
          </div>
        </div>

        {isAdmin() && (
          <div className="flex items-center gap-2">
            <button
              onClick={handleMint}
              disabled={minting}
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm border rounded-lg hover:bg-gray-50 disabled:opacity-50"
              title="Generate a one-shot registration token for the broker host to paste into /etc/tantor-agent/config.yaml"
            >
              {minting ? <Loader2 size={14} className="animate-spin" /> : <KeyRound size={14} />}
              {agent?.has_pending_token ? 'Re-issue token' : 'Generate token'}
            </button>
            {agent && agent.is_active && (
              <button
                onClick={handleRevoke}
                disabled={revoking}
                className="flex items-center gap-1.5 px-3 py-1.5 text-sm text-red-600 border border-red-200 rounded-lg hover:bg-red-50 disabled:opacity-50"
                title="Revoke the agent — disconnect it and reject future reconnects."
              >
                {revoking ? <Loader2 size={14} className="animate-spin" /> : <Power size={14} />}
                Revoke
              </button>
            )}
          </div>
        )}
      </div>

      {error && (
        <div className="mt-3 px-3 py-2 rounded-lg text-sm bg-red-50 text-red-700">{error}</div>
      )}

      {token && (
        <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm relative">
          <button
            onClick={() => setToken(null)}
            className="absolute top-2 right-2 text-amber-700 hover:text-amber-900"
            title="Dismiss"
          >
            <X size={14} />
          </button>
          <p className="text-amber-800 font-medium">
            Token issued — copy it now. We can't show it again.
          </p>
          <p className="text-amber-700 mt-1 text-xs">
            Expires {new Date(token.expires_at).toLocaleString()}. Paste into{' '}
            <code className="px-1 bg-white rounded">/etc/tantor-agent/config.yaml</code> on the broker host.
          </p>
          <div className="mt-2 flex items-center gap-2">
            <code className="flex-1 px-2 py-1.5 bg-white border rounded text-xs break-all">
              {token.registration_token}
            </code>
            <button
              onClick={handleCopy}
              className="flex items-center gap-1 px-2 py-1.5 text-xs border rounded hover:bg-white"
            >
              <Copy size={12} /> {copied ? 'Copied' : 'Copy'}
            </button>
          </div>
          <pre className="mt-3 p-2 bg-white border rounded text-xs overflow-x-auto whitespace-pre-wrap">{`scm_url: "${token.config_hint.scm_url}"
registration_token: "${token.registration_token}"`}</pre>
        </div>
      )}
    </div>
  );
}
