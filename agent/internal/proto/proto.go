// Package proto defines the on-wire JSON envelopes the agent exchanges with
// the SCM. See docs/AGENT_PROTOCOL.md for the full spec.
package proto

import "encoding/json"

// ProtocolVersion is the wire format version this agent speaks.
const ProtocolVersion = 1

// Frame kinds.
const (
	KindRegister    = "register"
	KindRegisterAck = "register_ack"
	KindHeartbeat   = "heartbeat"
	KindCmd         = "cmd"
	KindResult      = "result"
	KindEvent       = "event"
	KindError       = "error"
)

// Error codes used in error payloads.
const (
	ErrAuthFailed      = "auth_failed"
	ErrHostNotFound    = "host_not_found"
	ErrVersionMismatch = "version_mismatch"
	ErrDenied          = "denied"
	ErrTimeout         = "timeout"
	ErrExecFailed      = "exec_failed"
	ErrBadArgs         = "bad_args"
)

// Envelope is every frame on the WebSocket. Payload is left as raw JSON so
// the dispatcher can decode it into a kind-specific struct.
type Envelope struct {
	V       int             `json:"v"`
	Kind    string          `json:"kind"`
	ID      string          `json:"id,omitempty"`
	Ref     string          `json:"ref,omitempty"`
	Ts      string          `json:"ts,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// RegisterPayload — Agent → SCM, first frame after WS upgrade.
type RegisterPayload struct {
	Hostname      string   `json:"hostname"`
	IPAddresses   []string `json:"ip_addresses"`
	OS            OSInfo   `json:"os"`
	AgentVersion  string   `json:"agent_version"`
	AgentFeatures []string `json:"agent_features"`
	HostIDHint    string   `json:"host_id_hint,omitempty"`
}

// OSInfo describes the host's operating system. Populated best-effort.
type OSInfo struct {
	Family  string `json:"family"`            // rhel, debian, ubuntu, alpine, darwin, ...
	Version string `json:"version,omitempty"` // distro release
	Kernel  string `json:"kernel,omitempty"`  // uname -r
}

// RegisterAckPayload — SCM → Agent, in response to a successful Register.
type RegisterAckPayload struct {
	AgentID                  string   `json:"agent_id"`
	HostID                   string   `json:"host_id"`
	AgentJWT                 string   `json:"agent_jwt"`
	HeartbeatIntervalSec     int      `json:"heartbeat_interval_sec"`
	CommandTimeoutDefaultSec int      `json:"command_timeout_default_sec"`
	AllowedOperations        []string `json:"allowed_operations"`
}

// HeartbeatPayload — Agent → SCM, every HeartbeatIntervalSec.
type HeartbeatPayload struct {
	UptimeSec    int64              `json:"uptime_sec"`
	KafkaUnits   []KafkaUnitStatus  `json:"kafka_units,omitempty"`
	Load1        float64            `json:"load1,omitempty"`
	MemUsedPct   float64            `json:"mem_used_pct,omitempty"`
	DiskUsedPct  map[string]float64 `json:"disk_used_pct,omitempty"`
}

// KafkaUnitStatus is one row in the heartbeat's kafka_units list — the cheap
// systemctl-level info the SCM uses to keep the dashboard fresh.
type KafkaUnitStatus struct {
	Unit        string `json:"unit"`
	ActiveState string `json:"active_state"`
	SubState    string `json:"sub_state"`
	MainPID     int    `json:"main_pid,omitempty"`
	MemoryBytes int64  `json:"memory_bytes,omitempty"`
}

// CmdPayload — SCM → Agent, an operation to execute.
type CmdPayload struct {
	Op         string          `json:"op"`
	Args       json.RawMessage `json:"args,omitempty"`
	TimeoutSec int             `json:"timeout_sec,omitempty"`
}

// ResultPayload — Agent → SCM, completion of a Cmd.
type ResultPayload struct {
	ExitCode   int    `json:"exit_code"`
	Stdout     string `json:"stdout,omitempty"`
	Stderr     string `json:"stderr,omitempty"`
	Encoding   string `json:"encoding,omitempty"` // "base64" for binary stdout
	DurationMs int64  `json:"duration_ms"`
}

// EventPayload — Agent → SCM, mid-execution streaming output.
type EventPayload struct {
	Stream string `json:"stream"` // "stdout" | "stderr"
	Data   string `json:"data"`
}

// ErrorPayload — either side, used when something goes wrong.
type ErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
