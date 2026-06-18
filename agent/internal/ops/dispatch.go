package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jimmy-fb/tantor/agent/internal/exec"
	"github.com/jimmy-fb/tantor/agent/internal/proto"
)

// Dispatcher routes incoming cmd frames to the right handler, enforcing
// the allowlist before any command runs. It returns either a ResultPayload
// or an ErrorPayload — the WS client wraps the response in an envelope.
type Dispatcher struct {
	allow        *Allowlist
	defaultTimeo time.Duration
}

// NewDispatcher returns a Dispatcher seeded with the allowlist and a
// default per-command timeout (used when the SCM's cmd doesn't override it).
func NewDispatcher(allow *Allowlist, defaultTimeoutSec int) *Dispatcher {
	if defaultTimeoutSec <= 0 {
		defaultTimeoutSec = 60
	}
	return &Dispatcher{
		allow:        allow,
		defaultTimeo: time.Duration(defaultTimeoutSec) * time.Second,
	}
}

// Features exposes the capability buckets derived from the allowlist. The
// WS client uses this in the register payload.
func (d *Dispatcher) Features() []string {
	return d.allow.Features()
}

// Run executes one cmd payload. The returned values are mutually exclusive
// — exactly one of res, errPayload is non-nil.
func (d *Dispatcher) Run(ctx context.Context, cmd *proto.CmdPayload) (*proto.ResultPayload, *proto.ErrorPayload) {
	// Pull args.path out for the allowlist check (path-scoped ops need it).
	path := extractPath(cmd.Args)

	if !d.allow.Permit(cmd.Op, path) {
		return nil, &proto.ErrorPayload{
			Code:    proto.ErrDenied,
			Message: fmt.Sprintf("op %q is not in the agent's local allowlist", cmd.Op),
		}
	}

	timeout := d.defaultTimeo
	if cmd.TimeoutSec > 0 {
		timeout = time.Duration(cmd.TimeoutSec) * time.Second
	}

	switch {
	case strings.HasPrefix(cmd.Op, "systemctl."):
		return d.runSystemctl(ctx, cmd, timeout)
	case strings.HasPrefix(cmd.Op, "journalctl."):
		return d.runJournalctl(ctx, cmd, timeout)
	case strings.HasPrefix(cmd.Op, "kafka_cli."):
		return d.runKafkaCLI(ctx, cmd, timeout)
	case cmd.Op == "file.download":
		return d.runDownload(ctx, cmd, timeout)
	case strings.HasPrefix(cmd.Op, "file."):
		return d.runFile(ctx, cmd, timeout)
	case cmd.Op == "exec.script":
		return d.runScript(ctx, cmd, timeout)
	case strings.HasPrefix(cmd.Op, "exec."):
		return d.runExec(ctx, cmd, timeout)
	default:
		return nil, &proto.ErrorPayload{
			Code:    proto.ErrBadArgs,
			Message: fmt.Sprintf("unknown op %q", cmd.Op),
		}
	}
}

// extractPath pulls "path" out of the raw JSON args, if present. Used by
// the allowlist check for file.read / file.write entries.
func extractPath(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var probe struct {
		Path string `json:"path"`
	}
	_ = json.Unmarshal(raw, &probe)
	return probe.Path
}

// runResult is a tiny helper that converts an exec.Result + execErr into
// a ResultPayload or ErrorPayload pair.
func runResult(r *exec.Result, execErr error) (*proto.ResultPayload, *proto.ErrorPayload) {
	if execErr != nil && errors.Is(execErr, exec.ErrTimeout) {
		return nil, &proto.ErrorPayload{
			Code:    proto.ErrTimeout,
			Message: "command exceeded its timeout",
		}
	}
	if execErr != nil && r == nil {
		return nil, &proto.ErrorPayload{
			Code:    proto.ErrExecFailed,
			Message: execErr.Error(),
		}
	}
	// Non-zero exit is NOT an exec failure — it's a normal Result. The
	// SCM gets to decide how to interpret it (e.g. systemctl is-active
	// returns exit=3 for inactive units, which is expected).
	return &proto.ResultPayload{
		ExitCode:   r.ExitCode,
		Stdout:     r.Stdout,
		Stderr:     r.Stderr,
		DurationMs: r.DurationMs,
	}, nil
}
