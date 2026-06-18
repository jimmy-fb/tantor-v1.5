package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jimmy-fb/tantor/agent/internal/exec"
	"github.com/jimmy-fb/tantor/agent/internal/proto"
)

// systemctlArgs maps the cmd.args JSON for any systemctl.* op.
//
// daemon_reload + reset_failed_all are global; all other verbs require Unit.
// The `Mode` field is only used by `kill` (--kill-who=all / main / control).
type systemctlArgs struct {
	Unit string `json:"unit"`
	Mode string `json:"mode,omitempty"`
}

// validUnit guards against shell-metacharacter injection in the unit name.
// systemd unit names are conservative: alnum + "-_.@\\".
func validUnit(name string) bool {
	if name == "" {
		return false
	}
	if len(name) > 255 {
		return false
	}
	for _, r := range name {
		if r >= 'a' && r <= 'z' {
			continue
		}
		if r >= 'A' && r <= 'Z' {
			continue
		}
		if r >= '0' && r <= '9' {
			continue
		}
		switch r {
		case '-', '_', '.', '@', '\\':
			continue
		}
		return false
	}
	return true
}

// validKillMode constrains the --kill-who argument to the three valid values.
func validKillMode(m string) bool {
	switch m {
	case "", "main", "control", "all":
		return true
	}
	return false
}

func (d *Dispatcher) runSystemctl(ctx context.Context, cmd *proto.CmdPayload, timeout time.Duration) (*proto.ResultPayload, *proto.ErrorPayload) {
	var args systemctlArgs
	if len(cmd.Args) > 0 {
		if err := json.Unmarshal(cmd.Args, &args); err != nil {
			return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: err.Error()}
		}
	}

	verb := strings.TrimPrefix(cmd.Op, "systemctl.")

	// Verbs that take NO unit argument.
	switch verb {
	case "daemon_reload":
		r, err := exec.Run(ctx, timeout, true, "systemctl", "daemon-reload")
		return runResult(r, err)
	case "reset_failed_all":
		r, err := exec.Run(ctx, timeout, true, "systemctl", "reset-failed")
		return runResult(r, err)
	}

	// All remaining verbs require a unit.
	if !validUnit(args.Unit) {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: fmt.Sprintf("invalid unit name %q", args.Unit)}
	}

	var argv []string
	switch verb {
	case "is_active":
		argv = []string{"systemctl", "is-active", args.Unit}
	case "status":
		argv = []string{"systemctl", "status", "--no-pager", args.Unit}
	case "cat":
		argv = []string{"systemctl", "cat", args.Unit}
	case "start", "stop", "restart", "enable", "disable":
		argv = []string{"systemctl", verb, args.Unit}
	case "reset_failed":
		argv = []string{"systemctl", "reset-failed", args.Unit}
	case "kill":
		if !validKillMode(args.Mode) {
			return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: "kill mode must be one of: main, control, all"}
		}
		mode := args.Mode
		if mode == "" {
			mode = "all"
		}
		argv = []string{"systemctl", "kill", "--kill-who=" + mode, args.Unit}
	default:
		return nil, &proto.ErrorPayload{
			Code:    proto.ErrBadArgs,
			Message: fmt.Sprintf("unsupported systemctl verb %q", verb),
		}
	}

	// Most systemctl results that the SCM cares about are non-zero in
	// legitimate cases (is-active -> 3 for inactive). The dispatcher
	// returns the exit code in the result frame and lets the SCM
	// interpret it; only an exec error (process couldn't start) is an
	// agent-level error.
	r, err := exec.Run(ctx, timeout, true, argv...)
	return runResult(r, err)
}
