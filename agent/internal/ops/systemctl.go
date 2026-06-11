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
type systemctlArgs struct {
	Unit string `json:"unit"`
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

func (d *Dispatcher) runSystemctl(ctx context.Context, cmd *proto.CmdPayload, timeout time.Duration) (*proto.ResultPayload, *proto.ErrorPayload) {
	var args systemctlArgs
	if err := json.Unmarshal(cmd.Args, &args); err != nil {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: err.Error()}
	}
	if !validUnit(args.Unit) {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: fmt.Sprintf("invalid unit name %q", args.Unit)}
	}

	verb := strings.TrimPrefix(cmd.Op, "systemctl.")
	systemctlVerb := verb
	switch verb {
	case "is_active":
		systemctlVerb = "is-active"
	case "status", "start", "stop", "restart":
		// already valid as-is
	default:
		return nil, &proto.ErrorPayload{
			Code:    proto.ErrBadArgs,
			Message: fmt.Sprintf("unsupported systemctl verb %q", verb),
		}
	}

	// `systemctl is-active <unit>` returns exit 3 for "inactive" and exit
	// 0 for "active" — that's expected, NOT an error.
	r, err := exec.Run(ctx, timeout, true, "systemctl", systemctlVerb, args.Unit)
	return runResult(r, err)
}
