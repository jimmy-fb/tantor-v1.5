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

type ssArgs struct {
	Ports []int `json:"ports,omitempty"`
}

type cglsArgs struct {
	Unit string `json:"unit"`
}

func (d *Dispatcher) runExec(ctx context.Context, cmd *proto.CmdPayload, timeout time.Duration) (*proto.ResultPayload, *proto.ErrorPayload) {
	suffix := strings.TrimPrefix(cmd.Op, "exec.")
	switch suffix {
	case "ss":
		var args ssArgs
		if len(cmd.Args) > 0 {
			if err := json.Unmarshal(cmd.Args, &args); err != nil {
				return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: err.Error()}
			}
		}
		argv := []string{"ss", "-tnlp"}
		if len(args.Ports) > 0 {
			// `ss -tnlp '( sport = :9092 or sport = :9094 )'` — we
			// don't render that filter here; instead the SCM
			// filters in Python after parsing. Simpler.
		}
		r, err := exec.Run(ctx, timeout, true, argv...)
		return runResult(r, err)

	case "systemd-cgls":
		var args cglsArgs
		if err := json.Unmarshal(cmd.Args, &args); err != nil {
			return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: err.Error()}
		}
		if !validUnit(args.Unit) {
			return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: "invalid unit name"}
		}
		r, err := exec.Run(ctx, timeout, true, "systemd-cgls", "--unit", args.Unit, "--no-pager")
		return runResult(r, err)

	default:
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: fmt.Sprintf("unsupported exec verb %q", suffix)}
	}
}
