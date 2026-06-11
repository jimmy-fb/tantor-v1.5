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

type journalReadArgs struct {
	Unit     string `json:"unit"`
	Lines    int    `json:"lines"`
	Since    string `json:"since"`    // "2 hours ago", "2026-06-11 08:00:00"
	Priority string `json:"priority"` // emerg, alert, crit, err, warning, notice, info, debug
	Grep     string `json:"grep"`     // PCRE-flavored grep, journalctl passes as -g
}

// validSince accepts a conservative subset of journalctl's --since values:
// "Nm ago" / "Nh ago" / "Nd ago" / ISO-8601-ish timestamps. Anything else is
// rejected to keep shell injection off the table.
func validSince(s string) bool {
	if s == "" {
		return true
	}
	if len(s) > 64 {
		return false
	}
	for _, r := range s {
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
		case '-', '_', ':', ' ', '.':
			continue
		}
		return false
	}
	return true
}

func (d *Dispatcher) runJournalctl(ctx context.Context, cmd *proto.CmdPayload, timeout time.Duration) (*proto.ResultPayload, *proto.ErrorPayload) {
	verb := strings.TrimPrefix(cmd.Op, "journalctl.")
	if verb == "tail" {
		// Streaming variant is not yet implemented — falls back to a
		// bounded read. The WS dispatcher will be extended to emit
		// event frames when this is wired up.
		return nil, &proto.ErrorPayload{
			Code:    proto.ErrBadArgs,
			Message: "journalctl.tail (streaming) is not yet implemented; use journalctl.read",
		}
	}
	if verb != "read" {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: fmt.Sprintf("unsupported journalctl verb %q", verb)}
	}

	var args journalReadArgs
	if err := json.Unmarshal(cmd.Args, &args); err != nil {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: err.Error()}
	}
	if !validUnit(args.Unit) {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: fmt.Sprintf("invalid unit name %q", args.Unit)}
	}
	if args.Lines <= 0 || args.Lines > 10000 {
		args.Lines = 200
	}
	if !validSince(args.Since) {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: "invalid since clause"}
	}

	argv := []string{
		"journalctl",
		"-u", args.Unit,
		"--no-pager",
		"-n", fmt.Sprintf("%d", args.Lines),
	}
	if args.Since != "" {
		argv = append(argv, "--since", args.Since)
	}
	if args.Priority != "" {
		if !validUnit(args.Priority) { // simple chars only
			return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: "invalid priority"}
		}
		argv = append(argv, "-p", args.Priority)
	}
	// Grep is intentionally NOT supported in the MVP — handing arbitrary
	// regex to journalctl would widen the attack surface for an agent
	// without obvious operator value. The SCM filters in Python instead.

	r, err := exec.Run(ctx, timeout, true, argv...)
	return runResult(r, err)
}
