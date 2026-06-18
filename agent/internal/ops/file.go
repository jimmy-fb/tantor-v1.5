package ops

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jimmy-fb/tantor/agent/internal/exec"
	"github.com/jimmy-fb/tantor/agent/internal/proto"
)

type fileReadArgs struct {
	Path string `json:"path"`
}

type fileWriteArgs struct {
	Path    string `json:"path"`
	Content string `json:"content"`
	Mode    int    `json:"mode"`     // 0644 by default
	Owner   string `json:"owner"`    // optional, requires sudo
	Sudo    bool   `json:"use_sudo"` // when true, `sudo install -m ...`
}

type fileDeleteArgs struct {
	Path string `json:"path"`
}

func (d *Dispatcher) runFile(ctx context.Context, cmd *proto.CmdPayload, timeout time.Duration) (*proto.ResultPayload, *proto.ErrorPayload) {
	switch cmd.Op {
	case "file.read":
		return d.runFileRead(ctx, cmd, timeout)
	case "file.write":
		return d.runFileWrite(ctx, cmd, timeout)
	case "file.delete":
		return d.runFileDelete(ctx, cmd, timeout)
	default:
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: fmt.Sprintf("unsupported file op %q", cmd.Op)}
	}
}

func (d *Dispatcher) runFileDelete(ctx context.Context, cmd *proto.CmdPayload, timeout time.Duration) (*proto.ResultPayload, *proto.ErrorPayload) {
	var args fileDeleteArgs
	if err := json.Unmarshal(cmd.Args, &args); err != nil {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: err.Error()}
	}
	clean := filepath.Clean(args.Path)
	if clean != args.Path || strings.Contains(clean, "..") {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: "path must be absolute and free of '..'"}
	}
	// `rm` is locked down via sudoers — only specific unit-file globs are
	// allowed (see installer/sudoers.d/tantor-agent). Other paths refuse.
	r, err := exec.Run(ctx, timeout, true, "rm", "-f", clean)
	return runResult(r, err)
}

func (d *Dispatcher) runFileRead(ctx context.Context, cmd *proto.CmdPayload, timeout time.Duration) (*proto.ResultPayload, *proto.ErrorPayload) {
	var args fileReadArgs
	if err := json.Unmarshal(cmd.Args, &args); err != nil {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: err.Error()}
	}
	clean := filepath.Clean(args.Path)
	if clean != args.Path || strings.Contains(clean, "..") {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: "path must be absolute and free of '..'"}
	}

	// Read via `sudo -n cat` so we can pick up files owned by the kafka
	// user (server.properties etc.) without making the agent root.
	r, err := exec.Run(ctx, timeout, true, "cat", clean)
	res, errP := runResult(r, err)
	if errP != nil {
		return nil, errP
	}
	// If the body isn't valid utf-8, re-encode as base64 so JSON survives.
	if !utf8.ValidString(res.Stdout) {
		res.Stdout = base64.StdEncoding.EncodeToString([]byte(res.Stdout))
		res.Encoding = "base64"
	}
	return res, nil
}

func (d *Dispatcher) runFileWrite(ctx context.Context, cmd *proto.CmdPayload, timeout time.Duration) (*proto.ResultPayload, *proto.ErrorPayload) {
	var args fileWriteArgs
	if err := json.Unmarshal(cmd.Args, &args); err != nil {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: err.Error()}
	}
	clean := filepath.Clean(args.Path)
	if clean != args.Path || strings.Contains(clean, "..") {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: "path must be absolute and free of '..'"}
	}
	mode := args.Mode
	if mode == 0 {
		mode = 0o644
	}

	// Write to a temp file inside the agent's state dir, then `sudo
	// install -m <mode> tmp target` to atomically place it. This relies
	// on /usr/bin/install being on the sudoers allowlist.
	tmp, err := os.CreateTemp("", "tantor-agent-write-*")
	if err != nil {
		return nil, &proto.ErrorPayload{Code: proto.ErrExecFailed, Message: err.Error()}
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	if _, err := tmp.WriteString(args.Content); err != nil {
		return nil, &proto.ErrorPayload{Code: proto.ErrExecFailed, Message: err.Error()}
	}
	if err := tmp.Close(); err != nil {
		return nil, &proto.ErrorPayload{Code: proto.ErrExecFailed, Message: err.Error()}
	}

	r, err := exec.Run(ctx, timeout, true, "install", "-m", fmt.Sprintf("%o", mode), tmp.Name(), clean)
	return runResult(r, err)
}
