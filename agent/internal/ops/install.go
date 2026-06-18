// Install-time operations: download tarballs, extract them, run installed
// scripts. Used by the agent-based deployer path so initial cluster
// installation never needs SSH.
package ops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jimmy-fb/tantor/agent/internal/exec"
	"github.com/jimmy-fb/tantor/agent/internal/proto"
)

// downloadArgs is the cmd.args body for file.download.
type downloadArgs struct {
	URL    string `json:"url"`
	Dest   string `json:"dest"`
	SHA256 string `json:"sha256,omitempty"`
	Mode   int    `json:"mode,omitempty"`
}

// runDownload fetches URL and saves to Dest. The dest path must live under
// /opt/kafka-*, /opt/tantor-stage/, or /tmp/tantor-agent-download-*. SHA256
// (if supplied) is verified before the file is moved into place.
//
// Network: the agent host needs outbound HTTPS to the SCM. That's the same
// connection the agent's WebSocket already uses — no new firewall holes
// required.
func (d *Dispatcher) runDownload(ctx context.Context, cmd *proto.CmdPayload, timeout time.Duration) (*proto.ResultPayload, *proto.ErrorPayload) {
	var args downloadArgs
	if err := json.Unmarshal(cmd.Args, &args); err != nil {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: err.Error()}
	}
	if args.URL == "" {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: "url is required"}
	}
	if !strings.HasPrefix(args.URL, "http://") && !strings.HasPrefix(args.URL, "https://") {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: "url must be http(s)://"}
	}
	dest := filepath.Clean(args.Dest)
	if dest != args.Dest || strings.Contains(dest, "..") {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: "dest must be absolute and free of '..'"}
	}
	// Restrict where downloads can land. The sudoers profile + agent
	// allowlist mean the agent never gets to write outside these prefixes
	// even if the SCM is compromised.
	if !strings.HasPrefix(dest, "/opt/kafka") &&
		!strings.HasPrefix(dest, "/opt/tantor-stage/") &&
		!strings.HasPrefix(dest, "/tmp/tantor-agent-download-") {
		return nil, &proto.ErrorPayload{
			Code:    proto.ErrBadArgs,
			Message: "dest must live under /opt/kafka*, /opt/tantor-stage/, or /tmp/tantor-agent-download-*",
		}
	}

	start := time.Now()
	httpCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(httpCtx, "GET", args.URL, nil)
	if err != nil {
		return nil, &proto.ErrorPayload{Code: proto.ErrExecFailed, Message: err.Error()}
	}
	req.Header.Set("User-Agent", "tantor-agent/1.5")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, &proto.ErrorPayload{Code: proto.ErrExecFailed, Message: fmt.Sprintf("http get: %v", err)}
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, &proto.ErrorPayload{
			Code:    proto.ErrExecFailed,
			Message: fmt.Sprintf("http get %s: %d", args.URL, resp.StatusCode),
		}
	}

	// Write to a temp file in the same dir so the final rename is atomic
	// and on the same filesystem.
	tmp, err := os.CreateTemp(filepath.Dir(dest), ".tantor-dl-*")
	if err != nil {
		// /opt/kafka-* may not exist yet — fall back to /tmp.
		tmp, err = os.CreateTemp("", "tantor-dl-*")
		if err != nil {
			return nil, &proto.ErrorPayload{Code: proto.ErrExecFailed, Message: err.Error()}
		}
	}
	defer func() {
		_ = os.Remove(tmp.Name())
	}()

	hasher := sha256.New()
	tee := io.MultiWriter(tmp, hasher)
	n, err := io.Copy(tee, resp.Body)
	if err != nil {
		_ = tmp.Close()
		return nil, &proto.ErrorPayload{Code: proto.ErrExecFailed, Message: fmt.Sprintf("download body: %v", err)}
	}
	if err := tmp.Close(); err != nil {
		return nil, &proto.ErrorPayload{Code: proto.ErrExecFailed, Message: err.Error()}
	}
	gotSha := hex.EncodeToString(hasher.Sum(nil))
	if args.SHA256 != "" && !strings.EqualFold(gotSha, args.SHA256) {
		return nil, &proto.ErrorPayload{
			Code:    proto.ErrExecFailed,
			Message: fmt.Sprintf("sha256 mismatch: expected %s got %s", args.SHA256, gotSha),
		}
	}

	// Atomic install into the final path. We use `sudo install` so we can
	// land under /opt/kafka-* with the right mode/ownership.
	mode := args.Mode
	if mode == 0 {
		mode = 0o644
	}
	r, err := exec.Run(ctx, timeout, true, "install", "-m", fmt.Sprintf("%o", mode), tmp.Name(), dest)
	res, errP := runResult(r, err)
	if errP != nil {
		return nil, errP
	}
	// Attach a small JSON line to stdout so the SCM can confirm the hash.
	res.Stdout = fmt.Sprintf("{\"bytes\":%d,\"sha256\":%q,\"duration_ms\":%d}\n",
		n, gotSha, time.Since(start).Milliseconds()) + res.Stdout
	return res, nil
}

// scriptArgs is the cmd.args body for exec.script.
type scriptArgs struct {
	// Script is the name of a script in the agent's scripts dir
	// (/usr/local/lib/tantor-agent/scripts/). The agent NEVER executes
	// arbitrary scripts from the SCM — only ones pre-installed alongside
	// the binary.
	Script string `json:"script"`
	// Args are passed positionally. Each one is validated against
	// validScriptArg (path/identifier characters only — no shell
	// metacharacters).
	Args []string `json:"args,omitempty"`
}

const scriptsDir = "/usr/local/lib/tantor-agent/scripts"

// validScriptArg rejects shell metacharacters. The scripts shipped with
// the agent expect simple positional args (install dirs, version numbers,
// usernames) and never need shell-escape characters.
func validScriptArg(a string) bool {
	if len(a) > 4096 {
		return false
	}
	for _, r := range a {
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
		case '.', '-', '_', '/', ':', ',', '=', '@', '+', '~':
			continue
		}
		return false
	}
	return true
}

// validScriptName limits which file under scriptsDir may run. Conservative:
// just letters, digits, dashes; .sh extension required.
func validScriptName(s string) bool {
	if s == "" {
		return false
	}
	if !strings.HasSuffix(s, ".sh") {
		return false
	}
	if strings.ContainsAny(s, "/\\") {
		return false
	}
	for _, r := range s[:len(s)-3] {
		if r >= 'a' && r <= 'z' {
			continue
		}
		if r >= 'A' && r <= 'Z' {
			continue
		}
		if r >= '0' && r <= '9' {
			continue
		}
		if r == '-' || r == '_' {
			continue
		}
		return false
	}
	return true
}

func (d *Dispatcher) runScript(ctx context.Context, cmd *proto.CmdPayload, timeout time.Duration) (*proto.ResultPayload, *proto.ErrorPayload) {
	var args scriptArgs
	if err := json.Unmarshal(cmd.Args, &args); err != nil {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: err.Error()}
	}
	if !validScriptName(args.Script) {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: fmt.Sprintf("invalid script name %q", args.Script)}
	}
	path := filepath.Join(scriptsDir, args.Script)
	if _, err := os.Stat(path); err != nil {
		return nil, &proto.ErrorPayload{
			Code:    proto.ErrBadArgs,
			Message: fmt.Sprintf("script %s not found on this host (install agent scripts package)", args.Script),
		}
	}
	for _, a := range args.Args {
		if !validScriptArg(a) {
			return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: fmt.Sprintf("disallowed character in arg %q", a)}
		}
	}
	argv := append([]string{path}, args.Args...)
	// Scripts run via sudo so they can chown, useradd, install systemd
	// units, etc. Sudoers profile pins this to /usr/local/lib/tantor-agent/scripts/*.sh
	r, err := exec.Run(ctx, timeout, true, argv...)
	return runResult(r, err)
}
