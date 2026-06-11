// Package exec wraps os/exec with the conventions the agent needs: a
// per-command timeout, captured stdout/stderr, and "use sudo -n only when
// running as a non-root user" semantics.
package exec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	osexec "os/exec"
	"time"
)

// Result is the outcome of a single command.
type Result struct {
	ExitCode   int
	Stdout     string
	Stderr     string
	DurationMs int64
}

// ErrTimeout is returned when the per-command deadline is reached.
var ErrTimeout = errors.New("command timeout")

// Run executes argv with the given timeout. argv[0] is the program; the
// rest are its arguments — no shell, no expansion, no interpolation.
//
// If sudo is true AND the agent isn't already root, argv is prefixed with
// `sudo -n` (non-interactive). The sudoers profile installed alongside the
// agent grants passwordless sudo only on the explicit allowlist of
// systemctl / journalctl / cat / install commands.
func Run(ctx context.Context, timeout time.Duration, sudo bool, argv ...string) (*Result, error) {
	if len(argv) == 0 {
		return nil, errors.New("empty argv")
	}

	if sudo && os.Geteuid() != 0 {
		argv = append([]string{"sudo", "-n"}, argv...)
	}

	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	cmd := osexec.CommandContext(cctx, argv[0], argv[1:]...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()
	dur := time.Since(start).Milliseconds()

	exit := 0
	if runErr != nil {
		var exitErr *osexec.ExitError
		if errors.As(runErr, &exitErr) {
			exit = exitErr.ExitCode()
		} else if cctx.Err() == context.DeadlineExceeded {
			return &Result{
				ExitCode:   -1,
				Stdout:     stdout.String(),
				Stderr:     stderr.String() + fmt.Sprintf("\n(killed after %dms — timeout)\n", timeout.Milliseconds()),
				DurationMs: dur,
			}, ErrTimeout
		} else {
			// exec failed before producing a process (e.g. ENOENT)
			return &Result{
				ExitCode:   -1,
				Stderr:     runErr.Error(),
				DurationMs: dur,
			}, runErr
		}
	}

	return &Result{
		ExitCode:   exit,
		Stdout:     stdout.String(),
		Stderr:     stderr.String(),
		DurationMs: dur,
	}, nil
}
