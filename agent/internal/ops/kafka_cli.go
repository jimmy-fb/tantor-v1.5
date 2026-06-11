package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/jimmy-fb/tantor/agent/internal/exec"
	"github.com/jimmy-fb/tantor/agent/internal/proto"
)

// kafkaCLIArgs is the cmd.args body for kafka_cli.* ops.
type kafkaCLIArgs struct {
	Bootstrap  string   `json:"bootstrap"`
	InstallDir string   `json:"install_dir"` // /opt/kafka-<slug>-<id>
	Args       []string `json:"args"`
	// Optional command-line config file (consumer.properties etc.) for
	// SASL/SSL-enabled clusters. Path must be inside the cluster install
	// dir to keep the agent from being a generic file reader.
	CommandConfig string `json:"command_config,omitempty"`
}

// kafkaScript maps the op suffix to the bin/ script under the install dir.
var kafkaScript = map[string]string{
	"topics":          "bin/kafka-topics.sh",
	"configs":         "bin/kafka-configs.sh",
	"acls":            "bin/kafka-acls.sh",
	"consumer_groups": "bin/kafka-consumer-groups.sh",
}

// validBootstrap rejects bootstrap strings that aren't plausible host:port,host:port,... lists.
func validBootstrap(b string) bool {
	if b == "" {
		return false
	}
	if len(b) > 1024 {
		return false
	}
	for _, r := range b {
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
		case '.', '-', '_', ':', ',':
			continue
		}
		return false
	}
	return true
}

// validKafkaArg rejects shell metacharacters that would let the SCM
// pivot into arbitrary command execution via `bash -c` style escape.
// kafka-*.sh args are well-defined and don't need anything beyond
// alnum + dot + dash + underscore + slash + colon + comma + equals.
func validKafkaArg(a string) bool {
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
		case '.', '-', '_', '/', ':', ',', '=', '@', ' ', '+', '*', '?', '[', ']', '{', '}':
			continue
		}
		return false
	}
	return true
}

func (d *Dispatcher) runKafkaCLI(ctx context.Context, cmd *proto.CmdPayload, timeout time.Duration) (*proto.ResultPayload, *proto.ErrorPayload) {
	suffix := strings.TrimPrefix(cmd.Op, "kafka_cli.")
	script, ok := kafkaScript[suffix]
	if !ok {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: fmt.Sprintf("unknown kafka_cli verb %q", suffix)}
	}

	var args kafkaCLIArgs
	if err := json.Unmarshal(cmd.Args, &args); err != nil {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: err.Error()}
	}
	if !validBootstrap(args.Bootstrap) {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: "bootstrap missing or contains forbidden characters"}
	}
	if args.InstallDir == "" {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: "install_dir is required"}
	}
	cleanInstall := filepath.Clean(args.InstallDir)
	if cleanInstall != args.InstallDir || !strings.HasPrefix(cleanInstall, "/opt/kafka") {
		return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: "install_dir must be /opt/kafka*"}
	}
	for _, a := range args.Args {
		if !validKafkaArg(a) {
			return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: fmt.Sprintf("disallowed character in args entry %q", a)}
		}
	}
	if args.CommandConfig != "" {
		cleanCC := filepath.Clean(args.CommandConfig)
		if cleanCC != args.CommandConfig || !strings.HasPrefix(cleanCC, cleanInstall+"/") {
			return nil, &proto.ErrorPayload{Code: proto.ErrBadArgs, Message: "command_config must live under install_dir"}
		}
	}

	argv := []string{filepath.Join(cleanInstall, script), "--bootstrap-server", args.Bootstrap}
	argv = append(argv, args.Args...)
	if args.CommandConfig != "" {
		argv = append(argv, "--command-config", args.CommandConfig)
	}

	// kafka-*.sh runs as the kafka user via sudo. The sudoers profile
	// includes `/opt/kafka-*/bin/*` for the tantor-agent account.
	r, err := exec.Run(ctx, timeout, true, argv...)
	return runResult(r, err)
}
