// tantor-agent — host-side agent for Tantor. Connects OUT to the Tantor
// management server over WSS, registers, then executes the allowlisted
// operations the SCM dispatches. See docs/AGENT_PROTOCOL.md for the wire
// protocol.
//
// The agent is OPTIONAL. If it isn't running, Tantor falls back to SSH +
// CLI for the same operations.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jimmy-fb/tantor/agent/internal/config"
	"github.com/jimmy-fb/tantor/agent/internal/ops"
	"github.com/jimmy-fb/tantor/agent/internal/ws"
)

func main() {
	configPath := flag.String("config", "/etc/tantor-agent/config.yaml",
		"path to the agent YAML config")
	versionFlag := flag.Bool("version", false, "print the agent version and exit")
	flag.Parse()

	if *versionFlag {
		log.Printf("tantor-agent %s", ws.Version)
		return
	}

	logger := log.New(os.Stderr, "tantor-agent: ", log.LstdFlags|log.LUTC)

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Fatalf("config: %v", err)
	}

	allow := ops.NewAllowlist(cfg.AllowedOperations)
	dispatcher := ops.NewDispatcher(allow, 60)

	client := ws.NewClient(cfg, dispatcher, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT/SIGTERM cleanly so the systemd unit can stop the
	// agent without leaving sockets in CLOSE_WAIT.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sig
		logger.Printf("received %s; shutting down", s)
		cancel()
	}()

	logger.Printf("tantor-agent %s starting (config=%s, scm=%s)",
		ws.Version, *configPath, cfg.SCMUrl)
	client.Run(ctx)
	logger.Printf("tantor-agent exited cleanly")
}
