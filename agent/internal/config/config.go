// Package config loads the agent's YAML config from /etc/tantor-agent/config.yaml
// (or wherever -config points). The config carries the SCM endpoint, the
// registration token (first-boot only), the local allowlist of operations,
// and TLS settings.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the on-disk shape. All fields are optional except SCMUrl and
// AllowedOperations.
type Config struct {
	// SCMUrl is the Tantor server's WebSocket endpoint. Must be wss:// in
	// production; ws:// is accepted for local dev only.
	SCMUrl string `yaml:"scm_url"`

	// RegistrationToken is the one-shot token an admin mints from the
	// Tantor UI. After the first successful registration the agent
	// persists a long-lived JWT in StateDir and ignores this field.
	RegistrationToken string `yaml:"registration_token"`

	// AllowedOperations is the local allowlist of op strings the agent
	// will accept from the SCM. The SCM ALSO enforces an allowlist server
	// side; the agent's list wins if they disagree (defense in depth).
	//
	// Path-scoped ops use the form "<op>:<glob>", e.g.
	//   file.read:/etc/kafka/
	//   file.write:/opt/kafka-*/config/server.properties
	AllowedOperations []string `yaml:"allowed_operations"`

	// TLSVerify toggles validation of the SCM's TLS certificate chain.
	// Default true. Set false ONLY in dev/lab with a self-signed SCM.
	TLSVerify *bool `yaml:"tls_verify,omitempty"`

	// CABundlePath pins a specific CA bundle (PEM) for the SCM's TLS
	// cert. Empty = system trust store.
	CABundlePath string `yaml:"ca_bundle_path,omitempty"`

	// HeartbeatIntervalSec overrides the SCM-supplied heartbeat cadence.
	// Default 15s.
	HeartbeatIntervalSec int `yaml:"heartbeat_interval_sec,omitempty"`

	// StateDir is where the persisted agent JWT lives. Default
	// /var/lib/tantor-agent. Mode 0700 owner tantor-agent.
	StateDir string `yaml:"state_dir,omitempty"`

	// AgentFeatures is the operator-controlled capability list reported
	// to the SCM during registration. The SCM uses this to skip
	// dispatching ops the operator hasn't enabled. Optional — if empty
	// the agent derives the list from AllowedOperations.
	AgentFeatures []string `yaml:"agent_features,omitempty"`
}

// TLSVerifyEnabled returns true when the config either omits tls_verify
// (default) or sets it to true.
func (c *Config) TLSVerifyEnabled() bool {
	return c.TLSVerify == nil || *c.TLSVerify
}

// HeartbeatInterval returns the heartbeat cadence in seconds with the
// default applied.
func (c *Config) HeartbeatInterval() int {
	if c.HeartbeatIntervalSec > 0 {
		return c.HeartbeatIntervalSec
	}
	return 15
}

// StateDirOrDefault returns StateDir or the production default.
func (c *Config) StateDirOrDefault() string {
	if c.StateDir != "" {
		return c.StateDir
	}
	return "/var/lib/tantor-agent"
}

// Load reads the YAML config from disk, applies defaults, and validates.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.SCMUrl == "" {
		return errors.New("scm_url is required")
	}
	u, err := url.Parse(c.SCMUrl)
	if err != nil {
		return fmt.Errorf("scm_url invalid: %w", err)
	}
	if u.Scheme != "wss" && u.Scheme != "ws" {
		return fmt.Errorf("scm_url must use wss:// (or ws:// for dev), got %s", u.Scheme)
	}
	if len(c.AllowedOperations) == 0 {
		return errors.New("allowed_operations is required (non-empty list)")
	}
	for _, op := range c.AllowedOperations {
		op = strings.TrimSpace(op)
		if op == "" {
			return errors.New("allowed_operations contains an empty entry")
		}
	}
	return nil
}
