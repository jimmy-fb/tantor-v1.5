// Package ops routes incoming cmd frames to handlers, enforcing the local
// allowlist before any command is executed.
package ops

import (
	"path/filepath"
	"strings"
)

// Allowlist is the per-agent operation allowlist loaded from the YAML
// config. Entries are either bare op strings ("systemctl.start") or
// path-scoped ("file.read:/etc/kafka/" — glob matches args.path).
type Allowlist struct {
	bare       map[string]struct{}
	pathScoped map[string][]string // op -> list of glob patterns
}

// NewAllowlist parses the YAML allowed_operations list into a lookup table.
func NewAllowlist(entries []string) *Allowlist {
	al := &Allowlist{
		bare:       make(map[string]struct{}),
		pathScoped: make(map[string][]string),
	}
	for _, raw := range entries {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if i := strings.Index(raw, ":"); i > 0 {
			op := raw[:i]
			pattern := raw[i+1:]
			al.pathScoped[op] = append(al.pathScoped[op], pattern)
			continue
		}
		al.bare[raw] = struct{}{}
	}
	return al
}

// Permit reports whether the given op is allowed, optionally against an
// args.path value. For bare ops, path is ignored. For path-scoped ops, the
// agent matches path against each glob; any match grants permission.
func (a *Allowlist) Permit(op, path string) bool {
	if _, ok := a.bare[op]; ok {
		return true
	}
	patterns, ok := a.pathScoped[op]
	if !ok {
		return false
	}
	if path == "" {
		return false
	}
	for _, pat := range patterns {
		// If the pattern ends with "/", it's a prefix match (any file
		// under that directory). Otherwise glob match.
		if strings.HasSuffix(pat, "/") {
			if strings.HasPrefix(path, pat) {
				return true
			}
			continue
		}
		ok, err := filepath.Match(pat, path)
		if err == nil && ok {
			return true
		}
	}
	return false
}

// Features returns the operator-facing capability list derived from the
// allowlist. Used in the register payload so the SCM knows what to dispatch.
func (a *Allowlist) Features() []string {
	seen := map[string]struct{}{}
	for op := range a.bare {
		seen[featureFor(op)] = struct{}{}
	}
	for op := range a.pathScoped {
		seen[featureFor(op)] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for f := range seen {
		out = append(out, f)
	}
	return out
}

// featureFor groups individual ops into capability buckets:
//
//	systemctl.* -> "systemctl"
//	journalctl.* -> "journalctl"
//	kafka_cli.* -> "kafka_cli"
//	file.read* -> "file_read"
//	file.write* -> "file_write"
//	exec.* -> the bare op (operators may want fine-grained gating here)
func featureFor(op string) string {
	switch {
	case strings.HasPrefix(op, "systemctl."):
		return "systemctl"
	case strings.HasPrefix(op, "journalctl."):
		return "journalctl"
	case strings.HasPrefix(op, "kafka_cli."):
		return "kafka_cli"
	case op == "file.read" || strings.HasPrefix(op, "file.read"):
		return "file_read"
	case op == "file.write" || strings.HasPrefix(op, "file.write"):
		return "file_write"
	default:
		return op
	}
}
