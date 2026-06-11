//go:build !linux

package sysinfo

// UptimeSec — no portable kernel uptime on darwin/freebsd. Returns 0;
// the WS client falls back to the agent's own uptime counter.
func UptimeSec() int64 { return 0 }
