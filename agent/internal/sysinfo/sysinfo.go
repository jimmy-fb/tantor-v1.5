// Package sysinfo gathers cheap per-host facts (OS family, kernel, IPs,
// disk usage) for inclusion in register + heartbeat frames. Best-effort —
// the agent registers and heartbeats even if any field can't be read.
package sysinfo

import (
	"bufio"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

// OSInfo describes the host OS. Family is one of: rhel, debian, ubuntu,
// alpine, darwin, linux (fallback).
type OSInfo struct {
	Family  string
	Version string
	Kernel  string
}

// Detect reads /etc/os-release + uname to populate the OSInfo.
func Detect() OSInfo {
	info := OSInfo{Family: runtime.GOOS}
	if runtime.GOOS != "linux" {
		info.Kernel = unameRelease()
		return info
	}

	if f, err := os.Open("/etc/os-release"); err == nil {
		defer f.Close()
		fields := map[string]string{}
		s := bufio.NewScanner(f)
		for s.Scan() {
			line := s.Text()
			i := strings.Index(line, "=")
			if i <= 0 {
				continue
			}
			k := line[:i]
			v := strings.Trim(line[i+1:], `"`)
			fields[k] = v
		}
		id := strings.ToLower(fields["ID"])
		info.Family = id
		info.Version = fields["VERSION_ID"]
	}
	info.Kernel = unameRelease()
	return info
}

func unameRelease() string {
	out, err := exec.Command("uname", "-r").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// Hostname returns the host's local hostname or "unknown".
func Hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "unknown"
	}
	return h
}

// LocalIPs returns the host's non-loopback IPv4 + IPv6 addresses.
func LocalIPs() []string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	out := []string{}
	for _, a := range addrs {
		ipn, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		if ipn.IP.IsLoopback() {
			continue
		}
		out = append(out, ipn.IP.String())
	}
	return out
}

// UptimeSec returns the host's uptime in seconds. Implementation lives in
// platform-specific files (sysinfo_linux.go vs sysinfo_other.go) so the
// agent cross-compiles to darwin/amd64 for local dev while still using
// the syscall on the broker host.
