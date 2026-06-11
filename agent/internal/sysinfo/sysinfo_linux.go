//go:build linux

package sysinfo

import "syscall"

// UptimeSec reads kernel uptime via the sysinfo(2) syscall.
func UptimeSec() int64 {
	var info syscall.Sysinfo_t
	if err := syscall.Sysinfo(&info); err != nil {
		return 0
	}
	return int64(info.Uptime)
}
