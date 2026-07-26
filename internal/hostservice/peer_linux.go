//go:build linux

package hostservice

import (
	"golang.org/x/sys/unix"
	"net"
)

func peerUID(connection *net.UnixConn) (int, error) {
	raw, err := connection.SyscallConn()
	if err != nil {
		return -1, err
	}
	uid := -1
	var socketErr error
	err = raw.Control(func(fd uintptr) {
		credential, getErr := unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if getErr != nil {
			socketErr = getErr
			return
		}
		uid = int(credential.Uid)
	})
	if err != nil {
		return -1, err
	}
	return uid, socketErr
}
