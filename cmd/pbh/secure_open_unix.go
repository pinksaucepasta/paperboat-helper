//go:build unix

package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/pinksaucepasta/paperboat-helper/internal/filetransfer"
	"golang.org/x/sys/unix"
)

func secureOpenNoFollow(root, path string) (*os.File, error) {
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == "." || filepath.IsAbs(relative) {
		return nil, &filetransfer.Error{Code: filetransfer.InvalidPath, Cause: err}
	}
	components := strings.Split(relative, string(filepath.Separator))
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			return nil, &filetransfer.Error{Code: filetransfer.InvalidPath}
		}
	}

	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, &filetransfer.Error{Code: filetransfer.InvalidPath, Cause: err}
	}
	for index, component := range components {
		flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW
		if index < len(components)-1 {
			flags |= unix.O_DIRECTORY
		}
		next, openErr := unix.Openat(fd, component, flags, 0)
		closeErr := unix.Close(fd)
		if openErr != nil {
			return nil, &filetransfer.Error{Code: filetransfer.InvalidPath, Cause: errors.Join(openErr, closeErr)}
		}
		if closeErr != nil {
			_ = unix.Close(next)
			return nil, &filetransfer.Error{Code: filetransfer.InvalidPath, Cause: closeErr}
		}
		fd = next
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, &filetransfer.Error{Code: filetransfer.InvalidPath}
	}
	return file, nil
}
