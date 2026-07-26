//go:build !darwin && !linux

package main

import (
	"context"
	"errors"
	"io"
)

func runServiceCommand(context.Context, []string, io.Reader, io.Writer, io.Writer) error {
	return errors.New("system service management is unsupported on this platform")
}
