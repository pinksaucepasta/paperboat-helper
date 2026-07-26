//go:build !darwin && !linux

package main

import (
	"context"
	"errors"
	"io"
)

func runDoctor(context.Context, []string, io.Writer, io.Writer) error {
	return errors.New("doctor is unsupported on this platform")
}
