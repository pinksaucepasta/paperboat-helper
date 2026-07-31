package configsync

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"unicode/utf8"
)

var ErrTextMergeConflict = errors.New("config text merge is not clean")

func mergeRegularText(ctx context.Context, gitBinary string, base, local, remote []byte, maxBytes int64) ([]byte, error) {
	if gitBinary == "" || maxBytes < 1 || int64(len(base)) > maxBytes || int64(len(local)) > maxBytes ||
		int64(len(remote)) > maxBytes || !mergeText(base) || !mergeText(local) || !mergeText(remote) {
		return nil, ErrTextMergeConflict
	}
	root, err := os.MkdirTemp("", "paperboat-merge-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(root)
	paths := []string{filepath.Join(root, "local"), filepath.Join(root, "base"), filepath.Join(root, "remote")}
	for index, value := range [][]byte{local, base, remote} {
		if err := os.WriteFile(paths[index], value, 0o600); err != nil {
			return nil, err
		}
	}
	output := &boundedMergeBuffer{limit: maxBytes}
	command := exec.CommandContext(ctx, gitBinary, "merge-file", "-p", "-q", paths[0], paths[1], paths[2])
	command.Stdout = output
	command.Stderr = &boundedMergeBuffer{limit: 64 << 10}
	err = command.Run()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil || output.overflow || !mergeText(output.Bytes()) {
		return nil, ErrTextMergeConflict
	}
	return append([]byte(nil), output.Bytes()...), nil
}

func mergeText(value []byte) bool {
	return utf8.Valid(value) && !bytes.ContainsRune(value, 0)
}

type boundedMergeBuffer struct {
	bytes.Buffer
	limit    int64
	overflow bool
}

func (b *boundedMergeBuffer) Write(value []byte) (int, error) {
	if int64(b.Len()+len(value)) > b.limit {
		b.overflow = true
		return len(value), nil
	}
	return b.Buffer.Write(value)
}
