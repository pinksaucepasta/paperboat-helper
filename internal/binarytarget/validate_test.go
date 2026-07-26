package binarytarget

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestValidateExecutableTargets(t *testing.T) {
	root := t.TempDir()
	write := func(name string, body []byte) string {
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, body, 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	elf := make([]byte, 32)
	copy(elf, "\x7fELF")
	elf[4], elf[5] = 2, 1
	binary.LittleEndian.PutUint16(elf[18:20], 62)
	macho := make([]byte, 32)
	binary.LittleEndian.PutUint32(macho[:4], 0xfeedfacf)
	binary.LittleEndian.PutUint32(macho[4:8], 0x0100000c)
	linux, darwin := write("linux", elf), write("darwin", macho)
	if err := Validate(linux, "linux", "amd64"); err != nil {
		t.Fatal(err)
	}
	if err := Validate(darwin, "darwin", "arm64"); err != nil {
		t.Fatal(err)
	}
	for name, input := range map[string]struct{ path, platform, architecture string }{
		"wrong platform":     {linux, "darwin", "amd64"},
		"wrong architecture": {darwin, "darwin", "amd64"},
		"unsupported":        {linux, "windows", "amd64"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := Validate(input.path, input.platform, input.architecture); err == nil {
				t.Fatal("mismatch accepted")
			}
		})
	}
}
