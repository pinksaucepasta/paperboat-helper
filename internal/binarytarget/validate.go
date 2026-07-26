package binarytarget

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

var ErrInvalid = errors.New("executable target does not match the declared platform")

func Validate(path, platform, architecture string) error {
	file, err := os.Open(path)
	if err != nil {
		return ErrInvalid
	}
	defer file.Close()
	header := make([]byte, 32)
	if _, err := io.ReadFull(file, header); err != nil {
		return ErrInvalid
	}
	switch platform {
	case "linux":
		if string(header[:4]) != "\x7fELF" || header[4] != 2 || header[5] != 1 {
			return ErrInvalid
		}
		machine := binary.LittleEndian.Uint16(header[18:20])
		if architecture == "amd64" && machine == 62 || architecture == "arm64" && machine == 183 {
			return nil
		}
	case "darwin":
		if binary.LittleEndian.Uint32(header[:4]) != 0xfeedfacf {
			return ErrInvalid
		}
		cpu := binary.LittleEndian.Uint32(header[4:8])
		if architecture == "amd64" && cpu == 0x01000007 || architecture == "arm64" && cpu == 0x0100000c {
			return nil
		}
	}
	return ErrInvalid
}
