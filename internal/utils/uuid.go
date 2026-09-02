package utils

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

func NewUUID() (string, error) {
	buf := uuidbufpool.Get().(*[16]byte)
	defer func() {
		// No reset required, it's overwritten on each run
		uuidbufpool.Put(buf)
	}()

	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	buf[6] = (buf[6] & 0x0f) | 0x40 // version 4
	buf[8] = (buf[8] & 0x3f) | 0x80 // variant 10

	h := hex.EncodeToString(buf[:])
	var sb strings.Builder
	sb.Grow(36)
	sb.WriteString(h[0:8])
	sb.WriteByte('-')
	sb.WriteString(h[8:12])
	sb.WriteByte('-')
	sb.WriteString(h[12:16])
	sb.WriteByte('-')
	sb.WriteString(h[16:20])
	sb.WriteByte('-')
	sb.WriteString(h[20:32])
	return sb.String(), nil
}

// IsValidUUID reports whether id has the exact shape produced by NewUUID.
// Hand-rolled rather than regexp: id comes straight from caller input and is
// used to build a filesystem path, so this doubles as the path-traversal
// guard (e.g. rejects "../../etc/passwd") and stays allocation-free.
func IsValidUUID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i := 0; i < len(id); i++ {
		switch i {
		case 8, 13, 18, 23:
			if id[i] != '-' {
				return false
			}
		default:
			if !IsHexDigit(id[i]) {
				return false
			}
		}
	}
	return true
}

func IsHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
}
