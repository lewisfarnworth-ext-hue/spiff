package utils

import (
	"regexp"
	"testing"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewUUID(t *testing.T) {
	id, err := NewUUID()
	if err != nil {
		t.Fatalf("newUUID() error = %v", err)
	}
	if !uuidPattern.MatchString(id) {
		t.Fatalf("newUUID() = %q, want RFC 4122 v4 UUID", id)
	}
}

func TestNewUUID_Unique(t *testing.T) {
	seen := make(map[string]struct{}, 100)
	for range 100 {
		id, err := NewUUID()
		if err != nil {
			t.Fatalf("newUUID() error = %v", err)
		}
		if _, ok := seen[id]; ok {
			t.Fatalf("newUUID() produced duplicate %q", id)
		}
		seen[id] = struct{}{}
	}
}

func BenchmarkNewUUID(b *testing.B) {
	for b.Loop() {
		if _, err := NewUUID(); err != nil {
			b.Fatal(err)
		}
	}
}
