package raftjss

import (
	"os"
	"testing"
)

func TestStore(t *testing.T) {
	defer os.Remove("test.json")
	os.Remove("test.json")
	s, err := Open("test.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetUint64([]byte("key"), 100); err != nil {
		t.Fatal(err)
	}
	val, err := s.GetUint64([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if val != 100 {
		t.Fatalf("expected %v, got %v", 100, val)
	}

	s, err = Open("test.json")
	if err != nil {
		t.Fatal(err)
	}
	val, err = s.GetUint64([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if val != 100 {
		t.Fatalf("expected %v, got %v", 100, val)
	}
	_, err = s.GetUint64([]byte("key2"))
	if err != ErrKeyNotFound {
		t.Fatalf("exptected %v, got %v", ErrKeyNotFound, err)
	}

}
