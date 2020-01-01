package raftjss

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
)

// ErrKeyNotFound is returned when a key is not found
var ErrKeyNotFound = errors.New("not found")

// StableStore represents key/value storage for Raft.
type StableStore struct {
	mu   sync.RWMutex
	path string
	kv   map[string]string
}

// Open a StableStore.
func Open(path string) (*StableStore, error) {
	s := &StableStore{path: path, kv: make(map[string]string)}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			if err := s.write(); err != nil {
				return nil, err
			}
			return s, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(data, &s.kv); err != nil {
		return nil, err
	}
	return s, nil
}

// Get value of key
func (s *StableStore) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.kv[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return []byte(value), nil
}

// Set value of key
func (s *StableStore) Set(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, _ := s.kv[string(key)]
	if len(value) == 0 {
		delete(s.kv, string(key))
	} else {
		s.kv[string(key)] = string(value)
	}
	var err error
	defer func() {
		if err != nil {
			if len(prev) == 0 {
				delete(s.kv, string(key))
			} else {
				s.kv[string(key)] = string(prev)
			}
		}
	}()
	err = s.write()
	return err
}

// GetUint64 is like Get, but for uint64 values
func (s *StableStore) GetUint64(key []byte) (uint64, error) {
	value, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(value), 10, 64)
}

// SetUint64 is like Set, but for uint64 values
func (s *StableStore) SetUint64(key []byte, value uint64) error {
	return s.Set(key, strconv.AppendUint(nil, value, 10))
}

// write store to disk. Atomic and performs fsync.
func (s *StableStore) write() error {
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return err
	}
	path := s.path + ".tmp-" + hex.EncodeToString(buf[:])
	data, err := json.MarshalIndent(s.kv, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.Create(path)
	if err == nil {
		defer func() {
			f.Close()
			os.Remove(path)
		}()
		if _, err = f.Write(data); err == nil {
			if err = f.Sync(); err == nil {
				if err = f.Close(); err == nil {
					return os.Rename(path, s.path)
				}
			}
		}
	}
	return err
}
