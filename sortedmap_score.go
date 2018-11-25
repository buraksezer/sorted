// Copyright (c) 2018 Burak Sezer. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gsorted

import (
	"encoding/binary"
	"hash/fnv"
)

type fnv64a struct{}

func (f fnv64a) Sum64(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum64()
}

// Hasher is responsible for generating unsigned, 64 bit hash of provided byte slice.
type Hasher interface {
	Sum64([]byte) uint64
}

// SortedMapWithScore is just a SortedMap which supports sort by score instead of keys.
type SortedMapWithScore struct {
	sm             *SortedMap
	hash           Hasher
	hkeysWithScore map[uint64]uint64
}

// NewSortedMapWithScore creates and returns a new SortedMapWithScore
func NewSortedMapWithScore(maxGarbageRatio float64, hasher Hasher) *SortedMapWithScore {
	if hasher == nil {
		hasher = fnv64a{}
	}
	return &SortedMapWithScore{
		hash:           hasher,
		sm:             NewSortedMap(maxGarbageRatio),
		hkeysWithScore: make(map[uint64]uint64),
	}
}

// Set sets a new key/value pair to the map.
func (m *SortedMapWithScore) Set(key, value []byte, score uint64) error {
	m.sm.mu.Lock()
	defer m.sm.mu.Unlock()

	// Create a new key with score. SkipList implementation will sort keys
	// lexicographically.
	tkey := make([]byte, len(key)+8)
	binary.BigEndian.PutUint64(tkey, score)
	copy(tkey[8:], key)

	// Get the last value, storage only calls Put on the last created table.
	s := m.sm.skiplists[len(m.sm.skiplists)-1]
	err := s.set(tkey, value)
	if err != nil {
		return err
	}

	hkey := m.hash.Sum64(key)
	m.hkeysWithScore[hkey] = score
	return nil
}

// GetScore returns the score for given key, if any. Otherwise it returns ErrKeyNotFound.
func (m *SortedMapWithScore) GetScore(key []byte) (uint64, error) {
	hkey := m.hash.Sum64(key)
	score, ok := m.hkeysWithScore[hkey]
	if !ok {
		return 0, ErrKeyNotFound
	}
	return score, nil
}

// Get returns the value for given key, if any. Otherwise it returns ErrKeyNotFound. The returned value is its own copy.
func (m *SortedMapWithScore) Get(key []byte) ([]byte, error) {
	m.sm.mu.RLock()
	defer m.sm.mu.RUnlock()

	hkey := m.hash.Sum64(key)
	score, ok := m.hkeysWithScore[hkey]
	if !ok {
		return nil, ErrKeyNotFound
	}

	// Create a new key with score.
	tkey := make([]byte, len(key)+8)
	binary.BigEndian.PutUint64(tkey, score)
	copy(tkey[8:], key)
	return m.sm.get(tkey)
}

// Delete deletes the key/value pair for given key. It may trigger compaction to reclaim memory.
func (m *SortedMapWithScore) Delete(key []byte) error {
	m.sm.mu.Lock()
	defer m.sm.mu.Unlock()

	hkey := m.hash.Sum64(key)
	score, ok := m.hkeysWithScore[hkey]
	if !ok {
		// Key not found. Just quit.
		return nil
	}

	tkey := make([]byte, len(key)+8)
	binary.BigEndian.PutUint64(tkey, score)
	copy(tkey[8:], key)
	err := m.sm.del(tkey)
	if err != nil {
		return err
	}
	delete(m.hkeysWithScore, hkey)
	return nil
}

// Range calls f sequentially for each key and value present in the SortedMap.
// If f returns false, range stops the iteration.
func (m *SortedMapWithScore) Range(f func(key, value []byte) bool) {
	m.sm.mu.RLock()
	defer m.sm.mu.RUnlock()

	fn := func(key, value []byte) bool {
		// Remove score from key.
		return f(key[8:], value)
	}
	m.sm.Range(fn)
}

// SubMap returns a view of the portion of this map whose keys range from fromKey, inclusive, to toKey, exclusive.
func (m *SortedMapWithScore) SubMap(fromScore, toScore uint64, f func(key, value []byte) bool) error {
	m.sm.mu.RLock()
	defer m.sm.mu.RUnlock()

	var fs, ts []byte
	if fromScore != 0 {
		fs = make([]byte, 8)
		binary.BigEndian.PutUint64(fs, fromScore)
	}
	if toScore != 0 {
		ts = make([]byte, 8)
		binary.BigEndian.PutUint64(ts, toScore)
	}

	fn := func(key, value []byte) bool {
		// Remove score from key.
		return f(key[8:], value)
	}
	return m.sm.SubMap(fs, ts, fn)
}

// HeadMap returns a view of the portion of this map whose keys are strictly less than toKey.
func (m *SortedMapWithScore) HeadMap(toScore uint64, f func(key, value []byte) bool) error {
	return m.SubMap(0, toScore, f)
}

// TailMap returns a view of the portion of this map whose keys are greater than or equal to fromKey.
func (m *SortedMapWithScore) TailMap(fromScore uint64, f func(key, value []byte) bool) error {
	return m.SubMap(fromScore, 0, f)
}

// Close stops background tasks, if any and waits for them until quit.
func (m *SortedMapWithScore) Close() error {
	return m.sm.Close()
}

// Len returns the length of SortedMap.
func (m *SortedMapWithScore) Len() int {
	return m.sm.Len()
}

// Check checks the key without reading its value and returns true, if any.
func (m *SortedMapWithScore) Check(key []byte) bool {
	m.sm.mu.RLock()
	defer m.sm.mu.RUnlock()

	hkey := m.hash.Sum64(key)
	_, ok := m.hkeysWithScore[hkey]
	return ok
}
