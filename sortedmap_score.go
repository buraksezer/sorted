// Copyright (c) 2018 Burak Sezer. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gsorted

import "encoding/binary"

// SortedMapWithScore is just a SortedMap which supports sort by score instead of keys.
type SortedMapWithScore struct {
	sm             *SortedMap
	hkeysWithScore map[uint64]uint64
}

// NewSortedMapWithScore creates and returns a new SortedMapWithScore
func NewSortedMapWithScore(maxGarbageRatio float64) *SortedMapWithScore {
	return &SortedMapWithScore{
		sm:             NewSortedMap(maxGarbageRatio),
		hkeysWithScore: make(map[uint64]uint64),
	}
}

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
	err := s.Set(tkey, value)
	if err != nil {
		return err
	}

	hkey := m.sm.hash.Sum64(key)
	m.hkeysWithScore[hkey] = score
	return nil
}

// GetScore returns the score for given key, if any. Otherwise it returns ErrKeyNotFound.
func (m *SortedMapWithScore) GetScore(key []byte) (uint64, error) {
	hkey := m.sm.hash.Sum64(key)
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

	hkey := m.sm.hash.Sum64(key)
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

	hkey := m.sm.hash.Sum64(key)
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

// Check checks the key without reading its value and returns true, if any.
func (m *SortedMapWithScore) Check(key []byte) bool {
	m.sm.mu.RLock()
	defer m.sm.mu.RUnlock()

	hkey := m.sm.hash.Sum64(key)
	_, ok := m.hkeysWithScore[hkey]
	return ok
}

// Range calls f sequentially for each key and value present in the SortedMap.
// If f returns false, range stops the iteration. Range may be O(N) with
// the number of elements in the map even if f returns false after a constant
// number of calls.
func (m *SortedMapWithScore) Range(f func(key, value []byte) bool) {
	m.sm.mu.RLock()
	defer m.sm.mu.RUnlock()

	// Scan available tables by starting the last added table.
	for i := len(m.sm.skiplists) - 1; i >= 0; i-- {
		s := m.sm.skiplists[i]
		it := s.NewIterator()
		for it.Next() {
			tkey := it.Key()
			// remove score and call user defined function.
			if !f(tkey[8:], it.Value()) {
				break
			}
		}
	}
}

func (m *SortedMapWithScore) SubMap(fromScore, toScore uint64, f func(key, value []byte) bool) {
	m.sm.mu.RLock()
	defer m.sm.mu.RUnlock()

	fs := make([]byte, 8)
	binary.BigEndian.PutUint64(fs, fromScore)
	ts := make([]byte, 8)
	binary.BigEndian.PutUint64(ts, toScore)

	// Scan available tables by starting the last added table.
	for i := len(m.sm.skiplists) - 1; i >= 0; i-- {
		s := m.sm.skiplists[i]
		it := s.SubMap(fs, ts)
		for it.Next() {
			tkey := it.Key()
			// remove score and call user defined function.
			if !f(tkey[8:], it.Value()) {
				break
			}
		}
	}
}

// Close stops background tasks, if any and waits for them until quit.
func (m *SortedMapWithScore) Close() error {
	return m.sm.Close()
}

// Len returns the length of SortedMap.
func (m *SortedMapWithScore) Len() int {
	return m.sm.Len()
}
