package gsorted

import "encoding/binary"

type SortedMapWithScore struct {
	sm             *SortedMap
	hkeysWithScore map[uint64]uint64
}

func NewSortedMapWithScore(maxGarbageRatio float64) *SortedMapWithScore {
	return &SortedMapWithScore{
		sm:             NewSortedMap(maxGarbageRatio),
		hkeysWithScore: make(map[uint64]uint64),
	}
}

func (m *SortedMapWithScore) Set(key, value []byte, score uint64) error {
	m.sm.mu.Lock()
	defer m.sm.mu.Unlock()

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

func (m *SortedMapWithScore) Get(key []byte) ([]byte, error) {
	m.sm.mu.RLock()
	defer m.sm.mu.RUnlock()

	hkey := m.sm.hash.Sum64(key)
	score, ok := m.hkeysWithScore[hkey]
	if !ok {
		return nil, ErrKeyNotFound
	}

	tkey := make([]byte, len(key)+8)
	binary.BigEndian.PutUint64(tkey, score)
	copy(tkey[8:], key)
	return m.sm.get(tkey)
}

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
	err := m.sm.delete(tkey)
	if err != nil {
		return err
	}
	delete(m.hkeysWithScore, hkey)
	return nil
}

func (m *SortedMapWithScore) Check(key []byte) bool {
	m.sm.mu.RLock()
	defer m.sm.mu.RUnlock()

	hkey := m.sm.hash.Sum64(key)
	_, ok := m.hkeysWithScore[hkey]
	return ok
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration. Range may be O(N) with
// the number of elements in the map even if f returns false after a constant
// number of calls.
func (m *SortedMapWithScore) Range(fn func(key, value []byte) bool) {
	m.sm.mu.RLock()
	defer m.sm.mu.RUnlock()

	// Scan available tables by starting the last added table.
	for i := len(m.sm.skiplists) - 1; i >= 0; i-- {
		s := m.sm.skiplists[i]
		it := s.NewIterator()
		for it.Next() {
			tkey := it.Key()
			if !fn(tkey[8:], it.Value()) {
				break
			}
		}
	}
}

func (m *SortedMapWithScore) Close() error {
	return m.sm.Close()
}

func (m *SortedMapWithScore) Len() int {
	return m.sm.Len()
}
