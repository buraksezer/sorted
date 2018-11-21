package gsorted

type SortedSetWithScore struct {
	m *SortedMapWithScore
}

func NewSortedSetWithScore(maxGarbageRatio float64) *SortedSetWithScore {
	return &SortedSetWithScore{
		m: NewSortedMapWithScore(maxGarbageRatio),
	}
}

func (s *SortedSetWithScore) Set(key []byte, score uint64) error {
	return s.m.Set(key, nil, score)
}

func (s *SortedSetWithScore) Delete(key []byte) error {
	return s.m.Delete(key)
}

func (s *SortedSetWithScore) Len() int {
	return s.m.Len()
}

func (s *SortedSetWithScore) Check(key []byte) bool {
	return s.m.Check(key)
}

func (s *SortedSetWithScore) Close() error {
	return s.m.Close()
}

func (s *SortedSetWithScore) Range(f func(key []byte) bool) {
	s.m.sm.mu.RLock()
	defer s.m.sm.mu.RUnlock()

	// Scan available tables by starting the last added skiplist.
	for i := len(s.m.sm.skiplists) - 1; i >= 0; i-- {
		sl := s.m.sm.skiplists[i]
		it := sl.NewIterator()
		for it.Next() {
			key := it.Key()[8:]
			if !f(key) {
				break
			}
		}
	}
}
