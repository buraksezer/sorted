package gsorted

type SortedSet struct {
	sm *SortedMap
}

func NewSortedSet(maxGarbageRatio float64) *SortedSet {
	return &SortedSet{
		sm: NewSortedMap(maxGarbageRatio),
	}
}

func (ss *SortedSet) Set(key []byte) error {
	return ss.sm.Set(key, nil)
}

func (ss *SortedSet) Delete(key []byte) error {
	return ss.sm.Delete(key)
}

func (ss *SortedSet) Len() int {
	return ss.sm.Len()
}

func (ss *SortedSet) Check(key []byte) bool {
	return ss.sm.Check(key)
}

func (ss *SortedSet) Close() error {
	return ss.sm.Close()
}

func (ss *SortedSet) Range(f func(key []byte) bool) {
	ss.sm.mu.RLock()
	defer ss.sm.mu.RUnlock()

	// Scan available tables by starting the last added skiplist.
	for i := len(ss.sm.skiplists) - 1; i >= 0; i-- {
		s := ss.sm.skiplists[i]
		it := s.NewIterator()
		for it.Next() {
			if !f(it.Key()) {
				break
			}
		}
	}
}
