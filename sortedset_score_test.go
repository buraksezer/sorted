package gsorted

import "testing"

func Test_SortedSetWithScore(t *testing.T) {
	ss := NewSortedSetWithScore(0)
	defer ss.Close()

	keys := []string{}
	for i := 0; i < 100; i++ {
		err := ss.Set(bkey(i), uint64(100-i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		keys = append(keys, string(bkey(i)))
	}

	for i := 0; i < 100; i++ {
		ok := ss.Check(bkey(i))
		if !ok {
			t.Fatalf("Key could not be found: %s", bkey(i))
		}
	}

	idx := 99
	ss.Range(func(key []byte) bool {
		if keys[idx] != string(key) {
			t.Fatalf("Invalid key: %s", string(key))
		}
		idx--
		return true
	})

	for i := 0; i < 100; i++ {
		err := ss.Delete(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	if ss.Len() != 0 {
		t.Fatalf("Expected length is zero. Got: %d", ss.Len())
	}
}
