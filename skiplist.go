// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package memdb provides a memory-backed implementation of the db.DB
// interface.
//
// A MemDB's memory consumption increases monotonically, even if keys are
// deleted or values are updated with shorter slices. Callers of the package
// are responsible for explicitly compacting a MemDB into a separate DB
// (whether in-memory or on-disk) when appropriate.
package gsorted

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sync/atomic"
)

// maxHeight is the maximum height of a MemDB's skiplist.
const maxHeight = 12

// A MemDB's skiplist consists of a number of nodes, and each node is
// represented by a variable number of ints: a key-offset, a value-offset, and
// between 1 and maxHeight next nodes. The key-offset and value-offset encode
// the node's key/value pair and are offsets into a MemDB's kvData slice.
// The remaining ints, for the next nodes in the skiplist's linked lists, are
// offsets into a MemDB's nodeData slice.
//
// The fXxx constants represent how to find the Xxx field of a node in the
// nodeData. For example, given an int 30 representing a node, and given
// nodeData[30:36] that looked like [60, 71, 82, 83, 84, 85], then
// nodeData[30 + fKey] = 60 would be the node's key-offset,
// nodeData[30 + fVal] = 71 would be the node's value-offset, and
// nodeData[30 + fNxt + 0] = 82 would be the next node at the height-0 list,
// nodeData[30 + fNxt + 1] = 83 would be the next node at the height-1 list,
// and so on. A node's height is implied by the skiplist construction: a node
// of height x appears in the height-h list iff 0 <= h && h < x.
const (
	fKey = iota
	fVal
	fNxt
)

const (
	// zeroNode represents the end of a linked list.
	zeroNode = 0
	// headNode represents the start of the linked list. It is equal to -fNxt
	// so that the next nodes at height-h are at nodeData[h].
	// The head node is an artificial node and has no key or value.
	headNode = -fNxt
)

// A node's key-offset and value-offset fields are offsets into a MemDB's
// kvData slice that stores varint-prefixed strings: the node's key and value.
// A negative offset means a zero-length string, whether explicitly set to
// empty or implicitly set by deletion.
const (
	kvOffsetEmptySlice  = -1
	kvOffsetDeletedNode = -2
)

// skipList is a memory-backed implementation of the db.DB interface.
//
// It is safe to call Get, Set, Delete and Find concurrently.
type skipList struct {
	// height is the number of such lists, which can increase over time.
	height int
	// kvData is an append-only buffer that holds varint-prefixed strings.
	kvData []byte
	// nodeData is an append-only buffer that holds a node's fields.
	nodeData []int

	garbage int64
	length  int64
}

// load loads a []byte from m.kvData.
func (s *skipList) load(kvOffset int) (b []byte) {
	if kvOffset < 0 {
		return nil
	}
	bLen, n := binary.Uvarint(s.kvData[kvOffset:])
	i, j := kvOffset+n, kvOffset+n+int(bLen)
	return s.kvData[i:j:j]
}

// save saves a []byte to m.kvData.
func (s *skipList) save(b []byte) (kvOffset int) {
	if len(b) == 0 {
		return kvOffsetEmptySlice
	}
	kvOffset = len(s.kvData)
	var buf [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(buf[:], uint64(len(b)))
	s.kvData = append(s.kvData, buf[:length]...)
	s.kvData = append(s.kvData, b...)
	return kvOffset
}

// findNode returns the first node n whose key is >= the given key (or nil if
// there is no such node) and whether n's key equals key. The search is based
// solely on the contents of a node's key. Whether or not that key was
// previously deleted from the MemDB is not relevant.
//
// If prev is non-nil, it also sets the first m.height elements of prev to the
// preceding node at each height.
func (s *skipList) findNode(key []byte, prev *[maxHeight]int) (n int, exactMatch bool) {
	for h, p := s.height-1, headNode; h >= 0; h-- {
		// Walk the skiplist at height h until we find either a zero node
		// or one whose key is >= the given key.
		n = s.nodeData[p+fNxt+h]
		for {
			if n == zeroNode {
				exactMatch = false
				break
			}
			kOff := s.nodeData[n+fKey]
			if c := bytes.Compare(s.load(kOff), key); c >= 0 {
				exactMatch = c == 0
				break
			}
			p, n = n, s.nodeData[n+fNxt+h]
		}
		if prev != nil {
			(*prev)[h] = p
		}
	}
	return n, exactMatch
}

// Get implements DB.Get, as documented in the leveldb/db package.
func (s *skipList) Get(key []byte) (value []byte, err error) {
	n, exactMatch := s.findNode(key, nil)
	vOff := s.nodeData[n+fVal]
	if !exactMatch || vOff == kvOffsetDeletedNode {
		return nil, ErrKeyNotFound
	}
	return s.load(vOff), nil
}

func (s *skipList) Check(key []byte) bool {
	n, exactMatch := s.findNode(key, nil)
	vOff := s.nodeData[n+fVal]
	if !exactMatch || vOff == kvOffsetDeletedNode {
		return false
	}
	return true
}

// Set implements DB.Set, as documented in the leveldb/db package.
func (s *skipList) Set(key, value []byte) error {
	// Increase length to count items in the skiplist.
	atomic.AddInt64(&s.length, 1)

	// Find the node, and its predecessors at all heights.
	var prev [maxHeight]int
	n, exactMatch := s.findNode(key, &prev)
	if exactMatch {
		s.nodeData[n+fVal] = s.save(value)
		return nil
	}
	// Choose the new node's height, branching with 25% probability.
	h := 1
	for h < maxHeight && rand.Intn(4) == 0 {
		h++
	}
	// Raise the skiplist's height to the node's height, if necessary.
	if s.height < h {
		for i := s.height; i < h; i++ {
			prev[i] = headNode
		}
		s.height = h
	}
	// Insert the new node.
	var x [fNxt + maxHeight]int
	n1 := len(s.nodeData)
	x[fKey] = s.save(key)
	x[fVal] = s.save(value)
	for i := 0; i < h; i++ {
		j := prev[i] + fNxt + i
		x[fNxt+i] = s.nodeData[j]
		s.nodeData[j] = n1
	}
	s.nodeData = append(s.nodeData, x[:fNxt+h]...)
	return nil
}

func (s *skipList) keyLen(n int) int {
	kvOffset := s.nodeData[n+fKey]
	bLen, nr := binary.Uvarint(s.kvData[kvOffset:])
	i, j := kvOffset+nr, kvOffset+nr+int(bLen)
	return j - i
}

func (s *skipList) valueLen(n int) int {
	kvOffset := s.nodeData[n+fVal]
	if kvOffset < 0 {
		// Value is nil
		return 0
	}
	bLen, nr := binary.Uvarint(s.kvData[kvOffset:])
	i, j := kvOffset+nr, kvOffset+nr+int(bLen)
	return j - i
}

// Delete implements DB.Delete, as documented in the leveldb/db package.
func (s *skipList) Delete(key []byte) error {
	n, exactMatch := s.findNode(key, nil)
	if !exactMatch || s.nodeData[n+fVal] == kvOffsetDeletedNode {
		return ErrKeyNotFound
	}

	garbage := s.keyLen(n) + s.valueLen(n) + 3 // 3 bytes for metadata
	atomic.AddInt64(&s.garbage, int64(garbage))
	atomic.AddInt64(&s.length, -1)

	s.nodeData[n+fVal] = kvOffsetDeletedNode
	return nil
}

// NewIterator implements DB.NewIterator, as documented in the leveldb/db package.
func (s *skipList) NewIterator() *Iterator {
	n, _ := s.findNode(nil, nil)
	for n != zeroNode && s.nodeData[n+fVal] == kvOffsetDeletedNode {
		n = s.nodeData[n+fNxt]
	}
	t := &Iterator{
		m:           s,
		restartNode: n,
	}
	t.fill()
	// The Iterator is positioned at the first node >= key. The iterator API
	// requires that the caller the Next first, so we set t.i0 to -1.
	t.i0 = -1
	return t
}

func (s *skipList) Len() int {
	return int(atomic.LoadInt64(&s.length))
}

// ApproximateMemoryUsage returns the approximate memory usage of the MemDB.
func (s *skipList) ApproximateMemoryUsage() int {
	return len(s.kvData)
}

// Empty returns whether the MemDB has no key/value pairs.
func (s *skipList) Empty() bool {
	return len(s.nodeData) == maxHeight
}

func newSkipList() *skipList {
	return &skipList{
		height: 1,
		kvData: make([]byte, 0, 4096),
		// The first maxHeight values of nodeData are the next nodes after the
		// head node at each possible height. Their initial value is zeroNode.
		nodeData: make([]int, maxHeight, 256),
	}
}

// Iterator is a MemDB iterator that buffers upcoming results, so that it does
// not have to acquire the MemDB's mutex on each Next call.
type Iterator struct {
	m *skipList
	// restartNode is the node to start refilling the buffer from.
	restartNode int

	limitNode int

	// i0 is the current Iterator position with respect to buf. A value of -1
	// means that the Iterator is at the start, end or both of the iteration.
	// i1 is the number of buffered entries.
	// Invariant: -1 <= i0 && i0 < i1 && i1 <= len(buf).
	i0, i1 int
	// buf buffers up to 32 key/value pairs.
	buf [32][2][]byte
}

// fill fills the Iterator's buffer with key/value pairs from the MemDB.
//
// Precondition: t.m.mutex is locked for reading.
func (t *Iterator) fill() {
	i, n := 0, t.restartNode
	for i < len(t.buf) && n != zeroNode && n != t.limitNode {
		if t.m.nodeData[n+fVal] != kvOffsetDeletedNode {
			t.buf[i][fKey] = t.m.load(t.m.nodeData[n+fKey])
			t.buf[i][fVal] = t.m.load(t.m.nodeData[n+fVal])
			i++
		}
		n = t.m.nodeData[n+fNxt]
	}
	if i == 0 {
		// There were no non-deleted nodes on or after t.restartNode.
		// The Iterator is exhausted.
		t.i0 = -1
	} else {
		t.i0 = 0
	}
	t.i1 = i
	t.restartNode = n
}

// Next implements Iterator.Next, as documented in the leveldb/db package.
func (t *Iterator) Next() bool {
	t.i0++
	if t.i0 < t.i1 {
		return true
	}
	if t.restartNode == zeroNode || t.restartNode == t.limitNode {
		t.i0 = -1
		t.i1 = 0
		return false
	}
	t.fill()
	return true
}

// Key implements Iterator.Key, as documented in the leveldb/db package.
func (t *Iterator) Key() []byte {
	if t.i0 < 0 {
		return nil
	}
	return t.buf[t.i0][fKey]
}

// Value implements Iterator.Value, as documented in the leveldb/db package.
func (t *Iterator) Value() []byte {
	if t.i0 < 0 {
		return nil
	}
	return t.buf[t.i0][fVal]
}

// Find implements DB.Find, as documented in the leveldb/db package.
func (s *skipList) SubMap(fromKey, toKey []byte) *Iterator {
	n, _ := s.findNode(fromKey, nil)
	for n != zeroNode && s.nodeData[n+fVal] == kvOffsetDeletedNode {
		n = s.nodeData[n+fNxt]
	}

	limitNode, _ := s.findNode(toKey, nil)
	t := &Iterator{
		m:           s,
		restartNode: n,
		limitNode:   limitNode,
	}
	t.fill()
	// The iterator is positioned at the first node >= key. The iterator API
	// requires that the caller the Next first, so we set t.i0 to -1.
	t.i0 = -1
	return t
}
