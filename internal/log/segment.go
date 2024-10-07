package log

import (
	"fmt"
	"os"
	"path/filepath"

	v1 "github.com/adityavit/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type Segment struct {
	store      *store
	index      *index
	config     Config
	baseOffset uint64
	nextOffset uint64
}

func NewSegment(dir string, baseOffset uint64, c Config) (*Segment, error) {
	segment := &Segment{
		baseOffset: baseOffset,
		config:     c,
	}
	storeFile, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	segment.store, err = newStore(storeFile)
	if err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	segment.index, err = newIndex(indexFile, c)
	if err != nil {
		return nil, err
	}
	// If can read the last entry, then we know the next offset
	// Otherwise, we start at the base offset
	if off, _, err := segment.index.Read(-1); err != nil {
		segment.nextOffset = baseOffset
	} else {
		segment.nextOffset = baseOffset + uint64(off) + 1
	}

	return segment, nil
}

func (s *Segment) Append(record *v1.Record) (offset uint64, err error) {
	// log.Printf("Appending record to segment %v", s)
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)
	// fmt.Printf("n: %d, pos: %d, curOffset: %d\n", n, pos, cur)
	if err != nil {
		return 0, err
	}
	// Write the index entry
	// We subtract the base offset because stored offset in the index is relative to the base offset
	if err = s.index.Write(uint32(s.nextOffset-uint64(s.baseOffset)), pos); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

func (s *Segment) Read(off uint64) (*v1.Record, error) {
	// Read the index entry
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &v1.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

func (s *Segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *Segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *Segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// nearestMultiple returns the nearest and lesser multiple of k in j
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
