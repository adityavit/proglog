package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	v1 "github.com/adityavit/proglog/api/v1"
)

type Log struct {
	Dir           string
	Config        Config
	mu            sync.RWMutex
	segments      []*Segment
	activeSegment *Segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	if err := l.setup(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))
		off, err := strconv.ParseUint(offStr, 10, 0)
		if err != nil {
			return err
		}
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	// Create segments
	l.segments = make([]*Segment, 0)
	for i := 0; i < len(baseOffsets); i++ {
		// There is two offsets with same value, one of them is the index and the other is the store
		// We only need one of them
		if i > 0 && baseOffsets[i] == baseOffsets[i-1] {
			continue
		}
		if err := l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
	}
	// If no segments are created, create one
	if len(l.segments) == 0 {
		if err := l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

// Create a new segment
func (l *Log) newSegment(baseOffset uint64) error {
	// fmt.Printf("Creating new segment at %v with base offset %v", l.Dir, baseOffset)
	segment, err := NewSegment(l.Dir, baseOffset, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, segment)
	l.activeSegment = segment
	// fmt.Printf("Created new segment %v with base offset %v", segment, baseOffset)
	return nil
}

// Append a record to the log
func (l *Log) Append(record *v1.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	offset, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(l.activeSegment.nextOffset)
	}
	return offset, err
}

func (l *Log) Read(offset uint64) (*v1.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var segment *Segment
	// Binary search can be used to find the segment
	for _, s := range l.segments {
		if s.baseOffset <= offset && offset < s.nextOffset {
			segment = s
			break
		}
	}
	if segment == nil || segment.nextOffset <= offset {
		return nil, fmt.Errorf("offset %d not found and is out of range", offset)
	}
	return segment.Read(offset)
}

func (l *Log) Close() error {
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	// fmt.Printf("segments: %v\n", l.segments)
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	offset := l.segments[len(l.segments)-1].nextOffset
	if offset == 0 {
		return 0, nil
	}
	return offset - 1, nil
}

func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*Segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

type segmentReader struct {
	*store
	off int64
}

func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &segmentReader{
			segment.store,
			0,
		}
	}
	return io.MultiReader(readers...)
}

func (s *segmentReader) Read(p []byte) (int, error) {
	n, err := s.ReadAt(p, s.off)
	s.off += int64(n)
	return n, err
}
