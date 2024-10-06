package log

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
)

const lenWidth = 8

var (
	enc = binary.BigEndian
)

type store struct {
	file *os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		file: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// check if file is nil
	if s.file == nil {
		return 0, 0, errors.New("file is nil")
	}

	// Check if file is closed
	_, err = s.file.Stat()
	if err != nil {
		return 0, 0, fmt.Errorf("log file is closed or inaccessible: %w", err)
	}

	pos = s.size
	// write the length of the record first
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// write the record itself
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// add the length of the record to the size
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if file is closed
	_, err := s.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("log file is closed or inaccessible: %w", err)
	}

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	buf := make([]byte, lenWidth)
	// read the length of the record
	if _, err := s.file.ReadAt(buf, int64(pos)); err != nil {
		return nil, err
	}
	n := enc.Uint64(buf)
	// create a buffer to hold the record
	buf = make([]byte, n)
	// read the record itself
	if _, err := s.file.ReadAt(buf, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return buf, nil
}

// ReadAt reads the record at the given position
func (s *store) ReadAt(buffer []byte, startPosition int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if file is closed
	_, err := s.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("log file is closed or inaccessible: %w", err)
	}

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.file.ReadAt(buffer, startPosition)
}

// Close closes the store and flushes any buffered data to the underlying file
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.file.Close()
}

func (s *store) Name() string {
	return s.file.Name()
}
