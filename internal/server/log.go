package server

import (
	"errors"
	"sync"

	v1 "github.com/adityavit/proglog/api/v1"
)

type Log struct {
	mu      sync.Mutex
	records []*v1.Record
}

var ErrOffsetNotFound = errors.New("offset not found")

func NewLog() *Log {
	return &Log{}
}

// Append adds a record to the log and returns the offset of the record
func (c *Log) Append(record *v1.Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

// Read takes in a offset and returns the record at that offset
func (c *Log) Read(offset uint64) (*v1.Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.records)) {
		return &v1.Record{}, ErrOffsetNotFound
	}
	return c.records[offset], nil
}
