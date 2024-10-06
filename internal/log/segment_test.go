package log

import (
	"io"
	"os"
	"testing"

	v1 "github.com/adityavit/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestNewSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-test")
	defer os.RemoveAll(dir)

	config := Config{}
	wantRecord := v1.Record{
		Value: []byte("hello world"),
	}
	wantRecords := uint64(3)
	config.Segment.MaxStoreBytes = uint64(len(wantRecord.Value) * int(wantRecords))
	// Store 3 entries
	config.Segment.MaxIndexBytes = entryWidth * wantRecords
	s, err := NewSegment(dir, 16, config)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.baseOffset)
	require.Equal(t, uint64(16), s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < wantRecords; i++ {
		// Append the record to the segment
		off, err := s.Append(&wantRecord)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		// Read the record back from the segment
		gotRecord, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, wantRecord.Value, *&gotRecord.Value)
	}
	// Write a record that exceeds the max index entry size
	_, err = s.Append(&wantRecord)
	require.Equal(t, io.EOF, err)
	require.True(t, s.IsMaxed())

	// Test max store bytes, already 3 entries exist, so maxed out
	config.Segment.MaxStoreBytes = uint64(len(wantRecord.Value) * int(wantRecords-1))
	config.Segment.MaxIndexBytes = 1024

	s, err = NewSegment(dir, 16, config)
	require.NoError(t, err)
	require.True(t, s.IsMaxed())

	// Remove the segment and open it again no max limits reached
	err = s.Remove()
	require.NoError(t, err)
	s, err = NewSegment(dir, 16, config)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
