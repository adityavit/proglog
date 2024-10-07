package log

import (
	"fmt"
	"io"
	"os"
	"testing"

	v1 "github.com/adityavit/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		// "append and read a record succeeds": testAppendRead,
		// "offset out of range error":         testOutOfRange,
		// "init with existing segments":       testInitWithExistingSegment,
		// "reader":                            testReader,
		"truncate": testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "log-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			config := Config{}
			config.Segment.InitialOffset = 0
			config.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, config)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	wantRecord := &v1.Record{
		Value: []byte("hello world"),
	}
	// append the record to the log
	off, err := log.Append(wantRecord)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	// read the record back from the log
	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, wantRecord.Value, read.Value)

	// append another record to the log
	wantRecord2 := &v1.Record{
		Value: []byte("hello again"),
	}
	off, err = log.Append(wantRecord2)
	require.NoError(t, err)
	require.Equal(t, uint64(1), off)

	read, err = log.Read(off)
	require.NoError(t, err)
	require.Equal(t, wantRecord2.Value, read.Value)
}

func testOutOfRange(t *testing.T, log *Log) {
	read, err := log.Read(2)
	require.Error(t, err)
	require.Nil(t, read)
}

func testInitWithExistingSegment(t *testing.T, log *Log) {
	wantRecord := &v1.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		off, err := log.Append(wantRecord)
		require.NoError(t, err)
		require.Equal(t, uint64(i), off)
	}
	require.NoError(t, log.Close())
	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = log.HighestOffset()
	require.NoError(t, err)
	fmt.Println("off", off)
	require.Equal(t, uint64(2), off)

	log, err = NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	off, err = log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, wantRecord.Value, read.Value)
	newWantRecord := &v1.Record{
		Value: []byte("hello world again"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(newWantRecord)
		require.NoError(t, err)
	}
	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(5), off)

	read, err = log.Read(off)
	require.NoError(t, err)
	require.Equal(t, newWantRecord.Value, read.Value)
}

func testReader(t *testing.T, log *Log) {
	wantRecord := &v1.Record{
		Value: []byte("hello world"),
	}
	offset, err := log.Append(wantRecord)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	reader := log.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)
	actualRecord := &v1.Record{}
	err = proto.Unmarshal(b[lenWidth:], actualRecord)
	require.NoError(t, err)
	require.Equal(t, wantRecord.Value, actualRecord.Value)
}

func testTruncate(t *testing.T, log *Log) {
	wantRecord := &v1.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		offset, err := log.Append(wantRecord)
		require.NoError(t, err)
		require.Equal(t, uint64(i), offset)
	}
	off, err := log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	err = log.Truncate(1)
	require.NoError(t, err)

	// off, err = log.LowestOffset()
	// require.NoError(t, err)
	// require.Equal(t, uint64(1), off)

	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	_, err = log.Read(0)
	require.Error(t, err)
}
