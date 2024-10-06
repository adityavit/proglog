package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testData := [][]byte{
		[]byte("hello world"),
		[]byte("foo bar"),
		[]byte("test data"),
	}
	expectedPos := uint64(0)
	for _, data := range testData {
		n, pos, err := s.Append(data)
		require.NoError(t, err)
		require.Equal(t, uint64(len(data)+lenWidth), n)
		require.Equal(t, expectedPos, pos)
		expectedPos += uint64(lenWidth) + uint64(len(data))

		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, data, read)
	}
}

func TestStoreReadAt(t *testing.T) {
	f, err := os.CreateTemp("", "store_read_at_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testData := []byte("hello world")
	_, pos, err := s.Append(testData)
	require.NoError(t, err)

	readData := make([]byte, len(testData))
	n, err := s.ReadAt(readData, int64(pos+lenWidth))
	require.NoError(t, err)
	require.Equal(t, len(testData), n)
	require.Equal(t, testData, readData)
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	_, _, err = s.Append([]byte("test data"))
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	// Trying to append after closing should fail
	_, _, err = s.Append([]byte("more data"))
	require.Error(t, err)

	// Trying to read after closing should fail
	_, err = s.Read(0)
	require.Error(t, err)
}

// Test store and close and reopen again
func TestStoreReopen(t *testing.T) {
	f, err := os.CreateTemp("", "store_reopen_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	_, _, err = s.Append([]byte("test data"))
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	// Reopen the store
	f, err = os.OpenFile(f.Name(), os.O_RDWR, 0o666)
	require.NoError(t, err)

	s, err = newStore(f)
	require.NoError(t, err)

	readData, err := s.Read(0)
	require.NoError(t, err)
	require.Equal(t, []byte("test data"), readData)

	err = s.Close()
	require.NoError(t, err)
}

func TestNewStore(t *testing.T) {
	f, err := os.CreateTemp("", "new_store_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	require.NotNil(t, s.file)
	require.NotNil(t, s.buf)
	require.Equal(t, uint64(0), s.size)
}
