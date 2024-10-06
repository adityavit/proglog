// Create an index file that stoes the offset of the record and the position of the record in the store

package log

import (
	"encoding/binary"
	"io"
	"os"

	mmap "github.com/edsrzf/mmap-go"
)

const (
	offsetWidth   = 4
	positionWidth = 8
	entryWidth    = offsetWidth + positionWidth
)

type Index struct {
	file *os.File
	mmap mmap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*Index, error) {
	idx := &Index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	idx.size = size
	// Truncate the file to the size of the segment
	if err := os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	idx.mmap, err = mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *Index) Close() error {
	if err := i.mmap.Flush(); err != nil {
		return err
	}
	if err := i.mmap.Unmap(); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	if err := i.file.Close(); err != nil {
		return err
	}
	return nil
}

// Read takes an index offset and returns the associated record offset and position in the store
func (i *Index) Read(offset int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if offset == -1 {
		out = uint32(i.size/entryWidth) - 1
	} else {
		out = uint32(offset)
	}
	pos = uint64(out) * entryWidth
	if pos+entryWidth > uint64(i.size) {
		return 0, 0, io.EOF
	}
	out = binary.BigEndian.Uint32(i.mmap[pos : pos+offsetWidth])
	pos = binary.BigEndian.Uint64(i.mmap[pos+offsetWidth : pos+entryWidth])
	return
}

// Write takes an offset and a position and writes the record to the index
func (i *Index) Write(offset uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entryWidth {
		return io.EOF
	}
	binary.BigEndian.PutUint32(i.mmap[i.size:i.size+offsetWidth], offset)
	binary.BigEndian.PutUint64(i.mmap[i.size+offsetWidth:i.size+entryWidth], pos)
	i.size += entryWidth
	return nil
}

func (i *Index) Name() string {
	return i.file.Name()
}

func (i *Index) Size() uint64 {
	return i.size
}
