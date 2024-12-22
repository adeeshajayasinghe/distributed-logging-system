package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	// Define number of bytes that takes from the index file of each entry
	offWidth uint64 = 4
	posWidth uint64 = 8
	// Can jump straight into the position using offset * entWidth
	entWidth uint64 = offWidth + posWidth
)

type index struct {
	file *os.File
	// Memory mapped file
	mmap gommap.MMap
	// size of the inded and where to write the next entry
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{file: f,}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())
	// Grow the file to the max index size since closing the file remove the empty space
	// Need to resize before memory mapped as we cannot resize once we memory maped 
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	// Maps the file into the memory avoiding copying of data between the disk and user space. 
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(), 
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {
	// Flushing the changes to the mmap to the actual file
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	// Flushing the file content to the disk
	if err := i.file.Sync(); err != nil {
		return err
	}
	// Truncate the file to the amount that's actually in it
	// Removing the empty space between the last entry and the end of the file is required otherwise service not restart properly
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return nil
}

// Takes in and offset and returns the associalted position in the store file
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos  = uint64(out) * entWidth
	if i.size < pos + entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error{
	if uint64(len(i.mmap)) < i.size + entWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size : i.size + offWidth], off)
	enc.PutUint64(i.mmap[i.size + offWidth : i.size + entWidth], pos)
	// Incremenet the size for the next future entry
	i.size += uint64(entWidth)
	return nil
}

// Get index's file path
func (i *index) Name() string {
	return i.file.Name()
}


