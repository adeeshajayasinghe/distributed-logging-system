package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// Encoding use to record sizes and index entries
var (
	enc = binary.BigEndian
)

// Number of bytes used to store the record's length
const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu sync.Mutex
	buf *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fileInfo, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fileInfo.Size())
	return &store{
		File: f,
		size: size,
		buf: bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	// We write to the buffer instead of directly to the file to reduce the number of sys calls and to improve performance
	// We write the length of the record because when we read the record we know how many bytes to read
	if err = binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// w is the number of bytes written
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	// Use position when creating the index entry in the index to find this record
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Since we are writing to the buffer (not directly to the file), flushing writes any buffered data to the 
	// underline write
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	// Read the length of the record
	// and store the value in the input array "size"
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	// Read the record data
	// and store the value in the input array "b"
	if _, err := s.File.ReadAt(b, int64(pos + lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Store any buffered data to the disk before closing the file
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}