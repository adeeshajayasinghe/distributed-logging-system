package log

import (
	"fmt"
	"log"
	"os"
	"path"

	api "github.com/adeeshajayasinghe/distributed-logging-system/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store *store
	index *index
	// baseOffset for get the relative offset of each segment
	// nextOffset use to know what offset to append new records  
	// both are absolute values not relative
	baseOffset, nextOffset uint64
	// To configure limits of store and index files, which let us know when the segment is maxed out
	config Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config: c,
	}
	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		// O_CREATE will create a new file if doesn't exist
		os.O_RDWR | os.O_CREATE | os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	log.Println("----block1")
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR | os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		// When the index file is empty
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur

	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	if err = s.index.Write(
		// Index offsets are relative to the base offset
		uint32(s.nextOffset - uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}

	s.nextOffset++
	return cur, nil
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, nil
}

// Checks whether the segment has reached its max size
// Need this to know it needs to create a new segment
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || 
		s.index.size >= s.config.Segment.MaxIndexBytes ||
		s.index.IsMaxed()
}

// Closes the segment and removes the index and store files
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		log.Println("---------------Error 1")
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		log.Println("---------------Error 2")
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		log.Println("---------------Error 3")
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		log.Println("----------Error in index")
		return err
	}
	if err := s.store.Close(); err != nil {
		log.Println("----------Error in store")
		return err
	}
	return nil
}

// Returns the nearest and lesser multiple of k in j
// for example nearestMultiple(9, 4) == 8. 
// We take the lesser multiple to make sure we stay under the user’s disk capacity.
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}

