package bitcask

import (
	"container/list"
	"fmt"
	"os"
)

// Bitcask ... bitcask db struct
type Bitcask struct {
	path    string
	buckets map[string]bucket
}

// bucket ... database slot
type bucket struct {
	actFid  uint32             // act file id
	maxFid  uint32             // max file id
	freeIds *list.List         // free file id list
	kinfos  map[string]keyInfo // key info map
}

// keyInfo ... keyinfo
type keyInfo struct {
	ti    uint32 // time
	fid   uint32 // file name id
	ksize uint32 // key string size
	vsize uint32 // value data size
	crc32 uint32 // crc32
}

// OpenDB ... open database
func (b *Bitcask) OpenDB(path string) (*Bitcask, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("invalid path")
	}
	b.path = path
	return b, nil
}

/* Internal
 */

//func _bucketCreate()
