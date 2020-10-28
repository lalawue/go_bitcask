package bitcask

import (
	"container/list"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
	"time"
	"unsafe"
)

// Config ... database config
type Config struct {
	Path         string // default '/tmp/go_bitcask'
	BucketName   string // default '0'
	DataFileSize uint32 // default 64MB
}

// Bitcask ... bitcask database
type Bitcask struct {
	config     *Config // config
	bucketName string  // current active bucket
	buckets    map[string]bucketInfo
}

// database slot
type bucketInfo struct {
	name     string                // bucket name
	actFid   uint32                // act file id
	maxFid   uint32                // max file id
	freeFids *list.List            // free file id list
	keyInfos map[string]recordInfo // key info map
	fp       *os.File              // opened active file
	buf      []byte                // reader cache
}

// record fixed entry
type recordInfo struct {
	ti     uint32 // time
	fid    uint32 // file name id
	offset uint32 // file offset
	ksize  uint32 // key string size
	vsize  uint32 // value data size
	crc32  uint32 // crc32
	// key content
	// value content
}

const _recordSize int = int(unsafe.Sizeof(recordInfo{}))

// OpenDB ... open database
func (b *Bitcask) OpenDB(config *Config) (*Bitcask, error) {
	_validateConfig(config)
	if _, err := os.Stat(config.Path); err != nil {
		os.MkdirAll(config.Path, 0755)
	}
	b.config = config
	b.buckets = make(map[string]bucketInfo)
	b.bucketName = config.BucketName
	b._bucketCreate(config.BucketName)
	return b, nil
}

// Get ... get value with key
func (b *Bitcask) Get(key string) ([]byte, error) {
	if len(key) <= 0 {
		return nil, fmt.Errorf("invalid key")
	}
	bi := b.buckets[b.bucketName]
	ri, err := bi.keyInfos[key]
	if !err {
		return nil, nil
	}
	// read fid file
	if fp, err := os.Open(b._fidPath(ri.fid, b.bucketName)); err == nil {
		defer fp.Close()
		ri.offset = 0
		offset, err := fp.Seek(int64(ri.offset), os.SEEK_SET) // relative to file begin
		if uint32(offset) != ri.offset || err != nil {
			return nil, fmt.Errorf("failed to get seek")
		}
		rri, keyStr, value, err := bi._readRecord(fp, true)
		if err == nil && *keyStr == key && rri.crc32 == ri.crc32 {
			return value, nil
		}
		return nil, err
	}
	return nil, fmt.Errorf("failed to open fid file")
}

// Set ... set key, value
func (b *Bitcask) Set(key string, value []byte) error {
	if len(key) <= 0 || len(value) <= 0 {
		return fmt.Errorf("invalid set params")
	}
	bi := b.buckets[b.bucketName]
	oldActFid := bi.actFid
	fid, offset := b._activeFid(bi)
	// create recordinfo
	ri := recordInfo{
		ti:     uint32(time.Now().Unix()),
		fid:    fid,
		offset: offset,
		ksize:  uint32(len(key)),
		vsize:  uint32(len(value)),
		crc32:  0,
	}
	// try close/open active fid file
	if bi.fp == nil || oldActFid != bi.actFid {
		if bi.fp != nil {
			bi.fp.Close()
		}
		fidPath := b._fidPath(bi.actFid, bi.name)
		if _, err := os.Stat(fidPath); err == nil {
			bi.fp, err = os.OpenFile(fidPath, os.O_WRONLY|os.O_APPEND, 0755)
			if err != nil {
				return err
			}
		} else {
			if fp, err := os.Create(fidPath); err == nil {
				bi.fp = fp
			} else {
				return fmt.Errorf("failed to create set fid file")
			}
		}
	}
	// FIXME: erase key first
	// write key, value
	err := bi._writeRecord(&ri, &key, value)
	if err != nil {
		return err
	}
	// update bucektinfo's key/recordinfo
	bi.keyInfos[key] = ri
	return nil
}

// Erase .. erase key in database
func (b *Bitcask) Erase(key string) error {
	if len(key) <= 0 {
		return fmt.Errorf("invalid erase params")
	}
	bi := b.buckets[b.bucketName]
	_, ok := bi.keyInfos[key]
	if !ok {
		return fmt.Errorf("key not exist")
	}
	// FIXME: insert erase ri info
	delete(bi.keyInfos, key)
	return nil
}

/* Internal
 */

// create bucket dir and insert into _buckets
func (b *Bitcask) _bucketCreate(name string) {
	path := b.config.Path + "/" + name
	if _, err := os.Stat(path); err != nil {
		os.Mkdir(path, 0755)
	}
	b.buckets[name] = bucketInfo{
		name:     name,
		actFid:   0,
		maxFid:   0,
		freeFids: list.New(),
		keyInfos: make(map[string]recordInfo),
		buf:      make([]byte, 64*1024),
	}
}

/* Record
 */

// map recordinfo to bytes
func _recordInfoToBytes(ri *recordInfo) []byte {
	var x reflect.SliceHeader
	x.Len = _recordSize
	x.Cap = _recordSize
	x.Data = uintptr(unsafe.Pointer(ri))
	return *(*[]byte)(unsafe.Pointer(&ri))
}

// map byte to recordinfo
func _bytesToRecordInfo(b []byte) *recordInfo {
	return (*recordInfo)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&b)).Data))
}

// read record info
func (bi *bucketInfo) _readRecord(fp *os.File, readValue bool) (*recordInfo, *string, []byte, error) {
	buf := bi.buf[:_recordSize]
	// read record info
	count, err := fp.Read(buf)
	if count != _recordSize || err != nil {
		return nil, nil, nil, fmt.Errorf("failed to read record")
	}
	// map to recordinfo
	ri := *_bytesToRecordInfo(buf)
	// read key
	keyBuf := make([]byte, ri.ksize)
	count, err = fp.Read(keyBuf)
	if uint32(count) != ri.ksize || err != nil {
		return nil, nil, nil, fmt.Errorf("failed to read key")
	}
	// try read value
	var valBuf []byte = nil
	if readValue {
		valBuf = make([]byte, ri.vsize)
		count, err = fp.Read(valBuf)
		if uint32(count) != ri.vsize || err != nil {
			return nil, nil, nil, fmt.Errorf("failed to read value")
		}
	}
	keyString := string(keyBuf)
	return &ri, &keyString, valBuf, nil
}

// write record info
func (bi *bucketInfo) _writeRecord(ri *recordInfo, key *string, value []byte) error {
	//fmt.Println(bi.fp)
	buf := _recordInfoToBytes(ri)
	count, err := bi.fp.Write(buf[:_recordSize])
	if count != _recordSize || err != nil {
		return err
	}
	count, err = bi.fp.Write([]byte(*key))
	if count != len(*key) || err != nil {
		return fmt.Errorf("failed to write record key")
	}
	count, err = bi.fp.Write(value)
	if count != len(value) || err != nil {
		return fmt.Errorf("failed to write record value")
	}
	bi.fp.Sync()
	return nil
}

/* File Info
 */

// fid name with leading '0'
func _indexString(fid uint32) string {
	fidString := fmt.Sprintf("%d", fid)
	return strings.Repeat("0", 10-len(fidString)) + fidString
}

// fid to real file id name
func (b *Bitcask) _fidPath(fid uint32, bucketName string) string {
	return fmt.Sprintf("%s/%s/%s.dat", b.config.Path, bucketName, _indexString(fid))
}

// get next available fid from free list or increase max fid
func (b *Bitcask) _nextEmptyFid(bucket bucketInfo) uint32 {
	if bucket.freeFids.Len() > 0 {
		fidElement := bucket.freeFids.Back()
		bucket.actFid = fidElement.Value.(uint32)
		bucket.freeFids.Remove(fidElement)
	} else {
		bucket.actFid = bucket.maxFid + 1
		bucket.maxFid = bucket.actFid
	}
	return bucket.actFid
}

// get current/next active fid
func (b *Bitcask) _activeFid(bucket bucketInfo) (uint32, uint32) {
	actFid := bucket.actFid
	var offset uint32 = 0
	for {
		info, err := os.Stat(b._fidPath(actFid, bucket.name))
		if err != nil {
			break
		}
		if uint32(info.Size()) >= b.config.DataFileSize {
			if actFid != bucket.maxFid {
				actFid = bucket.maxFid
			} else {
				actFid = b._nextEmptyFid(bucket)
			}
		} else {
			offset = uint32(info.Size())
			bucket.actFid = actFid
			break
		}
	}
	return actFid, offset
}

// validate/create config
func _validateConfig(config *Config) {
	if config == nil {
		config = &Config{}
	}
	if len(config.Path) <= 0 {
		if runtime.GOOS == "windows" {
			config.Path = os.Getenv("TEMP") + "/go_bitcask"
		} else {
			config.Path = "/tmp/go_bitcask"
		}
	}
	if config.DataFileSize <= 0 {
		config.DataFileSize = 64 * 1024 * 1024
	}
	if len(config.BucketName) <= 0 {
		config.BucketName = "0"
	}
}
