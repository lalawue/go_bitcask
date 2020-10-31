package bitcask

import (
	"bytes"
	"container/list"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"reflect"
	"runtime"
	"strconv"
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
	crc32table *crc32.Table
	buckets    map[string]bucketInfo
}

// database slot
type bucketInfo struct {
	name     string                // bucket name
	actFid   uint32                // act file id
	maxFid   uint32                // max file id
	freeFids *list.List            // free file id list
	keyInfos map[string]recordInfo // key info map
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

// record size
const recordSize int = int(unsafe.Sizeof(recordInfo{}))

// OpenDB ... open database
func (b *Bitcask) OpenDB(config *Config) (*Bitcask, error) {
	validateConfig(config)
	if _, err := os.Stat(config.Path); err != nil {
		os.MkdirAll(config.Path, 0755)
	}
	b.config = config
	b.buckets = make(map[string]bucketInfo)
	b.bucketName = config.BucketName
	b.crc32table = crc32.MakeTable(crc32.Castagnoli)
	loadBucketsInfo(b)
	return b, nil
}

// CloseDB ... close database
func (b *Bitcask) CloseDB() {
	b.config = nil
	b.buckets = make(map[string]bucketInfo)
	b.bucketName = ""
	b.crc32table = nil
}

// Get ... get value with key
func (b *Bitcask) Get(key string) ([]byte, error) {
	if len(b.buckets) <= 0 || len(key) <= 0 {
		return nil, fmt.Errorf("invalid key or empty map")
	}
	bi := b.buckets[b.bucketName]
	ri, err := bi.keyInfos[key]
	if !err {
		return nil, nil
	}
	// read fid file
	if fp, err := os.Open(fidPath(b, ri.fid, b.bucketName)); err == nil {
		defer fp.Close()
		ri.offset = 0
		offset, err := fp.Seek(int64(ri.offset), os.SEEK_SET) // relative to file begin
		if uint32(offset) != ri.offset || err != nil {
			return nil, fmt.Errorf("failed to get seek")
		}
		rri, rkey, rvalue, err := readRecord(fp, true)
		if err == nil && rkey == key && rri.crc32 == crc32Bytes(b, []byte(rkey), rvalue) {
			return rvalue, nil
		}
		return nil, err
	}
	return nil, fmt.Errorf("failed to open fid file")
}

// Set ... set key, value
func (b *Bitcask) Set(key string, value []byte) error {
	if len(b.buckets) <= 0 || len(key) <= 0 || len(value) <= 0 {
		return fmt.Errorf("invalid set params or empty map")
	}
	bi := b.buckets[b.bucketName]
	// old entry exist, delete old first
	err := b.Remove(key)
	if err != nil {
		return err
	}
	// create recordinfo
	fid, offset := activeFid(b, &bi)
	ri := recordInfo{
		ti:     uint32(time.Now().Unix()),
		fid:    fid,
		offset: offset,
		ksize:  uint32(len(key)),
		vsize:  uint32(len(value)),
		crc32:  crc32Bytes(b, []byte(key), value),
	}
	// write key, value
	err = writeRecord(fidPath(b, fid, ""), &ri, &key, value)
	if err != nil {
		return err
	}
	// update bucektinfo's key/recordinfo
	bi.keyInfos[key] = ri
	return nil
}

// Remove .. delete key in database
func (b *Bitcask) Remove(key string) error {
	if len(b.buckets) <= 0 || len(key) <= 0 {
		return fmt.Errorf("invalid erase params or empty map")
	}
	bi := b.buckets[b.bucketName]
	ri, ok := bi.keyInfos[key]
	if !ok {
		return nil
	}
	ri.vsize = 0 // mark deleted, keep record's old fid and offset
	fid, _ := activeFid(b, &bi)
	err := writeRecord(fidPath(b, fid, ""), &ri, &key, nil)
	if err != nil {
		// calm to keep mem record's vsize to 0
		return err
	}
	delete(bi.keyInfos, key)
	return nil
}

/* Internal
 */

// create bucket dir and insert into _buckets
func bucketCreate(b *Bitcask, name string, maxFid uint32) *bucketInfo {
	path := b.config.Path + "/" + name
	if _, err := os.Stat(path); err != nil {
		os.Mkdir(path, 0755)
	}
	bi := bucketInfo{
		name:     name,
		actFid:   0,
		maxFid:   maxFid,
		freeFids: list.New(),
		keyInfos: make(map[string]recordInfo),
	}
	activeFid(b, &bi)
	b.buckets[name] = bi
	return &bi
}

// get file info callback
type fileInfoCallback func(finfo os.FileInfo) error

// read file info from dir
func readFileInfoInDir(dirPath string, callback fileInfoCallback) error {
	if dirFp, err := os.Open(dirPath); err == nil {
		for {
			finfos, ferr := dirFp.Readdir(16)
			if len(finfos) <= 0 {
				if ferr == io.EOF {
					break
				} else {
					return err
				}
			} else {
				for i := 0; i < len(finfos); i++ {
					err := callback(finfos[i])
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	} else {
		return err
	}
}

// load every buckets info
func loadBucketsInfo(b *Bitcask) error {
	dbPath := b.config.Path
	// check ervery bucket dir
	err := readFileInfoInDir(dbPath, func(dirInfo os.FileInfo) error {
		bucketName := dirInfo.Name()
		if dirInfo.IsDir() && !strings.HasPrefix(bucketName, ".") {
			maxFid := 0
			bi := bucketCreate(b, bucketName, 0)
			// load every bucket's key/readinfo, mxFd
			bucketPath := dbPath + "/" + bucketName + "/"
			err := readFileInfoInDir(bucketPath, func(finfo os.FileInfo) error {
				if !finfo.IsDir() {
					// remove last '.dat', record max fid
					fname := finfo.Name()
					fid, err := strconv.Atoi(fname[0 : len(fname)-4])
					if err == nil && fid > maxFid {
						maxFid = fid
					}
					// load key/recordInfo
					if fp, err := os.Open(bucketPath + fname); err == nil {
						ri, key, _, err := readRecord(fp, false)
						if err != nil {
							return err
						}
						bi.keyInfos[key] = *ri
					} else {
						return err
					}
				}
				// update max fid
				bi.maxFid = uint32(maxFid)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil && len(b.buckets) <= 0 {
		bucketCreate(b, b.bucketName, 0)
	}
	return err
}

/* Record
 */

// map recordinfo to bytes
func recordInfoToBytes(ri *recordInfo) []byte {
	var x reflect.SliceHeader
	x.Len = recordSize
	x.Cap = recordSize
	x.Data = uintptr(unsafe.Pointer(ri))
	return *(*[]byte)(unsafe.Pointer(&ri))
}

// map byte to recordinfo
func bytesToRecordInfo(b []byte) *recordInfo {
	return (*recordInfo)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&b)).Data))
}

// read record info
func readRecord(fp *os.File, readValue bool) (*recordInfo, string, []byte, error) {
	buf := make([]byte, recordSize)
	// read record info
	count, err := fp.Read(buf)
	if count != recordSize || err != nil {
		return nil, "", nil, fmt.Errorf("failed to read record")
	}
	// map to recordinfo
	ri := *bytesToRecordInfo(buf)
	// read key
	keyBuf := make([]byte, ri.ksize)
	count, err = fp.Read(keyBuf)
	if uint32(count) != ri.ksize || err != nil {
		return nil, "", nil, fmt.Errorf("failed to read key")
	}
	// try read value
	var valBuf []byte = nil
	if readValue {
		valBuf = make([]byte, ri.vsize)
		count, err = fp.Read(valBuf)
		if uint32(count) != ri.vsize || err != nil {
			return nil, "", nil, fmt.Errorf("failed to read value: %s", err.Error())
		}
	} else {
		_, err = fp.Seek(int64(ri.vsize), os.SEEK_CUR)
		if err != nil {
			return nil, "", nil, fmt.Errorf("failed to skip value: %s", err.Error())
		}
	}
	return &ri, string(keyBuf), valBuf, nil
}

// write record info
func writeRecord(fidPath string, ri *recordInfo, key *string, value []byte) error {
	// open/append file pointer
	var fp *os.File = nil
	var err error = nil
	if _, err = os.Stat(fidPath); err == nil {
		fp, err = os.OpenFile(fidPath, os.O_WRONLY|os.O_APPEND, 0755)
		if err != nil {
			return fmt.Errorf("failed to openFile: %s", err.Error())
		}
	} else {
		if fp, err = os.Create(fidPath); err != nil {
			return fmt.Errorf("failed to create set fid file: %s", err.Error())
		}
	}
	defer fp.Close()
	// write record
	buf := recordInfoToBytes(ri)
	count, err := fp.Write(buf[:recordSize])
	if count != recordSize || err != nil {
		return fmt.Errorf("failed to write record: %s", err.Error())
	}
	count, err = fp.Write([]byte(*key))
	if count != len(*key) || err != nil {
		return fmt.Errorf("failed to write record key: %s", err.Error())
	}
	if value != nil {
		count, err = fp.Write(value)
		if count != len(value) || err != nil {
			return fmt.Errorf("failed to write record value: %s", err.Error())
		}
	}
	fp.Sync()
	return nil
}

/* File Info
 */

// fid name with leading '0'
func indexString(fid uint32) string {
	fidString := fmt.Sprintf("%d", fid)
	return strings.Repeat("0", 10-len(fidString)) + fidString
}

// fid to real file id name
func fidPath(b *Bitcask, fid uint32, bucketName string) string {
	if len(bucketName) <= 0 {
		bucketName = b.bucketName
	}
	return fmt.Sprintf("%s/%s/%s.dat", b.config.Path, bucketName, indexString(fid))
}

// get next available fid from free list or increase max fid
func nextEmptyFid(b *Bitcask, bucket *bucketInfo) uint32 {
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
func activeFid(b *Bitcask, bucket *bucketInfo) (uint32, uint32) {
	actFid := bucket.actFid
	var offset uint32 = 0
	for {
		info, err := os.Stat(fidPath(b, actFid, bucket.name))
		if err != nil {
			break
		}
		if uint32(info.Size()) >= b.config.DataFileSize {
			if actFid != bucket.maxFid {
				actFid = bucket.maxFid
			} else {
				actFid = nextEmptyFid(b, bucket)
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
func validateConfig(config *Config) {
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

// calculate crc32 from multiple []byte slice
func crc32Bytes(b *Bitcask, pBytes ...[]byte) uint32 {
	plen := len(pBytes)
	sBytes := make([][]byte, plen)
	for i := 0; i < plen; i++ {
		sBytes[i] = pBytes[i]
	}
	jBytes := bytes.Join(sBytes, nil)
	return crc32.Checksum(jBytes, b.crc32table)
}
