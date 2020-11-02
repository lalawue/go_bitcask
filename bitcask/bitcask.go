package bitcask

import (
	"bytes"
	"container/list"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"time"
	"unsafe"
)

// Bitcask ... bitcask database interface
type Bitcask interface {
	CloseDB()
	ChangeBucket(name string) error
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Remove(key string) error
	GC(name string) error
}

// Config ... database config
type Config struct {
	Path         string // default '/tmp/go_bitcask'
	BucketName   string // default '0'
	DataFileSize uint32 // default 64MB
}

// bitcask ... bitcask database control unit
type bitcask struct {
	config     *Config // config
	bucketName string  // current active bucket
	crc32table *crc32.Table
	buckets    map[string]bucketInfo
}

// database bucket slot
type bucketInfo struct {
	name     string                // bucket name
	actFid   uint32                // act file id
	maxFid   uint32                // max file id
	freeFids *list.List            // free file id list
	keyInfos map[string]recordInfo // key info map
	df       DataFile              // act file pointer
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
const recordSize uint32 = uint32(unsafe.Sizeof(recordInfo{}))

// OpenDB ... open database
func OpenDB(config *Config) (Bitcask, error) {
	b := bitcask{}
	validateConfig(config)
	if _, err := os.Stat(config.Path); err != nil {
		os.MkdirAll(config.Path, 0755)
	}
	b.config = config
	b.buckets = make(map[string]bucketInfo)
	b.bucketName = config.BucketName
	b.crc32table = crc32.MakeTable(crc32.IEEE)
	// load bucket and key/recordInfo
	err := loadBucketsInfo(&b)
	if err != nil {
		return nil, err
	}
	err = loadKeysInfo(&b)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// CloseDB ... close database
func (b *bitcask) CloseDB() {
	bi := b.buckets[b.bucketName]
	if bi.df != nil {
		bi.df.Close()
	}
	b.config = nil
	b.buckets = make(map[string]bucketInfo)
	b.bucketName = ""
	b.crc32table = nil
}

// ChangeBucket ... change active bucket
func (b *bitcask) ChangeBucket(name string) error {
	if len(b.buckets) <= 0 || len(name) <= 0 {
		return fmt.Errorf("invalid bucket name or empty map")
	}
	bi := b.buckets[b.bucketName]
	if bi.df != nil {
		bi.df.Sync()
	}
	_, ok := b.buckets[name]
	if !ok {
		b.createBucket(name, 0)
	}
	b.bucketName = name
	return nil
}

// Get ... get value with key
func (b *bitcask) Get(key string) ([]byte, error) {
	if len(b.buckets) <= 0 || len(key) <= 0 {
		return nil, fmt.Errorf("invalid key or empty map")
	}
	bi := b.buckets[b.bucketName]
	ri, ok := bi.keyInfos[key]
	if !ok {
		return nil, nil
	}
	// read act fid file
	var df DataFile = nil
	var err error = nil
	if bi.df != nil && bi.df.FileID() == ri.fid {
		df = bi.df
	} else {
		df, err = NewDataFile(b.fidPath(ri.fid, bi.name), ri.fid, true)
		if err == nil {
			defer df.Close()
		}
	}
	// read fid file
	if err == nil {
		rri, rkey, rvalue, err := bi.readRecord(df, ri.offset, true)
		if err == nil && rkey == key && rri.crc32 == crc32.Checksum(joinBytes([]byte(rkey), rvalue), b.crc32table) {
			return rvalue, nil
		}
		return nil, err
	}
	return nil, fmt.Errorf("failed to get fid file: %s", err.Error())
}

// Set ... set key, value
func (b *bitcask) Set(key string, value []byte) error {
	if len(b.buckets) <= 0 || len(key) <= 0 || len(value) <= 0 {
		return fmt.Errorf("invalid set params or empty map")
	}
	bi := b.buckets[b.bucketName]
	defer func() {
		b.buckets[b.bucketName] = bi
	}()
	// old entry exist, delete old first
	err := b.Remove(key)
	if err != nil {
		return err
	}
	// create recordinfo
	fid, offset := bi.activeFid(b)
	ri := recordInfo{
		ti:     uint32(time.Now().Unix()),
		fid:    fid,
		offset: offset,
		ksize:  uint32(len(key)),
		vsize:  uint32(len(value)),
		crc32:  crc32.Checksum(joinBytes([]byte(key), value), b.crc32table),
	}
	// write key, value
	err = bi.writeRecord(b.fidPath(fid, ""), fid, &ri, key, value)
	if err != nil {
		return err
	}
	// update bucektinfo's key/recordinfo
	bi.keyInfos[key] = ri
	return nil
}

// Remove .. delete key in database
func (b *bitcask) Remove(key string) error {
	if len(b.buckets) <= 0 || len(key) <= 0 {
		return fmt.Errorf("invalid erase params or empty map")
	}
	bi := b.buckets[b.bucketName]
	defer func() {
		b.buckets[b.bucketName] = bi
	}()
	ri, ok := bi.keyInfos[key]
	if !ok {
		return nil
	}
	ri.vsize = 0 // mark deleted, keep record's old fid and offset
	fid, _ := bi.activeFid(b)
	err := bi.writeRecord(b.fidPath(fid, ""), fid, &ri, key, nil)
	if err != nil {
		// calm to keep mem record's vsize to 0
		return err
	}
	delete(bi.keyInfos, key)
	return nil
}

// GC ... run gabage collection in bucket
func (b *bitcask) GC(name string) error {
	if len(name) <= 0 {
		name = b.bucketName
	}
	bi, ok := b.buckets[name]
	if !ok {
		return fmt.Errorf("invalid bucket name")
	}
	defer func() {
		b.buckets[name] = bi
	}()
	// collect delete/mark record info
	rmMapList := make(map[uint32]*list.List)
	err := collectDeletedRecordInfos(b, &bi, rmMapList)
	if err != nil {
		return err
	}
	// if not need gc
	if len(rmMapList) > 0 {
		err = mergeRecordInfos(b, &bi, rmMapList)
		if err != nil {
			return err
		}
	}
	// update last GC time
	bi.writeBucketInfo(b, name)
	bi.df.Sync()
	return nil
}

/* Bucket
 */

// create bucket dir and insert into _buckets
func (b *bitcask) createBucket(name string, maxFid uint32) *bucketInfo {
	path := filepath.Join(b.config.Path, name)
	if _, err := os.Stat(path); err != nil {
		os.Mkdir(path, 0750)
	}
	bi := bucketInfo{
		name:     name,
		actFid:   0,
		maxFid:   maxFid,
		freeFids: list.New(),
		keyInfos: make(map[string]recordInfo),
	}
	b.buckets[name] = bi
	return &bi
}

// get file info callback
type fileInfoCallback func(finfo os.FileInfo) error

// read file info from dir
func readFileInfoInDir(dirPath string, callback fileInfoCallback) error {
	var dirFp *os.File = nil
	var err error = nil
	if dirFp, err = os.Open(dirPath); err == nil {
		defer dirFp.Close()
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
	}
	return err
}

// load every buckets info
func loadBucketsInfo(b *bitcask) error {
	dbPath := b.config.Path
	// check ervery bucket dir
	err := readFileInfoInDir(dbPath, func(dirInfo os.FileInfo) error {
		bucketName := dirInfo.Name()
		if dirInfo.IsDir() {
			maxFid := 0
			// load every bucket's key/readinfo, mxFd
			err := readFileInfoInDir(filepath.Join(dbPath, bucketName), func(finfo os.FileInfo) error {
				if finfo.IsDir() {
					return nil
				}
				// remove last '.dat', record max fid
				fname := finfo.Name()
				fid, err := strconv.Atoi(fname[0 : len(fname)-4])
				if err == nil && fid > maxFid {
					maxFid = fid
				}
				// update max fid
				b.createBucket(bucketName, uint32(maxFid))
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil && len(b.buckets) <= 0 {
		b.createBucket(b.bucketName, 0)
	}
	return err
}

// load key infos
func loadKeysInfo(b *bitcask) error {
	for bucketName, bi := range b.buckets {
		var fid uint32 = 0
		var err error = nil
		var df DataFile = nil
		for fid = 0; fid <= bi.maxFid; fid++ {
			if df, err = NewDataFile(b.fidPath(fid, bucketName), fid, true); err == nil {
				var offset uint32 = 0
				for {
					ri, key, _, err := bi.readRecord(df, offset, false)
					if err != nil {
						break
					}
					if ri.vsize > 0 {
						bi.keyInfos[key] = *ri
					} else {
						delete(bi.keyInfos, key)
					}
					offset = offset + recordSize + ri.ksize + ri.vsize
				}
				df.Close()
			} else if fid < bi.maxFid {
				bi.freeFids.PushBack(fid)
			}
		}
		if err != nil && err != io.EOF && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// read last gc time
func (bi *bucketInfo) readBucketInfo(b *bitcask, name string) (int64, error) {
	infoPath := filepath.Join(b.config.Path, name+".info")
	var buf []byte = nil
	var err error = nil
	if buf, err = ioutil.ReadFile(infoPath); err == nil {
		lastTime, err := strconv.ParseInt(string(buf), 10, 64)
		if err != nil {
			return 0, err
		}
		return lastTime, nil
	}
	return 0, err
}

// write last gc time
func (bi *bucketInfo) writeBucketInfo(b *bitcask, name string) error {
	infoPath := filepath.Join(b.config.Path, name+".info")
	bufString := fmt.Sprintf("%d", time.Now().Unix())
	return ioutil.WriteFile(infoPath, []byte(bufString), 0644)
}

/* Record
 */

// map recordinfo to bytes
func recordInfoToBytes(ri *recordInfo) []byte {
	var x reflect.SliceHeader
	x.Len = int(recordSize)
	x.Cap = int(recordSize)
	x.Data = uintptr(unsafe.Pointer(ri))
	retBytes := *(*[]byte)(unsafe.Pointer(&ri))
	return retBytes[:recordSize]
}

// map byte to recordinfo
func bytesToRecordInfo(b []byte) *recordInfo {
	return (*recordInfo)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&b)).Data))
}

// read record info
func (bi *bucketInfo) readRecord(fp DataFile, offset uint32, readValue bool) (*recordInfo, string, []byte, error) {
	buf := make([]byte, recordSize)
	// read record info
	count, err := fp.ReadAt(buf, offset)
	if count != recordSize || err != nil {
		return nil, "", nil, err
	}
	// map to recordinfo
	ri := *bytesToRecordInfo(buf)
	// read key
	kvLen := ri.ksize
	if readValue {
		kvLen += ri.vsize
	}
	kvBuf := make([]byte, kvLen)
	count, err = fp.ReadAt(kvBuf, offset+recordSize)
	if uint32(count) != kvLen || err != nil {
		return nil, "", nil, fmt.Errorf("failed to read record")
	}
	// try read value
	var keyBuf []byte = kvBuf[:ri.ksize]
	var valBuf []byte = nil
	if readValue {
		valBuf = kvBuf[ri.ksize:]
	}
	return &ri, string(keyBuf), valBuf, nil
}

// write record info, bi.df was opened in activeFid()
func (bi *bucketInfo) writeRecord(fidPath string, fid uint32, ri *recordInfo, key string, value []byte) error {
	count, err := bi.df.Write(joinBytes(recordInfoToBytes(ri), []byte(key), value))
	if int(count) != (int(recordSize)+len(key)+len(value)) || err != nil {
		return fmt.Errorf("failed to write record: %s", err.Error())
	}
	return nil
}

/* Active File
 */

// fid to real file id name
func (b *bitcask) fidPath(fid uint32, bucketName string) string {
	if len(bucketName) <= 0 {
		bucketName = b.bucketName
	}
	return fmt.Sprintf("%s/%s/%09d.dat", b.config.Path, bucketName, fid)
}

// get next available fid from free list or increase max fid for GC
func (bi *bucketInfo) nextActFid(b *bitcask) {
	if bi.df != nil {
		bi.df.Close()
	}
	if bi.freeFids.Len() > 0 {
		fidElement := bi.freeFids.Back()
		bi.actFid = fidElement.Value.(uint32)
		bi.freeFids.Remove(fidElement)
	} else {
		bi.actFid = bi.maxFid + 1
		bi.maxFid = bi.actFid
	}
	bi.df, _ = NewDataFile(b.fidPath(bi.actFid, bi.name), bi.actFid, false)
}

// set available active fid, order was act/free/max/max+1
func (bi *bucketInfo) activeFid(b *bitcask) (uint32, uint32) {
	var dfSize uint32 = 0
	// try current act fid
	if bi.df != nil {
		dfSize = bi.df.Size()
		if dfSize < b.config.DataFileSize {
			return bi.actFid, dfSize
		}
		bi.df.Close()
	}
	// try free
	if bi.freeFids.Len() > 0 {
		fidElement := bi.freeFids.Front()
		bi.actFid = fidElement.Value.(uint32)
		bi.freeFids.Remove(fidElement)
		bi.df, _ = NewDataFile(b.fidPath(bi.actFid, bi.name), bi.actFid, false)
		if bi.df != nil {
			dfSize = bi.df.Size()
			if dfSize < b.config.DataFileSize {
				return bi.actFid, dfSize
			}
			bi.df.Close()
		}
	}
	// try max fid, or max + 1
	for {
		bi.df, _ = NewDataFile(b.fidPath(bi.maxFid, bi.name), bi.maxFid, false)
		if bi.df != nil {
			dfSize = bi.df.Size()
			if dfSize < b.config.DataFileSize {
				bi.actFid = bi.maxFid
				return bi.actFid, dfSize
			}
			bi.df.Close()
		}
		bi.maxFid++
	}
}

/* GC
 */

// collect delete/mark record info
func collectDeletedRecordInfos(b *bitcask, bi *bucketInfo, rmTable map[uint32]*list.List) error {
	// get last gc time
	lastSecond, _ := bi.readBucketInfo(b, bi.name)
	lastTime := time.Unix(lastSecond, 0)
	var fid uint32 = 0
	for fid = 0; fid <= bi.maxFid; fid++ {
		df, inErr := NewDataFile(b.fidPath(fid, bi.name), fid, true)
		if inErr != nil {
			continue
		}
		// only collect modified later than last gc
		if df.ModTime().Before(lastTime) {
			df.Close()
			continue
		}
		var offset uint32 = 0
		for {
			rmri, _, _, err := bi.readRecord(df, offset, false)
			if err != nil {
				inErr = err
				break
			} else if rmri.vsize == 0 {
				tableList := func(fid uint32) *list.List {
					lst, _ := rmTable[fid]
					if lst == nil {
						rmTable[fid] = &list.List{}
						lst = rmTable[fid]
					}
					return lst
				}
				// insert delete record
				tableList(rmri.fid).PushBack(recordInfo{
					ti:     rmri.ti,
					fid:    rmri.fid,
					offset: rmri.offset,
					ksize:  rmri.ksize,
					vsize:  rmri.vsize,
					crc32:  rmri.crc32,
				})
				// insert mark record
				rmri.fid = fid
				rmri.offset = offset
				tableList(rmri.fid).PushBack(*rmri)
			}
			offset = offset + recordSize + rmri.ksize + rmri.vsize
		}
		df.Close()
		if inErr != nil && inErr != io.EOF {
			return inErr
		}
	}
	return nil
}

// merge non delete/mark record into new data file
func mergeRecordInfos(b *bitcask, bi *bucketInfo, rmMapList map[uint32]*list.List) error {
	// check fid, offset in array
	isInList := func(lst *list.List, fid uint32, offset uint32) bool {
		for item := lst.Front(); item != nil; item = item.Next() {
			ri := item.Value.(recordInfo)
			if ri.fid == fid && ri.offset == offset {
				lst.Remove(item)
				return true
			}
		}
		return false
	}
	// first increase active fid
	bi.nextActFid(b)
	keyInfos := bi.keyInfos
	for inFid, lst := range rmMapList {
		inPath := b.fidPath(inFid, bi.name)
		hasSkip := false
		if inDf, inErr := NewDataFile(inPath, inFid, true); inErr == nil {
			var inOffset uint32 = 0
			for {
				inri, inkey, invalue, err := bi.readRecord(inDf, inOffset, true)
				if err != nil {
					inErr = err
					break
				}
				if isInList(lst, inFid, uint32(inOffset)) {
					// mark delete
					hasSkip = true
				} else {
					// update key/recordInfo
					inri.fid, inri.offset = bi.activeFid(b)
					keyInfos[inkey] = *inri
					// write to active fid
					err := bi.writeRecord(b.fidPath(inri.fid, bi.name), inri.fid, inri, inkey, invalue)
					if err != nil {
						inErr = err
						break
					}
				}
				inOffset = inOffset + recordSize + inri.ksize + inri.vsize
			}
			inDf.Close()
			if inErr != nil && inErr != io.EOF {
				return inErr
			}
		} else {
			// failed to open file
			return inErr
		}
		if hasSkip {
			os.Remove(inPath)
		}
		bi.freeFids.PushBack(inFid)
	}
	return nil
}

/* Helper
 */

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

// join bytes into one
func joinBytes(pBytes ...[]byte) []byte {
	plen := len(pBytes)
	sBytes := make([][]byte, plen)
	for i := 0; i < plen; i++ {
		sBytes[i] = pBytes[i]
	}
	return bytes.Join(sBytes, nil)
}
