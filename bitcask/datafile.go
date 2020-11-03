package bitcask

import (
	"os"
	"time"

	"golang.org/x/exp/mmap"
)

// DataFile ... DataFile interface
type DataFile interface {
	FileID() uint32
	Sync() error
	Close() error
	Size() uint32
	ReadAt([]byte, uint32) (uint32, error)
	Write([]byte) (uint32, error)
	ModTime() time.Time
}

// datafile ... control structure
type dataFile struct {
	path   string         // file path
	fid    uint32         // file id
	fp     *os.File       // os.File
	ra     *mmap.ReaderAt // mmap ReadAt
	offset uint32         // file size
	ti     time.Time      // last modified time
}

// NewDataFile ... create new dataFile
func NewDataFile(path string, fid uint32, readonly bool) (DataFile, error) {
	var err error = nil
	df := dataFile{
		path: path,
		fid:  fid,
	}

	flags := 0
	if readonly {
		flags = os.O_RDONLY
	} else {
		flags = os.O_RDWR | os.O_APPEND | os.O_CREATE
	}

	df.fp, err = os.OpenFile(path, flags, 0640)
	if err != nil {
		return nil, err
	}

	stat, err := df.fp.Stat()
	if err != nil {
		return nil, err
	}
	df.offset = uint32(stat.Size())
	df.ti = stat.ModTime()

	if readonly {
		df.ra, err = mmap.Open(path)
		if err != nil {
			return nil, err
		}
	}

	return &df, nil
}

func (df *dataFile) FileID() uint32 {
	return df.fid
}

func (df *dataFile) Sync() error {
	if df.ra != nil {
		return nil
	}
	return df.fp.Sync()
}

func (df *dataFile) Close() error {
	if df.ra != nil {
		df.ra.Close()
	} else {
		err := df.fp.Sync()
		if err != nil {
			return err
		}
	}
	err := df.fp.Close()
	if df.offset <= 0 {
		os.Remove(df.path)
		return nil
	}
	return err
}

func (df *dataFile) Size() uint32 {
	return df.offset
}

// read mmap or from RDWR file
func (df *dataFile) ReadAt(buf []byte, offset uint32) (uint32, error) {
	if df.ra != nil {
		if df.ra == nil {
			return 0, nil
		}
		count, err := df.ra.ReadAt(buf, int64(offset))
		return uint32(count), err
	}
	_, err := df.fp.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		return 0, err
	}
	count, err := df.fp.Read(buf)
	return uint32(count), err
}

func (df *dataFile) Write(buf []byte) (uint32, error) {
	count, err := df.fp.Write(buf)
	if err != nil {
		return 0, err
	}
	df.offset += uint32(count)
	return uint32(count), err
}

func (df *dataFile) ModTime() time.Time {
	if df.ra != nil {
		return df.ti
	}
	stat, _ := df.fp.Stat()
	return stat.ModTime()
}
