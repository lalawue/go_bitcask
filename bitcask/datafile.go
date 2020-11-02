package bitcask

import (
	"os"
	"time"

	"golang.org/x/exp/mmap"
)

// DataFile ... DataFile interface
type DataFile interface {
	FileID() uint32
	Close() error
	Sync() error
	Size() uint32
	ReadAt([]byte, uint32) (uint32, error)
	Write([]byte) (uint32, error)
	ModTime() time.Time
	Seek(uint32) error
}

type dataFile struct {
	fid    uint32
	fp     *os.File
	ra     *mmap.ReaderAt
	offset uint32
	ti     time.Time
}

// NewDataFile ... create new dataFile
func NewDataFile(path string, fid uint32, readonly bool) (DataFile, error) {
	var err error = nil
	df := dataFile{}
	df.fid = fid

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

func (df *dataFile) Close() error {
	if df.ra != nil {
		df.ra.Close()
	} else {
		err := df.fp.Sync()
		if err != nil {
			return err
		}
	}
	return df.fp.Close()
}

func (df *dataFile) Sync() error {
	if df.ra != nil {
		return nil
	}
	return df.fp.Sync()
}

func (df *dataFile) Size() uint32 {
	return df.offset
}

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

func (df *dataFile) Seek(offset uint32) error {
	_, err := df.fp.Seek(int64(offset), os.SEEK_SET)
	return err
}
