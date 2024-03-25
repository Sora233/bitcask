package bitcask

import (
	"errors"
	"io"
	"os"
)

type dbFile struct {
	f      *os.File
	offset int64
}

func openDBFile(path string) (*dbFile, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, withErr(err)
	}
	offset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, withErr(err)
	}
	return &dbFile{
		f:      f,
		offset: offset,
	}, nil
}

func (f *dbFile) Close() error {
	return f.f.Close()
}

func (f *dbFile) iter(yield func(*Entry, int64) bool) error {
	cur, err := f.f.Seek(0, io.SeekStart)
	if err != nil {
		return withErr(err)
	}
	var e *Entry
	for {
		e, err = f.ReadEntry(cur)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return withErr(err)
		}
		if !yield(e, cur) {
			break
		}
		cur += e.Size()
	}
	_, err = f.f.Seek(0, io.SeekEnd)
	return err
}

func (f *dbFile) ReadEntry(offset int64) (*Entry, error) {
	header, err := f.ReadHeader(offset)
	if err != nil {
		return nil, err
	}
	data := make([]byte, int(uint32(header.KeySize)+header.ValueSize))
	_, err = f.f.ReadAt(data, offset+int64(headerSize))
	if errors.Is(err, io.EOF) {
		return nil, ErrEntryCorrupted
	} else if err != nil {
		return nil, withErr(err)
	}
	return &Entry{
		Header: *header,
		Key:    data[:header.KeySize],
		Value:  data[header.KeySize:],
	}, nil
}

func (f *dbFile) ReadHeader(offset int64) (*Header, error) {
	var hb = make([]byte, headerSize)
	_, err := f.f.ReadAt(hb, offset)
	if errors.Is(err, io.EOF) {
		return nil, err
	} else if err != nil {
		return nil, withErr(err)
	}
	return decodeHeader(hb)
}

func (f *dbFile) WriteEntry(e *Entry) (int64, error) {
	data := encodeEntry(e)
	size, err := f.f.Write(data)
	if err != nil {
		return 0, withErr(err)
	}
	offset := f.offset
	f.offset += int64(size)
	return offset, nil
}
