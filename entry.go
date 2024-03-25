package bitcask

import (
	"encoding/binary"
	"math"
)

const headerSize int = 4 + 2 + 2

type Header struct {
	ValueSize uint32
	KeySize   uint16
	Mark      uint16
}

func (h *Header) Size() int64 {
	return int64(headerSize)
}

type Entry struct {
	Header
	Key   []byte
	Value []byte
}

const (
	MarkDeleted uint16 = 1 << iota
)

func (e *Entry) Size() int64 {
	return int64(headerSize) + int64(e.KeySize) + int64(e.ValueSize)
}

func (e *Entry) MarkDeleted() {
	e.Mark |= MarkDeleted
}

func (e *Entry) IsDeleted() bool {
	return e.Mark&MarkDeleted != 0
}

func MakeEntry(key, value []byte) *Entry {
	return &Entry{
		Header: Header{
			ValueSize: uint32(len(value)),
			KeySize:   uint16(len(key)),
			Mark:      0,
		},
		Key:   key,
		Value: value,
	}
}

func checkKey(key []byte) error {
	if len(key) > math.MaxUint16 {
		return ErrKeyTooLarge
	}
	return nil
}

func decodeHeader(data []byte) (*Header, error) {
	if len(data) != headerSize {
		return nil, ErrHeaderCorrupted
	}
	var header Header
	header.ValueSize = binary.LittleEndian.Uint32(data[0:4])
	header.KeySize = binary.LittleEndian.Uint16(data[4:6])
	header.Mark = binary.LittleEndian.Uint16(data[6:8])
	return &header, nil
}

func encodeHeader(header Header) []byte {
	data := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(data[0:4], header.ValueSize)
	binary.LittleEndian.PutUint16(data[4:6], header.KeySize)
	binary.LittleEndian.PutUint16(data[6:8], header.Mark)
	return data
}

func encodeEntry(e *Entry) []byte {
	data := encodeHeader(e.Header)
	data = append(data, e.Key...)
	data = append(data, e.Value...)
	return data
}
