package tsdb

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

type encodingBuf struct {
	B []byte
	C [binary.MaxVarintLen64]byte
}

type decodingBuf struct {
	err error
}

var (
	InvalidSizeError = errors.New("invalid size")
)

func newDecodingBuf() *decodingBuf {
	return &decodingBuf{}
}

func newEncodingBuf() *encodingBuf {
	return &encodingBuf{}
}

func (e *encodingBuf) Reset() {
	e.B = e.B[:0]
}

func (e *encodingBuf) Bytes() []byte {
	return e.B
}

func (e *encodingBuf) Len() int {
	return len(e.B)
}

func (e *encodingBuf) MarshalUint8(b uint8) {
	e.B = append(e.B, b)
}

func (e *encodingBuf) MarshalUint16(bytes ...uint16) {
	for _, b := range bytes {
		binary.LittleEndian.PutUint16(e.C[:], b)
		e.B = append(e.B, e.C[:uint16Size]...)
	}
}

func (e *encodingBuf) MarshalUint32(bytes ...uint32) {
	for _, b := range bytes {
		binary.LittleEndian.PutUint32(e.C[:], b)
		e.B = append(e.B, e.C[:uint32Size]...)
	}
}

func (e *encodingBuf) MarshalUint64(bytes ...uint64) {
	for _, b := range bytes {
		binary.LittleEndian.PutUint64(e.C[:], b)
		e.B = append(e.B, e.C[:uint64Size]...)
	}
}

func (e *encodingBuf) MarshalString(s string) {
	e.B = append(e.B, s...)
}

func (d *decodingBuf) UnmarshalUint16(data []byte) uint16 {
	if len(data) < uint16Size {
		d.err = InvalidSizeError
		return 0
	}
	return binary.LittleEndian.Uint16(data)
}

func (d *decodingBuf) UnmarshalUint32(data []byte) uint32 {
	if len(data) < uint32Size {
		d.err = InvalidSizeError
		return 0
	}
	return binary.LittleEndian.Uint32(data)
}

func (d *decodingBuf) UnmarshalUint64(data []byte) uint64 {
	if len(data) < uint64Size {
		d.err = InvalidSizeError
		return 0
	}
	return binary.LittleEndian.Uint64(data)
}

func (d *decodingBuf) UnmarshalString(data []byte) string {
	return *((*string)(unsafe.Pointer(&data)))
}
