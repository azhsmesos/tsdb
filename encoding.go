package tsdb

import (
	"encoding/binary"
)

type encodingBuf struct {
	B []byte
	C [binary.MaxVarintLen64]byte
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
