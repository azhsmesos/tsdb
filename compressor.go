package tsdb

import (
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

type BytesCompressorType int8

const (
	// NoopBytesCompressor 默认不压缩
	NoopBytesCompressor BytesCompressorType = iota

	// ZSTDBytesCompressor 使用ZSTD压缩算法
	ZSTDBytesCompressor

	// SnappyBytesCompressor 使用snappy压缩算法
	SnappyBytesCompressor
)

// noopBytesCompressor 默认不压缩
type noopBytesCompressor struct{}
type zstdBytesCompressor struct{}
type snappyBytesCompressor struct{}

// BytesCompressor 数据压缩接口
type BytesCompressor interface {
	Compress(data []byte) []byte
	Decompress(data []byte) ([]byte, error)
}

//  默认压缩算法

func newNoopBytesCompressor() BytesCompressor {
	return &noopBytesCompressor{}
}

func (n *noopBytesCompressor) Compress(data []byte) []byte {
	return data
}

func (n *noopBytesCompressor) Decompress(data []byte) ([]byte, error) {
	return data, nil
}

// ZSTD 压缩算法

func newZSTDBytesCompressor() BytesCompressor {
	return &zstdBytesCompressor{}
}

func (n *zstdBytesCompressor) Compress(data []byte) []byte {
	encoder, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	return encoder.EncodeAll(data, make([]byte, 0, len(data)))
}

func (n *zstdBytesCompressor) Decompress(data []byte) ([]byte, error) {
	decoder, _ := zstd.NewReader(nil)
	return decoder.DecodeAll(data, nil)
}

// snappy 压缩算法

func newSnappyBytesCompressor() BytesCompressor {
	return &snappyBytesCompressor{}
}

func (n *snappyBytesCompressor) Compress(data []byte) []byte {
	return snappy.Encode(nil, data)
}

func (n *snappyBytesCompressor) Decompress(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}
