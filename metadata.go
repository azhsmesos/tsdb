package tsdb

type metaSeries struct {
	Sid         string
	StartOffset uint64
	EndOffset   uint64
	Labels      []uint32
}

type seriesWithLabel struct {
	Name string
	Sids []uint32
}

type Metadata struct {
	MinTimestamp int64
	MaxTimestamp int64
	Series       []metaSeries
	Labels       []seriesWithLabel
}

type binaryMetaserializer struct{}

// MetaSerializer 编解码Segment元数据
type MetaSerializer interface {
	Marshal(Metadata) ([]byte, error)
	Unmarshal([]byte, *Metadata) error
}

func newBinaryMetaSerializer() MetaSerializer {
	return &binaryMetaserializer{}
}

func (b *binaryMetaserializer) Marshal(meta Metadata) ([]byte, error) {
	return nil, nil
}

func (b *binaryMetaserializer) Unmarshal(data []byte, meta *Metadata) error {
	return nil
}
