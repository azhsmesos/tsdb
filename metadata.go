package tsdb

import (
	"fmt"
	"sort"
	"strings"
)

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
	MinTimestamp          int64
	MaxTimestamp          int64
	Series                []metaSeries
	Labels                []seriesWithLabel
	SeriesIDRelatedLabels []LabelList
}

type binaryMetaserializer struct{}

const (
	endBlock   uint16 = 0xffff
	uint16Size        = 2
	uint32Size        = 4
	uint64Size        = 8
	signature         = "https://github.com/azhsmesos"
)

// MetaSerializer 编解码Segment元数据
type MetaSerializer interface {
	Marshal(Metadata) ([]byte, error)
	Unmarshal([]byte, *Metadata) error
}

func newBinaryMetaSerializer() MetaSerializer {
	return &binaryMetaserializer{}
}

func (b *binaryMetaserializer) Marshal(meta Metadata) ([]byte, error) {
	nowEncodingBuf := newEncodingBuf()

	labelOrdered := make(map[string]int)
	for index, labelToSids := range meta.Labels {
		labelOrdered[labelToSids.Name] = index
		nowEncodingBuf.MarshalUint16(uint16(len(labelToSids.Name)))
		nowEncodingBuf.MarshalString(labelToSids.Name)
		nowEncodingBuf.MarshalUint32(uint32(len(labelToSids.Sids)))
		nowEncodingBuf.MarshalUint32(labelToSids.Sids...)
	}
	nowEncodingBuf.MarshalUint16(endBlock)

	for index, series := range meta.Series {
		nowEncodingBuf.MarshalUint16(uint16(len(series.Sid)))
		nowEncodingBuf.MarshalString(series.Sid)
		nowEncodingBuf.MarshalUint64(series.StartOffset, series.EndOffset)

		labelList := meta.SeriesIDRelatedLabels[index]
		nowEncodingBuf.MarshalUint32(uint32(labelList.Len()))
		labelIndex := make([]uint32, 0, labelList.Len())
		for _, labelName := range labelList {
			labelIndex = append(labelIndex, uint32(labelOrdered[labelName.MarshalName()]))
		}
		sort.Slice(labelIndex, func(i, j int) bool {
			return labelIndex[i] < labelIndex[j]
		})
		nowEncodingBuf.MarshalUint32(labelIndex...)
	}
	nowEncodingBuf.MarshalUint16(endBlock)
	nowEncodingBuf.MarshalUint64(uint64(meta.MinTimestamp))
	nowEncodingBuf.MarshalUint64(uint64(meta.MaxTimestamp))
	nowEncodingBuf.MarshalString(signature)
	return DoCompress(nowEncodingBuf.Bytes()), nil
}

func (b *binaryMetaserializer) Unmarshal(data []byte, meta *Metadata) error {
	data, err := DoDecompress(data)
	if err != nil {
		return fmt.Errorf("faild to decompress, err: %v", err)
	}
	if len(data) < len(signature) {
		return fmt.Errorf("the data block is incomplete, data len: %d", len(data))
	}

	nowDecodingBuf := newDecodingBuf()
	// 首先判断数据是否完整
	if strings.EqualFold(nowDecodingBuf.UnmarshalString(data[len(data)-len(signature):]), signature) {
		return fmt.Errorf("the data block is incomplete, data: %s", nowDecodingBuf.UnmarshalString(data[len(data)-len(signature):]))
	}
	offset := 0
	labels := make([]seriesWithLabel, 0)
	for {
		var labelName string
		labelLen := nowDecodingBuf.UnmarshalUint16(data[offset : offset+uint16Size])
		offset += uint16Size
		if labelLen == endBlock {
			break
		}
		labelName = nowDecodingBuf.UnmarshalString(data[offset : offset+int(labelLen)])
		offset += int(labelLen)
		sidCount := nowDecodingBuf.UnmarshalUint32(data[offset : offset+uint32Size])
		offset += uint32Size
		sidList := make([]uint32, sidCount)
		for i := 0; i < int(sidCount); i++ {
			sidList[i] = nowDecodingBuf.UnmarshalUint32(data[offset : offset+uint32Size])
			offset += uint32Size
		}
		labels = append(labels, seriesWithLabel{
			Name: labelName,
			Sids: sidList,
		})
	}
	meta.Labels = labels

	seriesList := make([]metaSeries, 0)
	for {
		series := metaSeries{}
		sidLen := nowDecodingBuf.UnmarshalUint16(data[offset : offset+uint16Size])
		offset += uint16Size

		if sidLen == endBlock {
			break
		}

		series.Sid = nowDecodingBuf.UnmarshalString(data[offset : offset+int(sidLen)])
		offset += int(sidLen)

		series.StartOffset = nowDecodingBuf.UnmarshalUint64(data[offset : offset+uint64Size])
		offset += uint64Size

		series.EndOffset = nowDecodingBuf.UnmarshalUint64(data[offset : offset+uint64Size])
		offset += uint64Size

		labelCount := nowDecodingBuf.UnmarshalUint32(data[offset : offset+uint32Size])
		offset += uint32Size

		labelList := make([]uint32, labelCount)
		for i := 0; i < int(labelCount); i++ {
			labelList[i] = nowDecodingBuf.UnmarshalUint32(data[offset : offset+uint32Size])
			offset += uint32Size
		}
		series.Labels = labelList
		seriesList = append(seriesList, series)
	}
	meta.Series = seriesList
	meta.MinTimestamp = int64(nowDecodingBuf.UnmarshalUint64(data[offset : offset+uint64Size]))
	offset += uint64Size
	meta.MaxTimestamp = int64(nowDecodingBuf.UnmarshalUint64(data[offset : offset+uint64Size]))
	offset += uint64Size
	return nowDecodingBuf.err
}

func MarshalMeta(meta Metadata) ([]byte, error) {
	return defaultOpts.metaSerializer.Marshal(meta)
}

func UnmarshaMeta(data []byte, meta *Metadata) error {
	return defaultOpts.metaSerializer.Unmarshal(data, meta)
}
