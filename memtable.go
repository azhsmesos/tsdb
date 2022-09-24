package tsdb

import (
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"math"
	"os"
	"path"
	"sort"
	"sync"
	"sync/atomic"
)

type memtable struct {
	once          sync.Once
	segment       sync.Map
	indexMap      *memtableIndexMap
	labelVs       *labelValueList
	outdated      map[string]List
	outdatedMutex sync.RWMutex

	minTimestamp int64
	maxTimestamp int64

	seriesCount     int64
	dataPointsCount int64
}

func newMemtable() Segment {
	return &memtable{
		indexMap:     newMemtableIndexMap(),
		labelVs:      newLabelValueList(),
		outdated:     make(map[string]List),
		minTimestamp: math.MaxInt64,
		maxTimestamp: math.MinInt64,
	}
}

func (m *memtable) InsertRows(rows []*Row) {
	for _, row := range rows {
		m.labelVs.Set(metricName, row.Metric)
		for _, label := range row.Labels {
			m.labelVs.Set(label.Name, label.Value)
		}
		// todo 基于字符串排序
		row.Labels = row.Labels.AddMetric(row.Metric)
		row.Labels.Sorted()

		series := m.getSeries(row)
		points := series.Append(&row.Point)

		if points != nil {
			m.outdatedMutex.Lock()
			if _, ok := m.outdated[row.ID()]; !ok {
				m.outdated[row.ID()] = newTree()
			}
			m.outdated[row.ID()].Add(row.Point.Timestamp, row.Point)
			m.outdatedMutex.Unlock()
		}

		if atomic.LoadInt64(&m.minTimestamp) >= row.Point.Timestamp {
			atomic.StoreInt64(&m.minTimestamp, row.Point.Timestamp)
		}

		if atomic.LoadInt64(&m.maxTimestamp) <= row.Point.Timestamp {
			atomic.StoreInt64(&m.maxTimestamp, row.Point.Timestamp)
		}
		atomic.AddInt64(&m.dataPointsCount, 1)
		m.indexMap.UpdateIndex(row.ID(), row.Labels)
	}
}

func (m *memtable) MinTs() int64 {
	return atomic.LoadInt64(&m.minTimestamp)
}

func (m *memtable) MaxTs() int64 {
	return atomic.LoadInt64(&m.maxTimestamp)
}

func (m *memtable) Frozen() bool {
	if defaultOpts.onlyMemoryMode {
		return false
	}
	return m.MaxTs()-m.MinTs() > int64(defaultOpts.segmentDuration.Seconds())
}

func (m *memtable) Close() error {
	if m.dataPointsCount == 0 || defaultOpts.onlyMemoryMode {
		return nil
	}
	return writeToDisk(m)
}

func (m *memtable) Cleanup() error {
	return nil
}

func mkdir(dir string) {
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		return
	}
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		logrus.Error("BUG: failed to create dir: ", dir)
		return
	}
}

func (m *memtable) getSeries(row *Row) *memSeries {
	value, ok := m.segment.Load(row.ID())
	if ok {
		return value.(*memSeries)
	}

	atomic.AddInt64(&m.seriesCount, 1)
	newSeries := newSeries(row)
	m.segment.Store(row.ID(), newSeries)
	return newSeries
}

func writeToDisk(segment *memtable) error {
	dataBytes, descBytes, err := segment.Marshal()
	if err != nil {
		return fmt.Errorf("faild to marshal segment: %s", err.Error())
	}
	writeFile := func(file string, data []byte) error {
		if isFileExist(file) {
			return fmt.Errorf("%s file is already exist", file)
		}
		fd, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			return nil
		}
		defer fd.Close()
		_, err = fd.Write(data)
		return err
	}

	dirname := makeDirName(segment.MinTs(), segment.MaxTs())
	mkdir(dirname)

	if err = writeFile(path.Join(dirname, "data"), dataBytes); err != nil {
		return err
	}

	if err = writeFile(path.Join(dirname, "meta"), descBytes); err != nil {
		return err
	}
	return nil
}

// Marshal data, desc, err
func (m *memtable) Marshal() ([]byte, []byte, error) {
	sidList := make(map[string]uint32)
	startOffset := 0
	size := 0
	dataBuf := make([]byte, 0)
	dataBuf = append(dataBuf, make([]byte, uint64Size>>1)...)
	meta := Metadata{
		MinTimestamp: m.minTimestamp,
		MaxTimestamp: m.maxTimestamp,
	}

	m.segment.Range(func(key, value any) bool {
		seriesID := key.(string)
		sidList[seriesID] = uint32(size)
		size++
		series := value.(*memSeries)
		meta.SeriesIDRelatedLabels = append(meta.SeriesIDRelatedLabels, series.labels)
		m.outdatedMutex.RLock()
		listValue, ok := m.outdated[seriesID]
		m.outdatedMutex.RUnlock()

		var dataBytes []byte
		if ok {
			dataBytes = DoCompress(series.MergeOutdatedList(listValue).Bytes())
		} else {
			dataBytes = DoCompress(series.Bytes())
		}

		dataBuf = append(dataBuf, dataBytes...)
		endOffset := startOffset + len(dataBytes)
		meta.Series = append(meta.Series, metaSeries{
			Sid:         key.(string),
			StartOffset: uint64(startOffset),
			EndOffset:   uint64(endOffset),
		})
		startOffset = endOffset
		return true
	})
	labelIndex := make([]seriesWithLabel, 0)
	m.indexMap.Range(func(key string, value *memtableSidList) {
		list := make([]uint32, 0)
		for _, sid := range value.List() {
			list = append(list, sidList[sid])
		}
		sort.Slice(list, func(i, j int) bool {
			return list[i] < list[j]
		})
		labelIndex = append(labelIndex, seriesWithLabel{
			Name: key,
			Sids: list,
		})
	})
	meta.Labels = labelIndex
	metaBytes, err := MarshalMeta(meta)
	if err != nil {
		return nil, nil, err
	}
	metaLen := len(metaBytes)
	desc := &Desc{
		SeriesCount:     m.seriesCount,
		DataPointsCount: m.dataPointsCount,
		MaxTimestamp:    m.maxTimestamp,
		MinTimestamp:    m.minTimestamp,
	}

	descBytes, err := json.MarshalIndent(desc, "", "\t")
	dataLen := len(dataBuf) - (uint64Size >> 1)
	dataBuf = append(dataBuf, metaBytes...)
	newEncodingBuf := newEncodingBuf()

	newEncodingBuf.MarshalUint64(uint64(dataLen))
	dataLenBytes := newEncodingBuf.Bytes()
	copy(dataBuf[:uint64Size], dataLenBytes[:uint64Size])
	newEncodingBuf.Reset()

	newEncodingBuf.MarshalUint64(uint64(metaLen))
	metaLenBytes := newEncodingBuf.Bytes()
	copy(dataBuf[uint64Size:(uint64Size*2)], metaLenBytes[:uint64Size])
	return dataBuf, descBytes, nil
}
