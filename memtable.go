package tsdb

import (
	"github.com/sirupsen/logrus"
	"math"
	"os"
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
