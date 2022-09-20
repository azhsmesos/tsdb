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
