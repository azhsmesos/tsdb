package tsdb

import (
	"os"
	"sync"
)

type Segment interface {
	InsertRows(row []*Row)
	MinTs() int64
	MaxTs() int64
	Frozen() bool
	Close() error
	Cleanup() error
}

type segmentList struct {
	mutex sync.RWMutex
	head  Segment
	list  List
}

type Desc struct {
	SeriesCount     int64 `json:"seriesCount"`
	DataPointsCount int64 `json:"dataPointsCount"`
	MaxTimestamp    int64 `json:"maxTimestamp"`
	MinTimestamp    int64 `json:"minTimestamp"`
}

const (
	metricName = "__name__"
)

func newSegmentList() *segmentList {
	return &segmentList{
		head: newMemtable(),
		list: newTree(),
	}
}

func (s *segmentList) Add(segment Segment) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.list.Add(segment.MinTs(), segment)
}

func (s *segmentList) Replace(pre, next Segment) error {
	s.mutex.Lock()
	s.mutex.Unlock()
	if err := pre.Close(); err != nil {
		return err
	}

	if err := pre.Cleanup(); err != nil {
		return err
	}
	s.list.Add(pre.MinTs(), next)
	return nil
}

func isFileExist(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
