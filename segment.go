package tsdb

import "sync"

type Segment interface {
	InsertRows(row []*Row)
	MinTs() int64
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
