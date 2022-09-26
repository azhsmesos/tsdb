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
	Load() Segment
	QueryLabelValuse(label string) []string
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

func (s *segmentList) Get(start, end int64) []Segment {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	segments := make([]Segment, 0)
	iter := s.list.All()
	for iter.Next() {
		segment := iter.Value().(Segment)
		if s.Scope(segment, start, end) {
			segments = append(segments, segment)
		}
	}
	if s.Scope(s.head, start, end) {
		segments = append(segments, s.head)
	}
	return segments
}

func (s *segmentList) Scope(segment Segment, start, end int64) bool {
	if segment.MinTs() < start && segment.MaxTs() > start {
		return true
	}

	if segment.MinTs() > start && segment.MaxTs() < end {
		return true
	}

	if segment.MinTs() < end && segment.MaxTs() > end {
		return true
	}
	return false
}
