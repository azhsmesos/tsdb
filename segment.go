package tsdb

import "sync"

type Segment interface {
	InsertRows(row []*Row)
}

type segmentList struct {
	mutex sync.RWMutex
	head  Segment
	list  List
}
