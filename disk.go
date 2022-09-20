package tsdb

import (
	"github.com/sirupsen/logrus"
	"sync"
)

type diskSegment struct {
	dataFd       *MMapFile
	dataFilename string
	dir          string
	load         bool

	wait         sync.WaitGroup
	labelVs      *labelValueList
	indexMap     *diskIndexMap
	series       []metaSeries
	minTimestamp int64
	maxTimestamp int64

	seriesCount     int64
	dataPointsCount int64
}

func (ds *diskSegment) MinTs() int64 {
	return ds.minTimestamp
}

func (ds *diskSegment) InsertRows(_ []*Row) {
	logrus.Error("disk segments are not mutable")
}
