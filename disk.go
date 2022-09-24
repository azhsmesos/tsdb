package tsdb

import (
	"github.com/sirupsen/logrus"
	"os"
	"path"
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

func (ds *diskSegment) Frozen() bool {
	return true
}

func (ds *diskSegment) MaxTs() int64 {
	return 0
}

func (ds *diskSegment) Close() error {
	// 保证没有进程使用fd
	ds.wait.Wait()
	return ds.dataFd.Close()
}

func (ds *diskSegment) Cleanup() error {
	return os.RemoveAll(ds.dir)
}

func newDiskSegment(mmapFile *MMapFile, dirname string, minTimestamp, maxTimestamp int64) Segment {
	return &diskSegment{
		dataFd:       mmapFile,
		dir:          dirname,
		dataFilename: path.Join(dirname, "meta.json"),
		minTimestamp: minTimestamp,
		maxTimestamp: maxTimestamp,
		labelVs:      newLabelValueList(),
	}
}
