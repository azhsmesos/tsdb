package tsdb

import (
	"bytes"
	"github.com/sirupsen/logrus"
	"os"
	"path"
	"strings"
	"sync"
	"time"
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

type DReader struct {
	reader *bytes.Reader
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

func (ds *diskSegment) Load() Segment {
	if ds.load {
		return ds
	}
	start := time.Now()
	reader := bytes.NewReader(ds.dataFd.Bytes())
	dreader := &DReader{
		reader: reader,
	}
	dataLen, metaLen, err := dreader.Read()
	if err != nil {
		logrus.Errorf("faild to read %s, err: %v", ds.dataFilename, err)
		return ds
	}
	metaBytes := make([]byte, metaLen)
	_, err = reader.ReadAt(metaBytes, uint64Size>>1+int64(dataLen))
	if err != nil {
		logrus.Errorf("faild to read %s, metaData error: %v", ds.dataFilename, err)
		return ds
	}
	var meta Metadata
	if err = UnmarshaMeta(metaBytes, &meta); err != nil {
		logrus.Errorf("faild to unmarshal meta, error: %v", err)
		return ds
	}
	for _, label := range meta.Labels {
		key, value := UnmarshalLabelName(label.Name)
		if !strings.EqualFold(key, "") && strings.EqualFold(value, "") {
			ds.labelVs.Set(key, value)
		}
	}
	ds.indexMap = newDiskIndexMap(meta.Labels)
	ds.series = meta.Series
	ds.load = true
	logrus.Infof("load disk segment %s, time: %v", ds.dataFilename, time.Since(start))
	return ds
}

func (ds *diskSegment) QueryLabelValuse(label string) []string {
	return ds.labelVs.Get(label)
}

func (dr *DReader) Read() (int64, int64, error) {
	// 读取data长度
	diskDataLen := make([]byte, uint64Size)
	_, err := dr.reader.ReadAt(diskDataLen, 0)
	if err != nil {
		return 0, 0, err
	}
	nowDecodingBuf := newDecodingBuf()
	// nowDecodingBuf.UnmarshalUint64(diskDataLen)
	dataLen := nowDecodingBuf.UnmarshalUint64(diskDataLen)

	// 读取 meta长度
	diskDataLen = make([]byte, uint64Size)
	_, err = dr.reader.ReadAt(diskDataLen, uint64Size)
	if err != nil {
		return 0, 0, err
	}
	nowDecodingBuf = newDecodingBuf()
	metaLen := nowDecodingBuf.UnmarshalUint64(diskDataLen)
	return int64(dataLen), int64(metaLen), nil
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
