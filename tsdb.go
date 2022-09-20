package tsdb

import (
	"context"
	"errors"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type options struct {
	metaSerializer    MetaSerializer  // 元数据自定义Marshal接口
	bytesCompressor   BytesCompressor // 数据持久化存储压缩接口
	retention         time.Duration   // 数据保留时长
	segmentDuration   time.Duration   // 一个segment的时长
	writeTimeout      time.Duration   // 写超时
	onlyMemoryMode    bool
	enableOutdated    bool   // 是否可以写入过时数据（乱序写入）
	maxRowsPerSegment int64  // 每段的最大row的数量
	dataPath          string // Segment 持久化存储文件夹
}

type TSDB struct {
	segments *segmentList
	mutex    sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc

	queue chan []*Row
	wait  sync.WaitGroup
}

// Point 一个数据点
type Point struct {
	Timestamp int64
	Value     float64
}

// Label 一个标签组合
type Label struct {
	Name  string
	Value string
}

type LabelList []Label

var (
	timerPool   sync.Pool
	defaultOpts = &options{
		metaSerializer:    newBinaryMetaSerializer(),
		bytesCompressor:   newNoopBytesCompressor(),
		segmentDuration:   2 * time.Hour, // 默认两小时
		retention:         7 * 24 * time.Hour,
		writeTimeout:      30 * time.Second,
		onlyMemoryMode:    false,
		enableOutdated:    true,
		maxRowsPerSegment: 19960412, // 该数字可自定义
		dataPath:          ".",
	}
)

// Row 一行时序数据库，包括数据点和标签组合
type Row struct {
	Metric string
	Labels LabelList
	Point  Point
}

type Option func(c *options)

func OpenTSDB(opts ...Option) {
	for _, opt := range opts {
		opt(defaultOpts)
	}

}

// InsertRows 插入rows
func (tsdb *TSDB) InsertRows(rows []*Row) error {
	timer := getTimer(defaultOpts.writeTimeout)
	select {
	case tsdb.queue <- rows:
		putTimer(timer)
	case <-timer.C:
		putTimer(timer)
		return errors.New("failed to insert rows to database, write overload")
	}
	return nil
}

func getTimer(duration time.Duration) *time.Timer {
	if value := timerPool.Get(); value != nil {
		t := value.(*time.Timer)
		if t.Reset(duration) {
			logrus.Error("active timer trapped to the pool")
			return nil
		}
		return t
	}
	return time.NewTimer(duration)
}

func putTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}
