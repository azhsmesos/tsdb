package tsdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cespare/xxhash"
	"github.com/sirupsen/logrus"
	"io/fs"
	"io/ioutil"
	"path/filepath"
	"runtime"
	"strings"
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

const (
	defaultQueueSize = 512
	separator        = "/-/"
)

func OpenTSDB(opts ...Option) *TSDB {
	for _, opt := range opts {
		opt(defaultOpts)
	}

	db := &TSDB{
		segments: newSegmentList(),
		queue:    make(chan []*Row, defaultQueueSize),
	}

	// 加载文件
	db.loadFiles()

	worker := runtime.GOMAXPROCS(-1)
	db.ctx, db.cancel = context.WithCancel(context.Background())
	for i := 0; i < worker; i++ {
		// 刷盘
		go db.saveRows(db.ctx)
	}
	return db
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

func (db *TSDB) loadFiles() {
	mkdir(defaultOpts.dataPath)
	err := filepath.Walk(defaultOpts.dataPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("failed to traverse the dir: %s, err: %v", path, err)
		}
		// 文件后续都是默认以seg开头
		if !info.IsDir() || !strings.HasPrefix(info.Name(), "seg-") {
			return nil
		}

		files, err := ioutil.ReadDir(filepath.Join(defaultOpts.dataPath, info.Name()))
		if err != nil {
			return fmt.Errorf("failed to load the data storage, err: %v", err)
		}

		// 从磁盘加载出最近的segment数据进入内存
		nowDiskSegment := &diskSegment{}
		for _, file := range files {
			filename := filepath.Join(defaultOpts.dataPath, info.Name(), file.Name())
			if strings.EqualFold(file.Name(), "data") {
				mmapFile, err := OpenMMapFile(filename)
				if err != nil {
					return fmt.Errorf("failed to open mmap file %s, err: %v", filename, err)
				}
				nowDiskSegment.dataFd = mmapFile
				nowDiskSegment.dataFilename = filename
				nowDiskSegment.labelVs = newLabelValueList()
			}

			if strings.EqualFold(file.Name(), "meta") {
				data, err := ioutil.ReadFile(filename)
				if err != nil {
					return fmt.Errorf("failed to read file: %s, err: %v", filename, err)
				}
				// 构造meta文件数据格式
				desc := Desc{}
				if err = json.Unmarshal(data, &desc); err != nil {
					return fmt.Errorf("failed to json unmarshal meta file: %v", err)
				}
				nowDiskSegment.minTimestamp = desc.MinTimestamp
				nowDiskSegment.maxTimestamp = desc.MaxTimestamp
			}
		}
		db.segments.Add(nowDiskSegment)
		return nil
	})

	if err != nil {
		logrus.Error(err)
	}
}

func (db *TSDB) saveRows(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case rows := <-db.queue:
			head, err := db.writeColdSegment()
			if err != nil {
				logrus.Errorf("failed to write cold data to disk: %v, err: %v", head, err)
				continue
			}
			head.InsertRows(rows)
		}
	}
}

func (db *TSDB) writeColdSegment() (Segment, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	return nil, nil
}

func (row Row) ID() string {
	return joinSeprator(xxhash.Sum64([]byte(row.Metric)), row.Labels.Hash())
}

func joinSeprator(a, b interface{}) string {
	return fmt.Sprintf("%v%s%v", a, separator, b)
}

func GetDataPath(dataPath string) Option {
	return func(c *options) {
		c.dataPath = dataPath
	}
}
