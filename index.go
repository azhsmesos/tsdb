package tsdb

import (
	"github.com/RoaringBitmap/roaring"
	"sync"
)

// 负责管理内存索引的存储和搜索

type memtableSidList struct {
	container map[string]struct{}
	mutex     sync.RWMutex
}

type diskSidList struct {
	list  *roaring.Bitmap
	mutex sync.RWMutex
}

type memtableIndexMap struct {
	index map[string]*memtableSidList
	mutex sync.RWMutex
}

type diskIndexMap struct {
	label2sids   map[string]*diskSidList
	labelOrdered map[int]string

	mutex sync.RWMutex
}

func newMemtableIndexMap() *memtableIndexMap {
	return &memtableIndexMap{
		index: make(map[string]*memtableSidList),
	}
}
