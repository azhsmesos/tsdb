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

func newMemtableSidList() *memtableSidList {
	return &memtableSidList{
		container: make(map[string]struct{}),
	}
}

func newDiskIndexMap(swls []seriesWithLabel) *diskIndexMap {
	dim := &diskIndexMap{
		label2sids:   make(map[string]*diskSidList),
		labelOrdered: make(map[int]string),
	}
	for i := range swls {
		row := swls[i]
		dim.label2sids[row.Name] = newDiskSidList()
		for _, sid := range swls[i].Sids {
			dim.label2sids[row.Name].Add(sid)
		}
		dim.labelOrdered[i] = row.Name
	}
	return dim
}

func newDiskSidList() *diskSidList {
	return &diskSidList{
		list: roaring.New(),
	}
}

func (dsl *diskSidList) Add(value uint32) {
	dsl.mutex.Lock()
	defer dsl.mutex.Unlock()
	dsl.list.Add(value)
}

func (mim *memtableIndexMap) UpdateIndex(sid string, labels LabelList) {
	mim.mutex.Lock()
	defer mim.mutex.Unlock()
	for _, label := range labels {
		key := label.MarshalName()
		if _, ok := mim.index[key]; !ok {
			mim.index[key] = newMemtableSidList()
		}
		mim.index[key].Add(sid)
	}
}

func (msl *memtableSidList) Add(sid string) {
	msl.mutex.Lock()
	defer msl.mutex.Unlock()
	msl.container[sid] = struct{}{}
}

func (mim *memtableIndexMap) Range(fun func(key string, value *memtableSidList)) {
	mim.mutex.Lock()
	defer mim.mutex.Unlock()

	for key, sidLsit := range mim.index {
		fun(key, sidLsit)
	}
}

func (msl *memtableSidList) List() []string {
	msl.mutex.Lock()
	defer msl.mutex.Unlock()
	keys := make([]string, 0, len(msl.container))
	for k := range msl.container {
		keys = append(keys, k)
	}
	return keys
}
