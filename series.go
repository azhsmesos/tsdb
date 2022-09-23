package tsdb

import (
	"github.com/dgryski/go-tsz"
	"sync"
)

type tsStore struct {
	block        *tsz.Series
	lock         sync.RWMutex
	maxTimestamp int64
	count        int64
}

type memSeries struct {
	labels LabelList
	*tsStore
}

func newSeries(row *Row) *memSeries {
	return &memSeries{
		labels:  row.Labels,
		tsStore: &tsStore{},
	}
}

func (store *tsStore) Append(point *Point) *Point {
	store.lock.Lock()
	defer store.lock.Unlock()
	if store.maxTimestamp >= point.Timestamp {
		return point
	}
	store.maxTimestamp = point.Timestamp
	if store.count <= 0 {
		store.block = tsz.New(uint32(point.Timestamp))
	}
	store.block.Push(uint32(point.Timestamp), point.Value)
	store.maxTimestamp = point.Timestamp
	store.count++
	return nil
}
