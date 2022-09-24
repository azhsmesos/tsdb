package tsdb

import (
	"github.com/dgryski/go-tsz"
	"math"
	"sort"
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

func (store *tsStore) MergeOutdatedList(list List) *tsStore {
	if list == nil {
		return store
	}

	newStore := &tsStore{}
	point := store.All()
	item := list.All()
	for item.Next() {
		listPoint := item.Value().(Point)
		point = append(point, Point{
			Timestamp: listPoint.Timestamp,
			Value:     listPoint.Value,
		})
	}
	sort.Slice(point, func(i, j int) bool {
		return point[i].Timestamp < point[j].Timestamp
	})
	for i := 0; i < len(point); i++ {
		newStore.Append(&point[i])
	}
	return newStore
}

func (store *tsStore) Bytes() []byte {
	return store.block.Bytes()
}

func (store *tsStore) All() []Point {
	return store.Get(math.MinInt64, math.MaxInt64)
}

func (store *tsStore) Get(start, end int64) []Point {
	points := make([]Point, 0)
	item := store.block.Iter()
	for item.Next() {
		ts, val := item.Values()
		if ts > uint32(end) {
			break
		}
		if ts >= uint32(start) {
			points = append(points, Point{
				Timestamp: int64(ts),
				Value:     val,
			})
		}
	}
	return points
}
