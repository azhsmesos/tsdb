package tsdb

import (
	"github.com/cespare/xxhash"
	"sort"
	"strings"
	"sync"
)

type labelValueList struct {
	mutex  sync.RWMutex
	values map[string]map[string]struct{}
}

// Label 一个标签组合
type Label struct {
	Name  string
	Value string
}

type LabelList []Label

var (
	labelBufPoll = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 1024)
		},
	}
)

func newLabelValueList() *labelValueList {
	return &labelValueList{
		values: map[string]map[string]struct{}{},
	}
}

func (lvs *labelValueList) Set(label, value string) {
	lvs.mutex.Lock()
	defer lvs.mutex.Unlock()
	if _, ok := lvs.values[label]; !ok {
		lvs.values[label] = make(map[string]struct{})
	}
	lvs.values[label][value] = struct{}{}
}

func (ll *LabelList) AddMetric(metric string) LabelList {
	// todo 需要在这儿进行筛选吗，要不要异步进行
	labels := ll.filter()
	labels = append(labels, Label{
		Name:  metricName,
		Value: metric,
	})
	return labels
}

// filter 过滤脏数据
func (ll LabelList) filter() LabelList {
	labels := make(map[string]struct{})
	var size int
	for _, value := range ll {
		_, ok := labels[value.Name]
		if !strings.EqualFold(value.Name, "") && !strings.EqualFold(value.Value, "") && !ok {
			ll[size] = value
			size++
		}
		labels[value.Name] = struct{}{}
	}
	return ll[:size]
}

func (ll LabelList) Len() int {
	return len(ll)
}

func (ll LabelList) Less(i, j int) bool {
	return ll[i].Name < ll[j].Name
}

func (ll LabelList) Swap(i, j int) {
	ll[i], ll[j] = ll[j], ll[i]
}

func (ll LabelList) Sorted() {
	sort.Sort(ll)
}

func (ll LabelList) Hash() uint64 {
	buf := labelBufPoll.Get().([]byte)
	const sep = `\xff`
	for _, label := range ll {
		buf = append(buf, label.Name...)
		buf = append(buf, sep...)
		buf = append(buf, label.Value...)
		buf = append(buf, sep...)
	}
	hash := xxhash.Sum64(buf)
	buf = buf[:0]
	labelBufPoll.Put(buf)
	return hash
}

func (l Label) MarshalName() string {
	return joinSeprator(l.Name, l.Value)
}
