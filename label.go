package tsdb

import "sync"

type labelValueList struct {
	mutex  sync.RWMutex
	values map[string]map[string]struct{}
}

func newLabelValueList() *labelValueList {
	return &labelValueList{
		values: map[string]map[string]struct{}{},
	}
}
