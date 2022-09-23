package tsdb

import (
	"fmt"
	"strings"
	"testing"
)

func TestOpenTSDB(t *testing.T) {
	if strings.EqualFold("a", "a") {
		fmt.Printf("azh")
	}
}

func TestAVLTree(t *testing.T) {
	tree := newTree()
	tree.Add(2, "d")
	tree.Add(1, "a")
	tree.Add(2, "b")
	tree.Add(3, "c")
	tree.Add(4, "d")

	//nowDiskSegment := []string {"a", "b", "c"}

	iter := tree.All()
	for iter.Next() {
		fmt.Println(iter.Value().(string))
	}
}

func TestRowLabelsHash(t *testing.T) {
	row := &Row{
		Metric: "cpu.busy",
		Labels: []Label{
			{Name: "node", Value: "vm1"},
			{Name: "dc", Value: "gz-idc"},
		},
		Point: Point{Timestamp: 1600000001, Value: 0.1},
	}

	res := row.Labels.Hash()
	fmt.Println(res)
}

func TestOpenDB(t *testing.T) {
	tmpDir := "temp1/tsdb1"
	store := OpenTSDB(GetDataPath(tmpDir))
	fmt.Println(store)
}
