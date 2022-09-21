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
