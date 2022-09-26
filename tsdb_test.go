package tsdb

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"strconv"
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

var metrics = []string{
	"cpu.busy", "disk.used",
	"net.in.bytes", "net.out.bytes",
	"mem.used", "mem.idle", "mem.used.bytes", "mem.total.bytes",
}

func genPoints(ts int64, node, dc int) []*Row {
	points := make([]*Row, 0)
	for _, metric := range metrics {
		points = append(points, &Row{
			Metric: metric,
			Labels: []Label{
				{Name: "node", Value: "vm_node_azh" + strconv.Itoa(node)},
				{Name: "computer", Value: strconv.Itoa(dc)},
			},
			Point: Point{Timestamp: ts, Value: float64(ts)},
		})
	}

	return points
}

func TestInsertRow(t *testing.T) {
	var start int64 = 1000000000
	tmpDir := "temp1/tsdb1"
	store := OpenTSDB(GetDataPath(tmpDir))
	var now = start
	for i := 0; i < 720; i++ {
		for n := 0; n < 3; n++ {
			for j := 0; j < 24; j++ {
				_ = store.InsertRows(genPoints(now, n, j))
			}
		}

		now += 60 //1min
	}

	queryLabelValues(store)
}

func queryLabelValues(store *TSDB) {

	lvs := store.QueryLabelValues("node", 1000000000, 1100000002)
	logrus.Infof("data: %+v\n", lvs)
}

func TestOpenDB(t *testing.T) {

	tmpDir := "temp1/tsdb1"
	store := OpenTSDB(GetDataPath(tmpDir))
	row := &Row{
		Metric: "cpu.busy",
		Labels: []Label{
			{Name: "node", Value: "vm1"},
			{Name: "dc", Value: "gz-idc"},
		},
		Point: Point{Timestamp: 1600000001, Value: 0.1},
	}
	rows := make([]*Row, 0)
	rows = append(rows, row)
	_ = store.InsertRows(rows)
	fmt.Println(store)
}
