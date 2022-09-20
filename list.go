package tsdb

// List 排序链表结构
type List interface {
	Add(key int64, data interface{})
}

type node struct {
	height int
	key    int64
	value  interface{}
	left   *node
	right  *node
}

type avlTree struct {
	tree *node
}

func newTree() List {
	return &avlTree{
		&node{
			height: -2,
		},
	}
}

func (tree *avlTree) Add(key int64, value interface{}) {
	// todo 写avltree
}
