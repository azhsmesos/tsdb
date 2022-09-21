package tsdb

import (
	"math"
)

// List 排序链表结构
type List interface {
	Add(key int64, data interface{})
	Remove(key int64) bool
	Range(start, end int64) Iter
	All() Iter
}

type Iter interface {
	Next() bool
	Value() interface{}
}

type iter struct {
	cursor int
	data   []interface{}
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
	// 插入insert avlTree
	tree.tree = insert(key, value, tree.tree)
}

func (tree *avlTree) Remove(key int64) bool {
	if tree.tree.find(key) {
		tree.tree = tree.tree.delete(key)
		return true
	}
	return false
}

func (tree *avlTree) Range(start, end int64) Iter {
	return tree.tree.values(start, end)
}

func (tree *avlTree) All() Iter {
	return tree.tree.values(0, math.MaxInt64)
}

func (it *iter) Next() bool {
	it.cursor++
	if len(it.data) > it.cursor {
		return true
	}
	return false
}

func (it *iter) Value() interface{} {
	return it.data[it.cursor]
}

func insert(key int64, value interface{}, avlNode *node) *node {
	if avlNode == nil {
		return &node{
			key:   key,
			value: value,
		}
	}

	if avlNode.height == -2 {
		avlNode.key = key
		avlNode.value = value
		avlNode.height = 0
		return avlNode
	}

	diff := key - avlNode.key
	if diff > 0 {
		// 插入右子树
		avlNode.right = insert(key, value, avlNode.right)
	} else if diff < 0 {
		// 插入左子树
		avlNode.left = insert(key, value, avlNode.left)
	} else {
		avlNode.value = value
	}

	avlNode.keepBalance(key)
	avlNode.height = maxHeight(avlNode.left.nollHeight(), avlNode.right.nollHeight()) + 1
	return avlNode
}

func (avlNode *node) keepBalance(key int64) *node {
	if avlNode.left.nollHeight()-avlNode.right.nollHeight() == 2 {
		if key-avlNode.left.key < 0 {
			avlNode = avlNode.rr()
		} else {
			avlNode = avlNode.lr()
		}
	} else if avlNode.right.nollHeight()-avlNode.left.nollHeight() == 2 {
		if avlNode.right.right.nollHeight() > avlNode.right.left.nollHeight() {
			avlNode = avlNode.ll()
		} else {
			avlNode = avlNode.rl()
		}
	}

	avlNode.height = maxHeight(avlNode.left.nollHeight(), avlNode.right.nollHeight()) + 1
	return avlNode
}

// rr 插入节点在失衡节点的左子树的左子树中，需要右旋
func (avlNode *node) rr() *node {
	next := avlNode.left
	avlNode.left = next.right
	next.right = avlNode

	next.height = maxHeight(next.left.nollHeight(), next.right.nollHeight()) + 1
	avlNode.height = maxHeight(avlNode.left.nollHeight(), avlNode.right.nollHeight()) + 1
	return next
}

// lr 插入节点在失衡节点的左子树的右子树中，先左旋后右旋
func (avlNode *node) lr() *node {
	avlNode.left = avlNode.left.ll()
	return avlNode.rr()
}

// ll 插入的节点在失衡节点的右子树的右子树中，左旋
func (avlNode *node) ll() *node {
	next := avlNode.right
	avlNode.right = next.left
	next.left = avlNode

	next.height = maxHeight(next.left.nollHeight(), next.right.nollHeight()) + 1
	avlNode.height = maxHeight(avlNode.left.nollHeight(), avlNode.right.nollHeight())
	return next
}

// rl 插入的节点在失衡节点的右子树的左子树中，先右旋后左旋
func (avlNode *node) rl() *node {
	avlNode.right = avlNode.right.rr()
	return avlNode.ll()
}

func (avlNode *node) values(start, end int64) Iter {
	item := &iter{
		data: []interface{}{
			nil,
		},
	}
	item.data = list(item.data, start, end, avlNode)
	return item
}

func list(values []interface{}, start, end int64, avlNode *node) []interface{} {
	if avlNode != nil {
		values = list(values, start, end, avlNode.left)
		if avlNode.key >= start && avlNode.key <= end {
			values = append(values, avlNode.value)
		}
		values = list(values, start, end, avlNode.right)
	}
	return values
}

func maxHeight(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func (avlNode *node) find(key int64) bool {
	if avlNode == nil {
		return false
	}
	diff := key - avlNode.key
	if diff > 0 {
		return avlNode.right.find(key)
	} else if diff < 0 {
		return avlNode.left.find(key)
	} else {
		return true
	}
}

func (avlNode *node) delete(key int64) *node {
	if avlNode == nil {
		return avlNode
	}

	diff := key - avlNode.key
	if diff > 0 {
		avlNode.right = avlNode.right.delete(key)
	} else if diff < 0 {
		avlNode.left = avlNode.left.delete(key)
	} else {
		if avlNode.left != nil && avlNode.right != nil {
			rightNode := avlNode.right.minNode()
			avlNode.key = rightNode.key
			avlNode.value = rightNode.value
			avlNode.right = avlNode.right.delete(avlNode.key)
		} else if avlNode.left != nil {
			avlNode = avlNode.left
		} else {
			avlNode = avlNode.right
		}
	}

	if avlNode != nil {
		avlNode.height = maxHeight(avlNode.left.nollHeight(), avlNode.right.nollHeight()) + 1
		avlNode = avlNode.keepBalance(key)
	}
	return avlNode
}

func (avlNode *node) minNode() *node {
	if avlNode == nil {
		return avlNode
	}
	if avlNode.left == nil {
		return avlNode
	} else {
		return avlNode.left.minNode()
	}
}

func (avlNode *node) maxNode() *node {
	if avlNode == nil {
		return avlNode
	}
	if avlNode.right == nil {
		return avlNode
	} else {
		return avlNode.right.maxNode()
	}
}

func (avlNode *node) nollHeight() int {
	if avlNode != nil {
		return avlNode.height
	}
	return -1
}
