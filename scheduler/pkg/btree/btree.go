package btree

const DefaultFreeListSize = 32

type Item interface {
	Less(than Item) bool
}

type items []Item

func (s *items) insertAt(index int, item Item) {
	*s = append(*s, nil)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = item
}

func (s *items) removeAt(index int) Item {
	item := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	size := len(*s)
	(*s)[size-1] = nil
	*s = (*s)[:size-1]
	return item
}

func (s *items) pop() Item {

}

func (s *items) truncate(index int) {

}

func (s *items) find(item Item) (index int, found bool) {

}
