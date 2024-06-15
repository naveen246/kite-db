package file

import "fmt"

type Block struct {
	Filename string
	Number   int
}

func NewBlock(fileName string, blockNum int) *Block {
	return &Block{fileName, blockNum}
}

func (b Block) String() string {
	return fmt.Sprintf("file: %v, block: %v", b.Filename, b.Number)
}
