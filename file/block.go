package file

import "fmt"

type Block struct {
	Filename string
	Number   uint32
}

func GetBlock(fileName string, blockNum uint32) Block {
	return Block{fileName, blockNum}
}

func (b Block) String() string {
	return fmt.Sprintf("file: %v, block: %v", b.Filename, b.Number)
}
