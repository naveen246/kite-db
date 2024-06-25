package file

import "fmt"

type Block struct {
	Filename string
	Number   int64
}

func GetBlock(fileName string, blockNum int64) Block {
	return Block{fileName, blockNum}
}

func (b Block) String() string {
	return fmt.Sprintf("[file: %v, block: %v]", b.Filename, b.Number)
}
