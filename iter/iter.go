package iter

type Iterator interface {
	HasNext() bool
	Next() []byte
}
