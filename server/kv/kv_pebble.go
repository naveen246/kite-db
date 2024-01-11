package kv

import "github.com/cockroachdb/pebble"

var (
	KiteSlashSpanComparer = &pebble.Comparer{
		Compare:            compareWithSlash,
		Equal:              pebble.DefaultComparer.Equal,
		AbbreviatedKey:     pebble.DefaultComparer.AbbreviatedKey,
		FormatKey:          pebble.DefaultComparer.FormatKey,
		FormatValue:        pebble.DefaultComparer.FormatValue,
		Separator:          pebble.DefaultComparer.Separator,
		Split:              pebble.DefaultComparer.Split,
		Successor:          pebble.DefaultComparer.Successor,
		ImmediateSuccessor: pebble.DefaultComparer.ImmediateSuccessor,
		Name:               "kite-slash-spans",
	}
)
