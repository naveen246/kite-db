package kv

import "bytes"

func compareWithSlash(a, b []byte) int {
	for len(a) > 0 && len(b) > 0 {
		idxA, idxB := bytes.IndexByte(a, '/'), bytes.IndexByte(b, '/')
		switch {
		case idxA < 0 && idxB < 0:
			return bytes.Compare(a, b)
		case idxA < 0 && idxB >= 0:
			return -1
		case idxA >= 0 && idxB < 0:
			return +1
		}

		// At this point, both slices have '/'
		spanA, spanB := a[:idxA], b[:idxB]
		spanRes := bytes.Compare(spanA, spanB)
		if spanRes != 0 {
			return spanRes
		}

		a, b = a[idxA+1:], b[idxB+1:]
	}

	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return +1
	}

	return 0
}
