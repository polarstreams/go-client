package types

import "io"

type FixedLengthReader interface {
	io.ReadSeeker

	// Len returns the number of bytes of the unread portion of the reader.
	Len() int
}
