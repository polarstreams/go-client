package utils

import (
	"bytes"
	"io"
	"net/http"

	"github.com/polarstreams/go-client/internal/types"
)

func ReadBody(resp *http.Response) (string, error) {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return string(body), err
}

func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

// Tries to reuse a buffer-backed reader.
// Otherwise, it creates a new Reader from the buffered data.
func ToReadSeeker(r io.Reader) types.FixedLengthReader {
	readSeeker, ok := r.(types.FixedLengthReader)
	if ok {
		return readSeeker
	}
	if buf, ok := r.(*bytes.Buffer); ok {
		return bytes.NewReader(buf.Bytes())
	}

	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(r)
	bufSlice := buf.Bytes()

	return bytes.NewReader(bufSlice)
}
