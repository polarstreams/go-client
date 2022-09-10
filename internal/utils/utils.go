package utils

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
)

func ReadBody(resp *http.Response) (string, error) {
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	return string(body), err
}

func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

// Tries to reuse a buffer-backed reader.
// Otherwise, it creates a new Reader from the buffered data.
func ToReadSeeker(r io.Reader) io.ReadSeeker {
	readSeeker, ok := r.(io.ReadSeeker)
	if ok {
		return readSeeker
	}
	if buf, ok := r.(*bytes.Buffer); ok {
		return bytes.NewReader(buf.Bytes())
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	bufSlice := buf.Bytes()

	return bytes.NewReader(bufSlice)
}
