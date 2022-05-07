package utils

import (
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
