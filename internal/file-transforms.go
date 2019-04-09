package internal

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

func getFirstFileFromTarGzip(b []byte) ([]byte, error) {
	var out []byte

	gr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return out, err
	}

	tr := tar.NewReader(gr)
	tr.Next()

	out, err = ioutil.ReadAll(tr)
	if err != nil {
		return out, err
	}

	return out, nil
}
