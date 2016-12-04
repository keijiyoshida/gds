package dataframe

import (
	"encoding/csv"
	"io"
	"net/http"
	"os"
	"strings"
)

var (
	prefixHTTP  = "http://"
	prefixHTTPS = "https://"
)

// ReadCSV reads CSV data from r, creates data frame data and returns it.
func ReadCSV(r io.Reader, config Config) (*DataFrame, error) {
	data, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, err
	}

	return New(data, config)
}

// ReadCSVFile reads a CSV data file, creates data frame data and returns it.
func ReadCSVFile(path string, config Config) (*DataFrame, error) {
	rc, err := getCSVReadCloser(path)
	if err != nil {
		return nil, err
	}

	defer rc.Close()

	return ReadCSV(rc, config)
}

// getCSVReadCloser gets a io.ReadCloser of the CSV data specified by path.
func getCSVReadCloser(path string) (io.ReadCloser, error) {
	if strings.HasPrefix(path, prefixHTTP) || strings.HasPrefix(path, prefixHTTPS) {
		resp, err := http.Get(path)
		if err != nil {
			return nil, err
		}

		return resp.Body, nil
	}

	return os.Open(path)
}
