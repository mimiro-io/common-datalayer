package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	common_datalayer "github.com/mimiro-io/common-datalayer"
)

func TestNewSampleDataLayer(t *testing.T) {
	service := common_datalayer.Start(NewSampleDataLayer, common_datalayer.ConfigFileOption("sample_config.json"))

	// List datasets
	resp, err := http.Get("http://localhost:21712/datasets")
	fmt.Println("response: ", resp, err)
	println()
	content, _ := io.ReadAll(resp.Body)
	fmt.Println("content: ", string(content))
	println()

	// Post data
	reader := strings.NewReader(`[
		{"id": "@context", "namespaces": {"_": "http://sample/"}},
		{"id": "187", "props": {"name": "John Doe"}}
	]`)
	resp, err = http.Post("http://localhost:21712/datasets/sample/entities", "application/json", reader)
	fmt.Println("response: ", resp, err)
	println()

	// Get changes
	resp, err = http.Get("http://localhost:21712/datasets/sample/changes")
	fmt.Println("response: ", resp, err)
	println()
	content, _ = io.ReadAll(resp.Body)
	fmt.Println("content: ", string(content))
	println()

	service.Stop()
}