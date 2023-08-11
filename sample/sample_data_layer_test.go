package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	common_datalayer "mimiro.io/common-datalayer"
)

func TestNewSampleDataLayer(t *testing.T) {
	service := common_datalayer.CreateService([]string{"config.json"}, NewSampleDataLayer, EnrichConfig)

	// List datasets
	resp, err := http.Get("http://localhost:8080/datasets")
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
	resp, err = http.Post("http://localhost:8080/datasets/sample/entities", "application/json", reader)
	fmt.Println("response: ", resp, err)
	println()

	// Get changes
	resp, err = http.Get("http://localhost:8080/datasets/sample/changes")
	fmt.Println("response: ", resp, err)
	println()
	content, _ = io.ReadAll(resp.Body)
	fmt.Println("content: ", string(content))
	println()

	service.Stop()
}