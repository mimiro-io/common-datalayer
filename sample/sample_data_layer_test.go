package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	common_datalayer "github.com/mimiro-io/common-datalayer"
)

func TestStartStopSampleDataLayer(t *testing.T) {

	configFile := "./config" //filepath.Join(filepath.Dir(filename), "config")
	serviceRunner := common_datalayer.NewServiceRunner(NewSampleDataLayer)
	serviceRunner.WithConfigLocation(configFile)
	err := serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}
}

func TestNewSampleDataLayer(t *testing.T) {

	configFile := "./config"

	serviceRunner := common_datalayer.NewServiceRunner(NewSampleDataLayer)
	serviceRunner.WithConfigLocation(configFile)
	err := serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	// List datasets
	resp, err := http.Get("http://127.0.0.1:8090/datasets")
	fmt.Println("response: ", resp, err)
	println()
	content, _ := io.ReadAll(resp.Body)
	fmt.Println("content: ", string(content))
	println()

	// Post data
	reader := strings.NewReader(`[
		{"id": "@context", "namespaces": {"_": "http://data.sample.org/"}},
		{"id": "187", "props": {"name": "John Doe"}}
	]`)
	resp, err = http.Post("http://127.0.0.1:8090/datasets/sample/entities", "application/json", reader)
	fmt.Println("response: ", resp, err)
	println()

	// Get changes
	resp, err = http.Get("http://127.0.0.1:8090/datasets/sample/changes")
	fmt.Println("response: ", resp, err)
	println()
	content, _ = io.ReadAll(resp.Body)
	fmt.Println("content: ", string(content))
	println()

	serviceRunner.Stop()
}
