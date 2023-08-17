package main

import layer "github.com/mimiro-io/common-datalayer"

// main function
func main() {
	layer.Start(
		NewSampleDataLayer,
		layer.ConfigFileOption("sample/sample_config.json"),
	).AndWait()
}