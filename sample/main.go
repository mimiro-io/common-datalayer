package main

import layer "github.com/mimiro-io/common-datalayer"

// main function
func main() {
	//args := []string{"config.json"}

	_ = layer.Start(NewSampleDataLayer, EnrichConfig).AndWait()
}