package main

import layer "github.com/mimiro-io/common-datalayer"

// main function
func main() {
	layer.Start(
		NewSampleDataLayer,
		//layer.ConfigFileOption("local-config.json"),
		//layer.ConfigFileOption("override-config.json"),
		layer.EnrichConfigOption(EnrichConfig),
	).AndWait()
}