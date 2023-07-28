package main

import (
	. "mimiro.io/common-datalayer"
)

// main function
func main() {
	args := []string{"config.json"}

	StartService(args, NewSampleDataLayer, EnrichConfig)
}
