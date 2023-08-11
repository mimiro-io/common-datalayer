package main

import common_datalayer "mimiro.io/common-datalayer"

// main function
func main() {
	args := []string{"config.json"}

	common_datalayer.StartService(args, NewSampleDataLayer, EnrichConfig)
}