package main

import cdl "github.com/mimiro-io/common-datalayer"

// main function
func main() {
	serviceRunner := cdl.NewServiceRunner(NewSampleDataLayer)
	serviceRunner.WithConfigLocation("./config")
	serviceRunner.WithEnrichConfig(EnrichConfig)
	err := serviceRunner.Start()
	if err != nil {
		panic(err)
	}
}
