package main

import (
	cdl "github.com/mimiro-io/common-datalayer"
	"os"
)

// main function
func main() {
	args := os.Args[1:]
	configFolderLocation := args[0]
	serviceRunner := cdl.NewServiceRunner(NewSampleDataLayer)
	serviceRunner.WithConfigLocation(configFolderLocation)
	serviceRunner.WithEnrichConfig(EnrichConfig)
	serviceRunner.StartAndWait()
}
