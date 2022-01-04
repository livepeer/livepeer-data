package main

// This file intentionally minimal so that the analyzer binary
// can be imported and executed elsewhere. Main content of the
// CLI is in cmd/analyzer.go.

import "github.com/livepeer/livepeer-data/cmd/analyzer"

// Version content of this constant will be set at build time,
// using -ldflags, using output of the `git describe` command.
var Version = "undefined"

func main() {
	analyzer.Run(analyzer.BuildFlags{
		Version: Version,
	})
}
