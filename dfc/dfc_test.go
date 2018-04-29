package dfc

import (
	"flag"
	"testing"
)

var coverageTest bool

func init() {
	flag.BoolVar(&coverageTest, "coverageTest", false, "Set to true when deploying DFC for code coverage runs")
}

func TestCoverage(t *testing.T) {
	if coverageTest {
		Run()
	}
}
