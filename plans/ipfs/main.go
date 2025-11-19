// Welcome, testground plan writer!
// If you are seeing this for the first time, check out our documentation!
// https://app.gitbook.com/@protocol-labs/s/testground/

package main

import (
	testrun "github.com/testground/sdk-go/run"
)

var testcases = map[string]interface{}{
	"ipfsDemo": testrun.InitializedTestCaseFn(ipfsDemo),
}

func main() {
	testrun.InvokeMap(testcases)
}
