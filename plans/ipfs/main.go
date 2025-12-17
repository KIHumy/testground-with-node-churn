package main

import testrun "github.com/testground/sdk-go/run"

// This function is copied from Testground (main.go).
// Source: https://github.com/testground/testground/blob/master/plans/network/main.go
// License: MIT License (https://github.com/testground/testground/blob/master/LICENSE-MIT)
// License: Apache License (https://github.com/testground/testground/blob/master/LICENSE-APACHE)
// Adjustments: Deleted the old test cases and the routingPolicyTests. Added a new testcase.
var testcases = map[string]interface{}{
	"ipfsDemo": testrun.InitializedTestCaseFn(ipfsDemo),
}

// This function is copied from Testground (main.go).
// Source: https://github.com/testground/testground/blob/master/plans/network/main.go
// License: MIT License (https://github.com/testground/testground/blob/master/LICENSE-MIT)
// License: Apache License (https://github.com/testground/testground/blob/master/LICENSE-APACHE)
// Adjustments: Used the alias testrun instead of run.
func main() {
	testrun.InvokeMap(testcases)
}
