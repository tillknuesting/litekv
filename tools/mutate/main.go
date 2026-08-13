// Command mutate breaks the engine on purpose, one change at a time, and sees
// whether a test notices.
//
//	go run ./tools/mutate          # all of them
//	go run ./tools/mutate lock     # only those whose name matches
//
// The machinery is in package mutate; what is here is this repository's own
// list of what to break, in mutations.go, and the one setting that belongs to
// this suite rather than to the tool.
//
// Every mutation must be caught. A mutation that survives is a promise the code
// makes that nothing is holding it to.
package main

import (
	"fmt"
	"os"

	"github.com/tillknuesting/litekv/mutate"
)

// The engine's suite takes about forty-five seconds under -race, and during a
// sweep eight copies of it are running at once on one machine. Ten minutes is
// room to spare; see mutate.Options.Timeout for why erring the other way is the
// worst thing that can be done to this tool.
const timeout = "600s"

func main() {
	if err := mutate.Run(mutations, mutate.Options{Timeout: timeout}, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "mutate:", err)
		os.Exit(1)
	}
}
