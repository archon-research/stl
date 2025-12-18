package main

import (
	"fmt"
	"os"

	"github.com/archon-research/stl/internal/stl-verify/adapters/primary/cli"
)

func main() {
	fmt.Println("Starting STL Verify CLI...")
	if err := cli.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
