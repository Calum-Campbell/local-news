package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
)

func main() {
	ctx := context.Background()

	fileName := os.Args[0]

	fileContent, err := ioutil.ReadFile(fileName) // just pass the file name
	if err != nil {
		fmt.Print(err)
	}

}
