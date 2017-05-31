// +build ignore

// This program downloads the latest protocol buffer definitions from the CSI spec and generates the resulting go code.
// It is intended to be run by go generate.
package main

import (
	"bufio"
	"io"
	"log"
	"net/http"
	"os"
)

func main() {
	const url = "https://raw.githubusercontent.com/container-storage-interface/spec/master/spec.md"

	rsp, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer rsp.Body.Close()

	out, err := os.Create("csi.proto")
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	if _, err := out.WriteString("// DO NOT EDIT: regenerate with `go generate`\n"); err != nil {
		log.Fatalf("error writing header: %v", err)
	}
	if _, err := out.WriteString("syntax = 'proto3';\n\n"); err != nil {
		log.Fatalf("error writing header: %v", err)
	}

	if _, err := out.WriteString("option go_package = 'csilvm';\n\n"); err != nil {
		log.Fatalf("error writing header: %v", err)
	}


	r := bufio.NewReader(rsp.Body)
	inProtoDef := false
	ii := 1
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Fatalf("error reading spec.md: %v", err)
		}
		const protoStart = "```protobuf\n"
		if line == protoStart {
			if inProtoDef {
				log.Fatalf("expected code section to end before starting a new one: line %d", ii)
			}
			inProtoDef = true
			continue
		}
		const protoEnd = "```\n"
		if line == protoEnd {
			inProtoDef = false
			continue
		}
		if inProtoDef {
			if _, err := out.WriteString(line); err != nil {
				log.Fatalf("error writing line %d: %v", ii, err)
			}
		}
		ii++
	}
}
