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

func lineWriter(writeString func(string) (int, error), newline bool) func(string) {
	return func(line string) {
		var err error
		if line != "" {
			_, err = writeString(line)
		}
		if err == nil && newline {
			_, err = writeString("\n")
		}
		if err != nil {
			panic(err.Error())
		}
	}
}

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

	echo := lineWriter(out.WriteString, true)

	echo("// DO NOT EDIT: regenerate with `go generate`")
	echo("")
	echo("syntax = 'proto3';")
	echo("")
	echo("package csilvm;")
	echo("")
	echo(`import "github.com/gogo/protobuf/gogoproto/gogo.proto";`)
	echo("")

	echo("option go_package = 'csilvm';")
	echo("option (gogoproto.goproto_enum_prefix_all) = true;")
	echo("")

	var (
		r          = bufio.NewReader(rsp.Body)
		inProtoDef = false
		ii         = 1
	)
	echo = func() func(string) {
		liner := lineWriter(out.WriteString, false)
		return func(line string) {
			defer func() {
				if x := recover(); x != nil {
					log.Fatalf("error writing line %d: %v", ii, x)
				}
			}()
			liner(line)
		}
	}()
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
			echo(line)
		}
		ii++
	}
}
