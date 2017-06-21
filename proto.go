package csilvm

//go:generate go run gen.go
//go:generate docker build -t csilvm-proto -f Dockerfile.protobuf3 .
//go:generate docker run --volume $PWD:/work csilvm-proto protoc --gogoslick_out=. csi.proto
