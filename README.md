# CSI plugin for LVM2

## Generating protocol buffers

It is assumed that you have docker installed.

```bash
go generate .
```

This will download the CSI spec, extract the protocol buffer definitions to `csi.proto`, and generate Go code in csi.pb.go.

The process builds a docker container called `csilvm-proto` which you may want to remove afterwards.
