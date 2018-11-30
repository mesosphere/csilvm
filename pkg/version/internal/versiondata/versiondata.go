package versiondata

const (
	defaultProduct = "io.mesosphere.csi.lvm"
	defaultVersion = "v0-dev"
)

var (
	Product   = defaultProduct
	Version   = defaultVersion
	BuildSHA  string
	BuildTime string
)
