package version

import (
	"github.com/mesosphere/csilvm/pkg/version/internal/versiondata"
)

type Version struct {
	Product   string
	Version   string
	BuildSHA  string
	BuildTime string
}

// Get returns the build-time version metadata that's been baked into the binary.
// It consumes the metadata from the internal/versiondata package.
func Get() Version {
	return Version{
		Product:   versiondata.Product,
		Version:   versiondata.Version,
		BuildSHA:  versiondata.BuildSHA,
		BuildTime: versiondata.BuildTime,
	}
}
