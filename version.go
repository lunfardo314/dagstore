package dagstore

import (
	"fmt"
	"runtime/debug"
	"time"
)

// Version has the following structure: vA.B.C[pN]
// A is the major version. It is 0 until beta. Beta starts at 1
// B is the minor version. Change of the version means breaking change, for example of the schema
// C is the subversion. Change of it means non-breaking change
// pN marks the N-th optional patch of vA.B.C
const Version = "v0.1.0"

var (
	CommitHash       = "N/A"
	CommitTime       = "N/A"
	CommitTimeParsed time.Time
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				CommitHash = setting.Value
			}
			if setting.Key == "vcs.time" {
				CommitTime = setting.Value
				CommitTimeParsed, _ = time.Parse(time.RFC3339, CommitTime)
			}
		}
	}
}

func BannerString() string {
	return fmt.Sprintf("dagstore version %s, commit hash: %s, commit time: %s", Version, CommitHash, CommitTime)
}
