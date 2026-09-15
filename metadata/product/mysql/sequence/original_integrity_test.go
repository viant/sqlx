package sequence

import (
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
)

// These are SHA-256 hashes of the original published 24e180f source files.
// The default-restoration patch must not repair or rewrite this implementation.
func TestOriginalMySQLImplementationUnchanged(t *testing.T) {
	for name, want := range map[string]string{
		"transient.go": "4d412e54e67cf72c1fb96799811d6444a9bb2db4837150353f19d61761720999",
		"handler.go":   "a81b302f1079bc5fc59aa01ab373f869b3055c444f868234c0031a991913c901",
		"udf.go":       "ea989fa4ff1e6618f896d65450fcadb4ebf63f89eba3f4f7f45f4972c6ddfa94",
	} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != want {
			t.Fatalf("original %s changed: %s want %s", name, got, want)
		}
	}
}
