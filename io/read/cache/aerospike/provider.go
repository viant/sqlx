package aerospike

import (
	"crypto/sha256"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config describes one configured cache. Provider is aerospike://host:port/namespace;
// Location is the set name. Identity scopes native query keys within that set.
// It must distinguish views using different connectors even when SQL matches.
type Config struct {
	Provider string
	Location string
	Identity string
	TTL      time.Duration
	Timeout  TimeoutConfig
	// FailedRequestLimit is the tolerated number of consecutive failed native
	// record operations. A lookup can perform both lazy and warmup operations.
	FailedRequestLimit int
	ResetFailuresInMs  int
}

type provider struct {
	host      string
	port      int
	namespace string
}

func (c Config) validate() (*provider, error) {
	u, err := url.Parse(c.Provider)
	if err != nil || u.Scheme != "aerospike" || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" {
		return nil, fmt.Errorf("aerospike provider must be aerospike://host:port/namespace without credentials, query or fragment")
	}
	port, err := strconv.Atoi(u.Port())
	namespace := strings.TrimPrefix(u.Path, "/")
	if err != nil || port < 1 || port > 65535 || strings.TrimSpace(u.Hostname()) == "" || namespace == "" || len(namespace) > 31 || strings.ContainsAny(namespace, "/ \t\r\n") {
		return nil, fmt.Errorf("aerospike provider requires host, port (1..65535), and namespace (1..31 bytes)")
	}
	if strings.TrimSpace(c.Location) == "" || len(c.Location) > 63 || strings.ContainsRune(c.Location, 0) {
		return nil, fmt.Errorf("aerospike set name must contain 1..63 bytes")
	}
	// Zero and 0xffffffff have special server meanings; never silently turn a
	// fractional TTL into namespace-default expiry or an eternal cache entry.
	if c.TTL <= 0 || c.TTL%time.Second != 0 || c.TTL/time.Second >= time.Duration(^uint32(0)) {
		return nil, fmt.Errorf("aerospike TTL must be whole positive seconds below 4294967295")
	}
	for name, value := range map[string]int{"maxRetries": c.Timeout.MaxRetries, "totalTimeoutInMs": c.Timeout.TotalTimeoutMs, "socketTimeoutInMs": c.Timeout.SocketTimeoutMs, "sleepBetweenRetriesInMs": c.Timeout.SleepBetweenRetriesMs, "failedRequestLimit": c.FailedRequestLimit, "resetFailuresInMs": c.ResetFailuresInMs} {
		if value < 0 || name != "maxRetries" && name != "failedRequestLimit" && uint64(value) > uint64((1<<63-1)/int64(time.Millisecond)) {
			return nil, fmt.Errorf("aerospike %s is invalid", name)
		}
	}
	if (c.FailedRequestLimit == 0) != (c.ResetFailuresInMs == 0) {
		return nil, fmt.Errorf("aerospike failedRequestLimit and resetFailuresInMs must both be positive or both zero")
	}
	return &provider{host: strings.ToLower(u.Hostname()), port: port, namespace: namespace}, nil
}

func (c Config) keyPrefix() string {
	if c.Identity == "" {
		return ""
	}
	return fmt.Sprintf("%x/", sha256.Sum256([]byte(c.Identity)))
}
