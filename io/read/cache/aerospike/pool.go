package aerospike

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go"
)

// Pool owns configured cache clients and failure timers for a service lifetime.
// The zero value is ready to use. Call Close after all reads and warmups drain,
// including on failed startup. Injected caches/clients are never adopted.
// ClientPolicy controls connection setup and the native per-node connection pool;
// set it before first use and do not mutate it while the Pool is in use.
type Pool struct {
	ClientPolicy *as.ClientPolicy
	mu           sync.Mutex
	closed       bool
	clients      map[string]*as.Client
	caches       map[Config]*Cache
}

// NewCache returns the native cache service, sharing clients by seed endpoint and
// cache policy by exact configuration. Failed connects are returned to the caller;
// no background retry or client is retained after a failed connect.
func (p *Pool) NewCache(config Config) (*Cache, error) {
	provider, err := config.validate()
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, fmt.Errorf("aerospike client pool is required")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, fmt.Errorf("aerospike client pool is closed")
	}
	if service := p.caches[config]; service != nil {
		return service, nil
	}
	address := net.JoinHostPort(provider.host, strconv.Itoa(provider.port))
	client := p.clients[address]
	if client == nil {
		policy := as.NewClientPolicy()
		policy.Timeout = time.Second
		if p.ClientPolicy != nil {
			*policy = *p.ClientPolicy
		}
		client, err = as.NewClientWithPolicy(policy, provider.host, provider.port)
		if err != nil {
			if client != nil {
				client.Close()
			}
			return nil, fmt.Errorf("connect aerospike cache: %w", err)
		}
		if client == nil || !client.IsConnected() {
			if client != nil {
				client.Close()
			}
			return nil, fmt.Errorf("aerospike cache client is not connected")
		}
		if p.clients == nil {
			p.clients = make(map[string]*as.Client)
		}
		p.clients[address] = client
	}
	var failure *FailureHandler
	if config.FailedRequestLimit > 0 {
		reset := time.Duration(config.ResetFailuresInMs) * time.Millisecond
		failure = NewFailureHandler(int64(config.FailedRequestLimit), &reset)
	}
	timeout := config.Timeout
	service, err := New(provider.namespace, config.Location, client, uint32(config.TTL/time.Second), &timeout, failure)
	if err != nil {
		if failure != nil {
			_ = failure.Close()
		}
		return nil, err
	}
	service.identityPrefix = config.keyPrefix()
	if p.caches == nil {
		p.caches = make(map[Config]*Cache)
	}
	p.caches[config] = service
	return service, nil
}

// Close is idempotent. It is a service lifetime operation, distinct from Cache.Close
// which finalizes a single cache entry. No new clients can be opened afterward.
func (p *Pool) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	for _, service := range p.caches {
		if service.failureHandler != nil {
			_ = service.failureHandler.Close()
		}
	}
	for _, client := range p.clients {
		client.Close()
	}
	p.caches = nil
	p.clients = nil
	return nil
}
