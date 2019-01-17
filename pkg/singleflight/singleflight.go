// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package singleflight provides a duplicate function call suppression
// mechanism.
package singleflight

import (
	"bytes"
	"sync"
)

// call is an in-flight or completed singleflight.DoChan call
type call struct {
	wg sync.WaitGroup

	// These fields are written once before the WaitGroup is done
	// and are only read after the WaitGroup is done.
	val interface{}
	err error

	// These fields are read and written with the singleflight
	// mutex held before the WaitGroup is done, and are read but
	// not written after the WaitGroup is done.
	dups  int
	chans []chan<- Result

	// Read and written with the singleflight mutex held.
	nonce []byte
}

// Group represents a class of work and forms a namespace in
// which units of work can be executed with duplicate suppression.
type Group struct {
	mu sync.Mutex       // protects m
	m  map[string]*call // lazily initialized
}

// Result holds the results of DoChan, so they can be passed
// on a channel.
type Result struct {
	Val    interface{}
	Err    error
	Shared bool
}

// DoChan executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
// Returns a channel that will receive the results when they are ready.
// The Result value shared indicates whether v was given to multiple callers.
// Returns false if the specified nonce doesn't match the nonce of a call already in progress.
func (g *Group) DoChan(key string, nonce []byte, fn func() (interface{}, error)) (<-chan Result, bool) {
	ch := make(chan Result, 1)
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		if x, y := len(c.nonce), len(nonce); x != y || bytes.Compare(c.nonce, nonce) != 0 {
			g.mu.Unlock()
			return nil, false
		}
		c.dups++
		c.chans = append(c.chans, ch)
		g.mu.Unlock()
		return ch, true
	}
	c := &call{chans: []chan<- Result{ch}, nonce: nonce}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	go g.doCall(c, key, fn)

	return ch, true
}

// doCall handles the single call for a key.
func (g *Group) doCall(c *call, key string, fn func() (interface{}, error)) {
	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	for _, ch := range c.chans {
		ch <- Result{c.val, c.err, c.dups > 0}
	}
	g.mu.Unlock()
}
