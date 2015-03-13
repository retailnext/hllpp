// Copyright (c) 2015, RetailNext, Inc.
// All rights reserved.

// hllpp implements the HyperLogLog++ cardinality estimator as specified
// in http://goo.gl/Z5Sqgu.
package hllpp

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"
)

// HLLPP represents a single HyperLogLog++ estimator. Create one via New().
// It is _not_ safe to interact with an HLLPP object from multiple goroutines
// at once.
type HLLPP struct {
	// raw data be it sparse or dense (this makes serialization easier)
	data []byte

	// accumulates unsorted values in sparse mode
	tmpSet uint32Slice

	sparse       bool
	sparseLength uint32

	p uint8
	m uint32

	// p' and m'
	pp uint8
	mp uint32

	hasher hash.Hash64
}

// New creates a HyperLogLog++ estimator with p=14, p'=25, and sha1
// hashing function.
func New() *HLLPP {
	h, _ := NewWithConfig(Config{
		Precision:       14,
		SparsePrecision: 25,
		Hasher:          sha1.New(),
	})
	return h
}

// Config is used to set configurable fields on a HyperLogLog++ via
// NewWithConfig.
type Config struct {
	// Hashing function to apply in Add(). If Hasher implements hash.Hash64,
	// Sum64() will be used, otherwise Sum() will be used.
	Hasher hash.Hash

	// Precision (p). Must be in the range [4..16].
	Precision uint8

	// Precision in sparse mode (p'). Must be in the range [p..25] for this
	// implementation (at 25, sparse mode integers are 32 bits, which makes
	// it easier to use less memory).
	SparsePrecision uint8
}

// wraps a hash.Hash and implements hash.Hash64 on top of it
type hashWrapper struct {
	hash.Hash
	buf []byte
}

func (w hashWrapper) Sum64() uint64 {
	w.buf = w.Sum(w.buf[0:0])
	return binary.BigEndian.Uint64(w.buf)
}

// NewWithConfig creates a HyperLogLog++ estimator with the given Config.
func NewWithConfig(c Config) (*HLLPP, error) {
	p, pp := c.Precision, c.SparsePrecision
	if p < 4 || p > 16 || pp < p || pp > 25 {
		return nil, fmt.Errorf("invalid precision (p: %d, p': %d)", p, pp)
	}

	if c.Hasher == nil {
		return nil, errors.New("must specify Hasher")
	}

	if c.Hasher.Size() < 8 {
		return nil, errors.New("Hasher.Size() is less than 8, pick something else")
	}

	var hasher hash.Hash64
	if v, ok := c.Hasher.(hash.Hash64); ok {
		hasher = v
	} else {
		hasher = hashWrapper{Hash: c.Hasher}
	}

	return &HLLPP{
		p:      p,
		pp:     pp,
		m:      1 << p,
		mp:     1 << pp,
		sparse: true,
		hasher: hasher,
	}, nil
}

// Add will hash b and add the result to the HyperLogLog++ estimator h.
func (h *HLLPP) Add(v []byte) {
	h.hasher.Reset()
	h.hasher.Write(v)
	x := h.hasher.Sum64()

	if h.sparse {
		h.tmpSet = append(h.tmpSet, h.encodeHash(x))

		// is tmpSet >= 1/4 of memory limit?
		if 4*uint32(len(h.tmpSet))*8 >= 6*h.m/4 {
			h.flushTmpSet()
		}
	} else {

	}
}

// Count returns the current cardinality estimate for h.
func (h *HLLPP) Count() uint64 {
	if h.sparse {
		h.flushTmpSet()
		return linearCounting(h.mp, h.mp-h.sparseLength)
	}

	return 0
}

func linearCounting(m, v uint32) uint64 {
	return uint64(float64(m)*math.Log(float64(m)/float64(v)) + 0.5)
}

func (h *HLLPP) encodeHash(x uint64) uint32 {
	if sliceBits64(x, 64-h.p, 64-h.pp) == 0 {
		numZeros := rho((sliceBits64(x, 63-h.pp, 0) << h.pp) | mask(h.pp, 0))
		return uint32((sliceBits64(x, 63, 64-h.pp) << 7) | uint64(numZeros<<1) | 1)
	} else {
		return uint32(sliceBits64(x, 63, 64-h.pp) << 1)
	}
}

// Return index with respect to "p" arg, and rho with respect to h.p. This is so
// the h.pp index can be recovered easily when flushing the tmpSet.
func (h *HLLPP) decodeHash(k uint32, p uint8) (_ uint32, r uint8) {
	if k&1 > 0 {
		r = uint8(sliceBits32(k, 6, 1)) + (h.pp - h.p)
	} else {
		r = rho((uint64(k) | 1) << (63 - (h.pp - h.p - 1)))
	}

	return h.getIndex(k, p), r
}

// Return index with respect to precision "p".
func (h *HLLPP) getIndex(k uint32, p uint8) uint32 {
	if k&1 > 0 {
		return sliceBits32(k, 6+h.pp, 1+6+h.pp-p)
	} else {
		return sliceBits32(k, h.pp, 1+h.pp-p)
	}
}

// create a mask of numOnes 1's, shifted left shift bits
func mask(numOnes, shift uint8) uint64 {
	return ((1 << numOnes) - 1) << shift
}

// slice out bit section [x.from..x.to]
func sliceBits64(x uint64, high, low uint8) uint64 {
	return (x << (63 - high)) >> (low + (63 - high))
}

// slice out bit section [x.from..x.to]
func sliceBits32(x uint32, high, low uint8) uint32 {
	return (x << (31 - high)) >> (low + (31 - high))
}

// number of leading zeros plus 1 (rho as in "ϱ" in paper)
func rho(x uint64) (z uint8) {
	for bit := uint64(1 << 63); bit&x == 0 && bit > 0; bit >>= 1 {
		z++
	}
	return z + 1
}
