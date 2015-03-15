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
	"sort"
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

	// how many bits we are using to store each register value
	bitsPerRegister uint32

	p uint8
	m uint32

	// p' and m'
	pp uint8
	mp uint32

	hasher        hash.Hash64
	defaultHasher bool
}

// Approximate size in bytes of h (used for testing).
func (h *HLLPP) memSize() int {
	return cap(h.data) + 4*cap(h.tmpSet) + 4 + 1 + 4 + 1 + 4 + 4
}

// New creates a HyperLogLog++ estimator with p=14, p'=25, and sha1
// hashing function.
func New() *HLLPP {
	h, _ := NewWithConfig(Config{})
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

func (w *hashWrapper) Sum64() uint64 {
	w.buf = w.Sum(w.buf[0:0])
	return binary.BigEndian.Uint64(w.buf)
}

// NewWithConfig creates a HyperLogLog++ estimator with the given Config.
func NewWithConfig(c Config) (*HLLPP, error) {
	if c.Precision == 0 {
		c.Precision = 14
	}

	if c.SparsePrecision == 0 {
		c.SparsePrecision = 25
	}

	defaultHasher := false
	if c.Hasher == nil {
		defaultHasher = true
		c.Hasher = sha1.New()
	}

	p, pp := c.Precision, c.SparsePrecision
	if p < 4 || p > 16 || pp < p || pp > 25 {
		return nil, fmt.Errorf("invalid precision (p: %d, p': %d)", p, pp)
	}

	if c.Hasher.Size() < 8 {
		return nil, errors.New("Hasher.Size() is less than 8, pick something else")
	}

	var hasher hash.Hash64
	if v, ok := c.Hasher.(hash.Hash64); ok {
		hasher = v
	} else {
		hasher = &hashWrapper{Hash: c.Hasher}
	}

	return &HLLPP{
		p:             p,
		pp:            pp,
		m:             1 << p,
		mp:            1 << pp,
		sparse:        true,
		hasher:        hasher,
		defaultHasher: defaultHasher,
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

			// is sparse data bigger than dense data would be?
			if uint32(len(h.data))*8 >= 6*h.m {
				h.toNormal()
			}
		}
	} else {
		idx := uint32(sliceBits64(x, 63, 64-h.p))
		rho := rho(x<<h.p | 1<<(h.p-1))

		if rho > 31 && h.bitsPerRegister == 5 {
			h.bitsPerRegister = 6
			newData := make([]byte, h.m*h.bitsPerRegister/8)
			for i := uint32(0); i < h.m; i++ {
				setRegister(newData, 6, i, getRegister(h.data, 5, i))
			}
			h.data = newData
		}

		if rho > getRegister(h.data, h.bitsPerRegister, idx) {
			setRegister(h.data, h.bitsPerRegister, idx, rho)
		}
	}
}

func (h *HLLPP) toNormal() {
	if h.bitsPerRegister == 0 {
		h.bitsPerRegister = 5
	}

	newData := make([]byte, h.m*h.bitsPerRegister/8)

	reader := newSparseReader(h.data)
	for !reader.Done() {
		idx, rho := h.decodeHash(reader.Next(), h.p)

		if rho > 31 && h.bitsPerRegister == 5 {
			h.bitsPerRegister = 6
			h.toNormal()
			return
		}

		if rho > getRegister(newData, h.bitsPerRegister, idx) {
			setRegister(newData, h.bitsPerRegister, idx, rho)
		}
	}

	h.data = newData
	h.tmpSet = nil
	h.sparse = false
}

// create a mask of numOnes 1's, shifted left shift bits
func mask(numOnes, shift uint32) uint32 {
	return ((1 << numOnes) - 1) << shift
}

func setRegister(data []byte, bitsPerRegister, idx uint32, rho uint8) {
	bitIdx := idx * bitsPerRegister
	byteOffset := bitIdx / 8
	bitOffset := bitIdx % 8

	if 8-bitOffset >= bitsPerRegister {
		// can all fit in first byte

		leftShift := 8 - bitsPerRegister - bitOffset

		// clear existing register value
		data[byteOffset] &= ^byte(mask(bitsPerRegister, leftShift))
		data[byteOffset] |= rho << leftShift
	} else {
		// spread over two bytes

		numBitsInFirstByte := bitsPerRegister - (8 - bitOffset)

		data[byteOffset] &= ^byte(mask(8-bitOffset, 0))
		data[byteOffset] |= rho >> numBitsInFirstByte

		data[byteOffset+1] &= ^byte(mask(numBitsInFirstByte, 8-numBitsInFirstByte))
		data[byteOffset+1] |= rho << (8 - numBitsInFirstByte)
	}
}

func getRegister(data []byte, bitsPerRegister, idx uint32) uint8 {
	bitIdx := idx * bitsPerRegister
	byteOffset := bitIdx / 8
	bitOffset := bitIdx % 8

	if 8-bitOffset >= bitsPerRegister {
		// all fit in first byte
		return (data[byteOffset] >> (8 - bitOffset - bitsPerRegister)) & byte(mask(bitsPerRegister, 0))
	} else {
		// spread over two bytes

		numBitsInFirstByte := bitsPerRegister - (8 - bitOffset)

		rho := data[byteOffset] << numBitsInFirstByte
		rho |= data[byteOffset+1] >> (8 - numBitsInFirstByte)
		return rho & byte(mask(bitsPerRegister, 0))
	}
}

func alpha(m uint32) float64 {
	switch m {
	case 16:
		return 0.673
	case 32:
		return 0.697
	case 64:
		return 0.709
	default:
		return 0.7213 / (1 + 1.079/float64(m))
	}
}

// Count returns the current cardinality estimate for h.
func (h *HLLPP) Count() uint64 {
	if h.sparse {
		h.flushTmpSet()
		return linearCounting(h.mp, h.mp-h.sparseLength)
	}

	var (
		est      float64
		numZeros uint32
	)
	for i := uint32(0); i < h.m; i++ {
		reg := getRegister(h.data, h.bitsPerRegister, i)
		est += 1.0 / float64(uint64(1)<<reg)
		if reg == 0 {
			numZeros++
		}
	}

	if numZeros > 0 {
		lc := linearCounting(h.m, numZeros)
		if lc < threshold[h.p-4] {
			return lc
		}
	}

	est = alpha(h.m) * float64(h.m) * float64(h.m) / est

	if est <= float64(h.m*5) {
		est -= h.estimateBias(est)
	}

	return uint64(est + 0.5)
}

func (h *HLLPP) estimateBias(e float64) float64 {
	estimates := rawEstimateData[h.p-4]
	biases := biasData[h.p-4]

	index := sort.SearchFloat64s(estimates, e)

	if index == 0 {
		return biases[0]
	} else if index == len(estimates) {
		return biases[len(biases)-1]
	}

	e1, e2 := estimates[index-1], estimates[index]
	b1, b2 := biases[index-1], biases[index]

	r := (e - e1) / (e2 - e1)
	return b1*(1-r) + b2*r
}

func linearCounting(m, v uint32) uint64 {
	return uint64(float64(m)*math.Log(float64(m)/float64(v)) + 0.5)
}

func (h *HLLPP) encodeHash(x uint64) uint32 {
	if sliceBits64(x, 63-h.p, 64-h.pp) == 0 {
		r := rho((sliceBits64(x, 63-h.pp, 0) << h.pp) | (1<<h.pp - 1))
		return uint32(sliceBits64(x, 63, 64-h.pp)<<7 | uint64(r<<1) | 1)
	}

	return uint32(sliceBits64(x, 63, 64-h.pp) << 1)
}

// Return index with respect to "p" arg, and rho with respect to h.p. This is so
// the h.pp index can be recovered easily when flushing the tmpSet.
func (h *HLLPP) decodeHash(k uint32, p uint8) (_ uint32, r uint8) {
	if k&1 > 0 {
		r = uint8(sliceBits32(k, 6, 1)) + (h.pp - h.p)
	} else {
		r = rho((uint64(k) | 1) << (64 - (h.pp + 1) + h.p))
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

// slice out inclusive bit section [x.high..x.low]
func sliceBits64(x uint64, high, low uint8) uint64 {
	return (x << (63 - high)) >> (low + (63 - high))
}

// slice out inclusive bit section [x.high..x.low]
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
