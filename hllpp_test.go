// Copyright (c) 2015, RetailNext, Inc.
// All rights reserved.
package hllpp

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"testing"
)

func intToBytes(i uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}

func TestSparse(t *testing.T) {
	h := New()

	hd := New()
	hd.toNormal()
	denseSize := hd.memSize()

	if h.Count() != 0 {
		t.Errorf("Got %d", h.Count())
	}

	for _, count := range []uint64{1, 10, 100, 1000, 5000} {
		for i := uint64(0); i < count; i++ {
			h.Add(intToBytes(i))
		}

		for i := 0; i < 1000; i++ {
			h.Add(intToBytes(0))
		}

		if e := estimateError(h.Count(), count); e > 0.005 {
			t.Errorf("Got %d, expected %d (error of %f)", h.Count(), count, e)
		}

		if h.memSize() > denseSize {
			t.Errorf("Taking up more memory than dense: %d > %d", h.memSize(), int(6*h.m/8))
		}
	}

	if !h.sparse {
		t.Error("should still be sparse")
	}
}

func estimateError(got, exp uint64) float64 {
	var delta uint64
	if got > exp {
		delta = got - exp
	} else {
		delta = exp - got
	}
	return float64(delta) / float64(exp)
}

func TestDense(t *testing.T) {
	h := New()

	for _, pow := range []int{5, 6} {
		count := uint64(math.Pow10(pow))

		for i := uint64(0); i < count; i++ {
			h.Add(intToBytes(i))
		}

		if h.sparse {
			t.Error("shouldn't be sparse")
		}

		if estimateError(h.Count(), count) > 0.01 {
			t.Fatalf("Got %d, expected %d", h.Count(), count)
		}
	}
}

func TestBiasCorrection(t *testing.T) {
	h := New()

	// bias corrected range for p=14 is estimated cardinality ~10k => ~80k, so
	// make sure we have low error for values in this range

	for i := uint64(1); i < 100000; i++ {
		h.Add(intToBytes(i))

		if i%100 == 0 {
			if e := estimateError(h.Count(), i); e > 0.015 {
				t.Errorf("Got %d, expected %d (%f)", h.Count(), i, e)
			}
		}
	}
}

func bitsToUint32(bits string) uint32 {
	bits = strings.Replace(bits, " ", "", -1)
	i, err := strconv.ParseUint(bits, 2, 32)
	if err != nil {
		panic(err)
	}
	return uint32(i)
}

func bitsToUint64(bits string) uint64 {
	bits = strings.Replace(bits, " ", "", -1)
	i, err := strconv.ParseUint(bits, 2, 64)
	if err != nil {
		panic(err)
	}
	return i
}

func uint32ToBits(i uint32) string {
	return strings.TrimLeft(strconv.FormatUint(uint64(i), 2), "0")
}

func uint64ToBits(i uint64) string {
	return strings.TrimLeft(strconv.FormatUint(i, 2), "0")
}

func TestEncodeDecodeHash(t *testing.T) {
	h := New()

	// p is 14, p' is 25

	//                               p            p'
	x := bitsToUint64("11111111 00000000 11111111 00000000 11111111 11111111 11111111 11111111")
	e := h.encodeHash(x)

	// don't need to encode number of zeros
	if e != bitsToUint32("11111111 00000000 11111111 0  0") {
		t.Errorf("got %s", uint32ToBits(e))
	}

	i, r := h.decodeHash(e, h.p)

	if i != bitsToUint32("11111111 000000") {
		t.Errorf("got %s", uint32ToBits(i))
	}

	if r != 3 {
		t.Errorf("got %d", r)
	}

	//                              p            p'
	x = bitsToUint64("11111111 00000011 11111111 00000000 11111111 11111111 11111111 11111111")
	e = h.encodeHash(x)

	// don't need to encode number of zeros
	if e != bitsToUint32("11111111 00000011 11111111 0  0") {
		t.Errorf("got %s", uint32ToBits(e))
	}

	i, r = h.decodeHash(e, h.p)

	if i != bitsToUint32("11111111 000000") {
		t.Errorf("got %s", uint32ToBits(i))
	}

	if r != 1 {
		t.Errorf("got %d", r)
	}

	//                              p            p'
	x = bitsToUint64("11111111 00000010 11111111 00000000 11111111 11111111 11111111 11111111")
	e = h.encodeHash(x)

	// don't need to encode number of zeros
	if e != bitsToUint32("11111111 00000010 11111111 0  0") {
		t.Errorf("got %s", uint32ToBits(e))
	}

	i, r = h.decodeHash(e, h.p)

	if i != bitsToUint32("11111111 000000") {
		t.Errorf("got %s", uint32ToBits(i))
	}

	if r != 1 {
		t.Errorf("got %d", r)
	}

	//                              p            p'
	x = bitsToUint64("11111111 11111000 00000000 01111111 11111111 11111111 11111111 11111111")
	e = h.encodeHash(x)

	// need to encode rho' (which is 1 in this case)
	if e != bitsToUint32("11111111 11111000 00000000 0 000001 1") {
		t.Errorf("got %s", uint32ToBits(e))
	}

	i, r = h.decodeHash(e, h.p)

	if i != bitsToUint32("11111111 111110") {
		t.Errorf("got %s", uint32ToBits(i))
	}

	if r != 12 {
		t.Errorf("got %d", r)
	}

	//                              p            p'
	x = bitsToUint64("11111111 11111000 00000000 00000000 11111111 11111111 11111111 11111111")
	e = h.encodeHash(x)

	// need to encode a bigger rho (7 + 1 = 8)
	if e != bitsToUint32("11111111 11111000 00000000 0 001000 1") {
		t.Errorf("got %s", uint32ToBits(e))
	}

	i, r = h.decodeHash(e, h.p)

	if i != bitsToUint32("11111111 111110") {
		t.Errorf("got %s", uint32ToBits(i))
	}

	if r != 19 {
		t.Errorf("got %d", r)
	}

	// edge case with lots of zeros

	//                              p            p'
	x = bitsToUint64("00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000")
	e = h.encodeHash(x)

	if e != bitsToUint32("00000000 00000000 00000000 0 101000 1") {
		t.Errorf("got %s", uint32ToBits(e))
	}

	i, r = h.decodeHash(e, h.p)

	if i != bitsToUint32("00000000 000000") {
		t.Errorf("got %s", uint32ToBits(i))
	}

	if r != 51 {
		t.Errorf("got %d", r)
	}
}

func TestSliceBits(t *testing.T) {
	n := bitsToUint32("11111111 11111111 11111111 11111111")

	if s := uint32ToBits(sliceBits32(n, 31, 24)); s != "11111111" {
		t.Errorf("got %s", s)
	}

	n = bitsToUint32("11111111 00000000 11111111 00000000")

	if s := uint32ToBits(sliceBits32(n, 27, 20)); s != "11110000" {
		t.Errorf("got %s", s)
	}

	n = bitsToUint32("11111111 00000000 11111111 10101010")

	if s := uint32ToBits(sliceBits32(n, 5, 1)); s != "10101" {
		t.Errorf("got %s", s)
	}
}

func TestRho(t *testing.T) {
	if r := rho(0); r != 65 {
		t.Errorf("got %d", r)
	}

	if r := rho(1 << 63); r != 1 {
		t.Errorf("got %d", r)
	}

	if r := rho(1 << 60); r != 4 {
		t.Errorf("got %d", r)
	}
}

func bitsToBytes(bits string) []byte {
	bits = strings.Replace(bits, " ", "", -1)
	if len(bits)%8 != 0 {
		panic("bits not multiple of 8")
	}
	ret := make([]byte, len(bits)/8)
	for i := 0; i < len(bits)/8; i++ {
		b, err := strconv.ParseUint(bits[i*8:(i+1)*8], 2, 64)
		if err != nil {
			panic(err)
		}
		ret[i] = byte(b)
	}

	return ret
}

func bytesToBits(data []byte) string {
	bitStrs := make([]string, len(data))

	for i, b := range data {
		bitStrs[i] = strconv.FormatUint(uint64(b), 2)
	}

	return strings.Join(bitStrs, " ")
}

func TestRegisterPacking(t *testing.T) {
	data := make([]byte, 3)

	setRegister(data, 6, 0, 3)
	if !bytes.Equal(data, bitsToBytes("00001100 00000000 00000000")) {
		t.Errorf("got %s", bytesToBits(data))
	}
	if v := getRegister(data, 6, 0); v != 3 {
		t.Errorf("got %d", v)
	}

	setRegister(data, 6, 1, 38)
	if !bytes.Equal(data, bitsToBytes("00001110 01100000 00000000")) {
		t.Errorf("got %s", bytesToBits(data))
	}
	if v := getRegister(data, 6, 1); v != 38 {
		t.Errorf("got %d", v)
	}

	setRegister(data, 6, 2, 63)
	if !bytes.Equal(data, bitsToBytes("00001110 01101111 11000000")) {
		t.Errorf("got %s", bytesToBits(data))
	}
	if v := getRegister(data, 6, 2); v != 63 {
		t.Errorf("got %d", v)
	}

	setRegister(data, 6, 3, 30)
	if !bytes.Equal(data, bitsToBytes("00001110 01101111 11011110")) {
		t.Errorf("got %s", bytesToBits(data))
	}
	if v := getRegister(data, 6, 3); v != 30 {
		t.Errorf("got %d", v)
	}

	// sanity other ones are still set correctly
	if v := getRegister(data, 6, 0); v != 3 {
		t.Errorf("got %d", v)
	}
	if v := getRegister(data, 6, 1); v != 38 {
		t.Errorf("got %d", v)
	}
	if v := getRegister(data, 6, 2); v != 63 {
		t.Errorf("got %d", v)
	}

	// don't forget to unset bits when updating
	setRegister(data, 6, 0, 0)
	if !bytes.Equal(data, bitsToBytes("00000010 01101111 11011110")) {
		t.Errorf("got %s", bytesToBits(data))
	}
	if v := getRegister(data, 6, 0); v != 0 {
		t.Errorf("got %d", v)
	}

	setRegister(data, 6, 2, 0)
	if !bytes.Equal(data, bitsToBytes("00000010 01100000 00011110")) {
		t.Errorf("got %s", bytesToBits(data))
	}
	if v := getRegister(data, 6, 2); v != 0 {
		t.Errorf("got %d", v)
	}

	// try bit length other than 6
	data = make([]byte, 3)

	setRegister(data, 5, 0, 31)
	if !bytes.Equal(data, bitsToBytes("11111000 00000000 00000000")) {
		t.Errorf("got %s", bytesToBits(data))
	}
	if v := getRegister(data, 5, 0); v != 31 {
		t.Errorf("got %d", v)
	}

	setRegister(data, 5, 1, 15)
	if !bytes.Equal(data, bitsToBytes("11111011 11000000 00000000")) {
		t.Errorf("got %s", bytesToBits(data))
	}
	if v := getRegister(data, 5, 1); v != 15 {
		t.Errorf("got %d", v)
	}

	setRegister(data, 5, 2, 7)
	if !bytes.Equal(data, bitsToBytes("11111011 11001110 00000000")) {
		t.Errorf("got %s", bytesToBits(data))
	}
	if v := getRegister(data, 5, 2); v != 7 {
		t.Errorf("got %d", v)
	}

	// sanity others are still set
	if v := getRegister(data, 5, 0); v != 31 {
		t.Errorf("got %d", v)
	}
	if v := getRegister(data, 5, 1); v != 15 {
		t.Errorf("got %d", v)
	}
}
