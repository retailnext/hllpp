// Copyright (c) 2015, RetailNext, Inc.
// All rights reserved.
package hllpp

import (
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

	if h.Count() != 0 {
		t.Errorf("Got %d", h.Count())
	}

	for _, pow := range []int{0, 1, 2, 3} {
		count := uint64(math.Pow10(pow))

		for i := uint64(0); i < count; i++ {
			h.Add(intToBytes(i))
		}

		for i := 0; i < 1000; i++ {
			h.Add(intToBytes(0))
		}

		if h.Count() != count {
			t.Fatalf("Got %d, expected %d (len %d)", h.Count(), count, h.sparseLength)
		}
	}

	if !h.sparse {
		t.Error("should still be sparse")
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

	if r != 2 {
		t.Errorf("got %d", r)
	}

	//                              p            p'
	x = bitsToUint64("11111111 11111000 00000000 01111111 11111111 11111111 11111111 11111111")
	e = h.encodeHash(x)

	// need to encode rho (which is 1 in this case)
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
