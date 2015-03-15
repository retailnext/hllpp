// Copyright (c) 2015, RetailNext, Inc.
// All rights reserved.

package hllpp

import (
	"crypto/md5"
	"crypto/sha1"
	"fmt"
	"reflect"
	"testing"
)

func hllpEqual(h1, h2 HLLPP) bool {
	h1.hasher = nil
	h2.hasher = nil

	return reflect.DeepEqual(h1, h2)
}

func marshalUnmarshal(h *HLLPP) error {
	unmarshaled, err := Unmarshal(h.Marshal())
	if err != nil {
		panic(err)
	}

	if unmarshaled.Count() != h.Count() {
		return fmt.Errorf("mismatched count: got %d, expected %d", unmarshaled.Count(), h.Count())
	}

	if !hllpEqual(*h, *unmarshaled) {
		return fmt.Errorf("got %+v, expected %+v", unmarshaled, h)
	} else {
		return nil
	}
}

func TestMarshal(t *testing.T) {
	h := New()

	if err := marshalUnmarshal(h); err != nil {
		t.Error(err)
	}

	h.Add(intToBytes(1))

	if err := marshalUnmarshal(h); err != nil {
		t.Error(err)
	}

	for i := uint64(0); i < 1000; i++ {
		h.Add(intToBytes(i))
	}

	if !h.sparse {
		t.Error("Expecting sparse")
	}

	if err := marshalUnmarshal(h); err != nil {
		t.Error(err)
	}

	for i := uint64(0); i < 100000; i++ {
		h.Add(intToBytes(i))
	}

	if h.sparse {
		t.Error("Expecting dense")
	}

	if err := marshalUnmarshal(h); err != nil {
		t.Error(err)
	}
}

func TestUnmarshalErrors(t *testing.T) {
	uh, err := Unmarshal(nil)
	if uh != nil || err == nil {
		t.Error("Expected nil hll and some error")
	}

	uh, err = Unmarshal([]byte{})
	if uh != nil || err == nil {
		t.Error("Expected nil hll and some error")
	}

	h := New()
	for i := uint64(0); i < 10000; i++ {
		h.Add(intToBytes(i))
	}
	uh, err = Unmarshal(h.Marshal()[0:100])
	if uh != nil || err == nil {
		t.Error("Expected nil hll and some error")
	}
}

func TestMarshalHasher(t *testing.T) {
	h := New()

	uh, err := UnmarshalWithHasher(h.Marshal(), sha1.New())
	if err == nil {
		t.Error("Expected error about hasher")
	}

	h, err = NewWithConfig(Config{
		Hasher:          md5.New(),
		Precision:       12,
		SparsePrecision: 20,
	})
	if err != nil {
		t.Fatal(err)
	}

	uh, err = Unmarshal(h.Marshal())
	if err == nil {
		t.Error("Expected error about hasher")
	}

	uh, err = UnmarshalWithHasher(h.Marshal(), md5.New())
	if err != nil {
		t.Fatal(err)
	}

	if !hllpEqual(*h, *uh) {
		t.Errorf("Expected %+v, got %+v", *h, *uh)
	}
}
