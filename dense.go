// Copyright (c) 2015, RetailNext, Inc.
// All rights reserved.

package hllpp

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
