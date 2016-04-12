package hllpp

import (
	"bytes"
	"encoding/binary"
)

type pipelineHLL struct {
	Encoding byte
	_        [3]byte
	Card     uint64
	P        uint8
	_        [3]byte
	Mlen     int32
	// M is variable-length, and must be dealt with separately.
}

const (
	encodingDenseDirty    = 'd'
	encodingDenseClean    = 'D'
	encodingExplicitDirty = 'e'
	encodingExplicitClean = 'E'
)

// Converts dense or sparse data structure to clean Pipeline format (0.8.5
// vintage on x64)
func (h *HLLPP) AsPipeline() (bytes.Buffer, error) {
	p := pipelineHLL{Card: h.Count()}

	// Always use the clean encoding, as we have just calculated the
	// cardinality.
	if h.sparse {
		h.flushTmpSet()

		p.Encoding = encodingExplicitClean
		p.P = uint8(h.pp)
		p.Mlen = int32(h.sparseLength * 4)
	} else {
		p.Encoding = encodingDenseClean
		p.P = h.p
		p.Mlen = int32(len(h.data))
	}

	// Write the size-invariant preamble.
	var ret bytes.Buffer
	if err := binary.Write(&ret, binary.LittleEndian, &p); err != nil {
		return ret, err
	}

	if h.sparse {
		// Retailnext sparse encoding is not the same as Pipeline's SPARSE
		// encoding. It's actually the EXPLICIT encoding in Pipeline.
		output := h.sparseToExplicit()
		if err := binary.Write(&ret, binary.LittleEndian, &output); err != nil {
			return ret, err
		}
	} else {
		// Dense representation is fully compatible as-is.
		if _, err := ret.Write(h.data); err != nil {
			return ret, err
		}
	}

	return ret, nil
}

// Converts retailnext (index, rho) tuples from its encoding named 'sparse'
// into the 'explicit' encoding used by Pipeline, which is the same but packed
// into a uint32. It also does not use the 'difference encoding' described in
// the paper, as retailnext's implementation does.
func (h *HLLPP) sparseToExplicit() []uint32 {
	reader := newSparseReader(h.data)
	var output []uint32
	for !reader.Done() {
		idx, r := h.decodeHash(reader.Next(), h.pp)
		if r > 0xff {
			panic("register value would be truncated")
		} else if idx > 0xffffff {
			panic("register index would be truncated")
		}
		// 24 bits of register ID, 8 bits of register value.
		output = append(output, (idx<<8)|uint32(r&0xff))
	}
	return output
}
