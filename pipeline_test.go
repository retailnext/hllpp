package hllpp

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"testing"
)

// Number of iterations to test pipeline marshaling on. A high number here
// ensures that both dense and sparse (explicit) marshaling will be tested.
const iterations = 8000

func TestPipelineMarshal(t *testing.T) {
	var h = New()

	for i := 0; i < iterations; i++ {
		random := make([]byte, 32)
		if _, err := rand.Read(random); err != nil {
			t.Error(err)
		}
		h.Add(random)

		if h.sparse {
			h.flushTmpSet()
		}

		if buf, err := h.AsPipeline(); err != nil {
			t.Error(err)
		} else if err = checkPipelineMarshal(h, buf.Bytes()); err != nil {
			t.Error(err)
		}
	}
}

func checkPipelineMarshal(h *HLLPP, b []byte) error {
	var preamble pipelineHLL

	buf := bytes.NewBuffer(b)
	if err := binary.Read(buf, binary.LittleEndian, &preamble); err != nil {
		return err
	}

	if preamble.Encoding != encodingExplicitClean && preamble.Encoding != encodingDenseClean {
		return fmt.Errorf("unexpected encoding: %c", preamble.Encoding)
	}

	data := make([]byte, preamble.Mlen)
	if n, err := buf.Read(data); err != nil {
		return err
	} else if int32(n) != preamble.Mlen {
		return fmt.Errorf("short read expected %d bytes got %d", preamble.Mlen, n)
	}

	return nil
}
