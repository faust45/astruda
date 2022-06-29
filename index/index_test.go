package index

import (
	bytes "bytes"
	"github.com/faust45/astruda/utils"
	// bolt "go.etcd.io/bbolt"
	// "os"
	"encoding/json"
	"testing"
)

func TestNestedIndexMarshal(t *testing.T) {
	entry := IndexEntry{
		Scope: [][]byte{utils.IntToBytes(100)},
		Keys:  [][]byte{utils.IntToBytes(815), utils.IntToBytes(18)},
	}

	data := entry.marshal()
	a := unmarshalIndexEntry(data)

	for i, v := range entry.Scope {
		if !bytes.Equal(v, a.Scope[i]) {
			t.Fatalf("keys marshal fails")
		}
	}

	for i, v := range entry.Keys {
		if !bytes.Equal(v, a.Keys[i]) {
			t.Fatalf("keys marshal fails")
		}
	}
}
