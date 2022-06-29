package query

import (
	"bytes"
	"github.com/faust45/astruda/index"
	"github.com/faust45/astruda/utils"
	bolt "go.etcd.io/bbolt"
	"log"
	"os"
	"testing"
)

var (
	db *bolt.DB
)

func init() {
	log.SetOutput(os.Stdout)
	log.Printf("Init db")

	path := "./testdb.db"
	os.Remove(path)

	var err error
	db, err = bolt.Open(path, 0600, nil)
	if err != nil {
		log.Printf("fails to open db %s", err)
	}
}

func TestNestedIndexReader(t *testing.T) {
	id := []byte("1111")
	idx := index.Index{Name: "byDate"}
	// entry := index.IndexEntry{
	// 	Scope: [][]byte{[]byte("ada"), utils.IntToBytes(100)},
	// 	Keys:  [][]byte{utils.IntToBytes(3), utils.IntToBytes(15)},
	// }
	entry := index.IndexEntry{
		Scope: [][]byte{utils.IntToBytes(100)},
		Keys:  [][]byte{utils.IntToBytes(3), utils.IntToBytes(15)},
	}

	db.Update(func(tx *bolt.Tx) error {
		windex, _ := idx.Writer(tx)
		return windex(entry, id)
	})

	var coll [][]byte
	iter := func(key []byte) error {
		log.Printf("in iter fun")
		coll = append(coll, key)
		return nil
	}

	q := Query{Gt(utils.IntToBytes(50)), Any()}

	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("indexes"))
		if b == nil {
			t.Fatalf("Bucket indexes doesnt exists")
		}

		bidx := b.Bucket([]byte(idx.Name))

		Filter(bidx, q, iter)
		if len(coll) == 0 {
			t.Fatalf("Query should find some keys")
		}

		if !isContains(coll, id) {
			log.Printf("%+v", coll)
			t.Fatalf("Query should return id %s %+v", id, coll)
		}

		return nil
	})
}

func isContains(coll [][]byte, el []byte) bool {
	for _, v := range coll {
		if bytes.Equal(v, el) {
			return true
		}
	}

	return false
}
