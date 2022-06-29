package astruda

import (
	bytes "bytes"
	"encoding/json"
	idx "github.com/faust45/astruda/index"
	"github.com/faust45/astruda/utils"
	bolt "go.etcd.io/bbolt"
	"log"
	"os"
	"testing"
)

type Appointment struct {
	Id       ID
	ClientId ID
	SalonId  ID
	Name     string
	Age      Num
}

var (
	indexRoot = "indexes"
	index     = idx.Index{
		Name: "byAge",
		Fun:  byAge,
	}
)

func init() {
	log.SetOutput(os.Stdout)
	log.Printf("init db")

	os.Remove("./test.db")

	collections := []string{"appointments", "users"}
	indexes := []Index{index}
	conf := Conf{
		File:    "./testdb.db",
		Indexes: indexes,
		Coll:    collections,
	}

	err := Open(conf)
	if err != nil {
		log.Printf("fails to open db %s", err)
	}
}

func TestMarshalKeys(t *testing.T) {
	keys := [][]byte{[]byte("astra"), []byte("data"), []byte("ritali")}
	data := marshalKeys(keys)
	keysx := unmarshalKeys(data)

	for i, k := range keysx {
		if !bytes.Equal(keys[i], k) {
			t.Fatalf("keys marshal fails: looking for %s but got %s", keys[i], k)
		}

	}
}

func TestShouldCreateBuckets(t *testing.T) {
	db.View(func(tx *bolt.Tx) error {
		broot := tx.Bucket([]byte("indexes"))
		if broot == nil {
			t.Fatalf("indexes root doesnt exists")
		}

		b := broot.Bucket([]byte(index.Name))
		if b == nil {
			t.Fatalf("indexes %s bucket doesnt exists", index.Name)
		}

		return nil
	})
}

func TestIndexWriterCleaner(t *testing.T) {
	db.Update(func(tx *bolt.Tx) error {
		index := indexes["byAge"]
		windex, _ := indexWriter(tx, index)
		keys := [][]byte{
			[]byte("alisa"),
			[]byte("dali"),
		}

		docId := []byte("1")
		err := windex(keys, docId)
		if err != nil {
			t.Fatalf("index write fails: %s", err)
		}

		broot := tx.Bucket([]byte("indexes"))
		b := broot.Bucket([]byte(index.Name))

		for _, key := range keys {
			key = append(keys[0], docId...)
			if b.Get(key) == nil {
				t.Fatalf("index looks incorret")
			}
		}

		return nil
	})
}

func TestIndexWriter(t *testing.T) {
	db.Update(func(tx *bolt.Tx) error {
		index := indexes["byAge"]
		windex, _ := indexWriter(tx, index)
		keys := [][]byte{
			[]byte("alisa"),
			[]byte("dali"),
		}

		docId := []byte("1")
		err := windex(keys, docId)
		if err != nil {
			t.Fatalf("index write fails: %s", err)
		}

		keys = [][]byte{
			[]byte("ameli"),
			[]byte("111"),
		}

		err = windex(keys, docId)
		if err != nil {
			t.Fatalf("index write fails: %s", err)
		}

		broot := tx.Bucket([]byte("indexes"))
		b := broot.Bucket([]byte(index.Name))

		for _, key := range keys {
			key = append(key, docId...)
			if b.Get(key) == nil {
				t.Fatalf("index looks incorret")
			}
		}

		return nil
	})
}

func byAge(data []byte) (idx.IndexEntry, error) {
	var a Appointment
	err := json.Unmarshal(data, &a)
	if err != nil {
		return idx.IndexEntry{}, err
	}

	return idx.IndexEntry{}, nil
}

func TestNestedIndex(t *testing.T) {
	fn := func(data []byte) (idx.IndexEntry, error) {
		var a Appointment
		err := json.Unmarshal(data, &a)
		if err != nil {
			return idx.IndexEntry{}, err
		}

		return idx.IndexEntry{
			Scope: Bytes(a.SalonId),
			Keys:  Bytes(a.Age),
		}, nil
	}

	id := []byte("1111")
	entry := idx.IndexEntry{
		Scope: [][]byte{[]byte("ada"), utils.IntToBytes(100)},
		Keys:  [][]byte{utils.IntToBytes(815), utils.IntToBytes(18)},
	}

	//should add new index entry
	db.Update(func(tx *bolt.Tx) error {
		windex, _ := index.Writer(tx)
		return windex(entry, id)
	})

	db.View(func(tx *bolt.Tx) error {
		broot := tx.Bucket([]byte(indexRoot))
		b := broot.Bucket([]byte(index.Name))
		l1 := b.Bucket(entry.Scope[0])
		if l1 == nil {
			t.Fatalf("fails add index entry l1 key")
		}
		l2 := l1.Bucket(entry.Scope[1])
		if l2 == nil {
			t.Fatalf("fails add index entry l2 key")
		}
		k1 := l2.Get(entry.Keys[0])
		k2 := l2.Get(entry.Keys[1])
		if k1 == nil || k2 == nil {
			t.Fatalf("fails add l3 key")
		}

		return nil
	})

	entry1 := idx.IndexEntry{
		Scope: [][]byte{[]byte("ada"), utils.IntToBytes(200)},
		Keys:  [][]byte{utils.IntToBytes(815), utils.IntToBytes(18)},
	}

	db.Update(func(tx *bolt.Tx) error {
		windex, _ := index.Writer(tx)
		return windex(entry1, id)
	})

	//sould clean old index entry
	db.View(func(tx *bolt.Tx) error {
		broot := tx.Bucket([]byte(indexRoot))
		b := broot.Bucket([]byte(index.Name))
		l1 := b.Bucket(entry.Scope[0])
		l2 := l1.Bucket(entry.Scope[1])

		k1 := l2.Get(entry.Keys[0])
		k2 := l2.Get(entry.Keys[1])
		if k1 != nil && k2 != nil {
			t.Fatalf("fails clean index entry")
		}

		return nil
	})
}
