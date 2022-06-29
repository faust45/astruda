package index

import (
	"bytes"
	// query "github.com/faust45/astruda/query"
	"github.com/faust45/astruda/updates"
	utils "github.com/faust45/astruda/utils"
	bolt "go.etcd.io/bbolt"
	"log"
)

type IndexWriterFn func(IndexEntry, []byte) error
type IndexCleanerFn func([]byte) error
type IndexFn func([]byte) (IndexEntry, error)

type Index struct {
	Name        string
	NestedLevel int
	Fun         IndexFn
}

type IndexEntry struct {
	Scope [][]byte
	Keys  [][]byte
}

var (
	indexRoot = []byte("indexes")
)

func (index Index) UpdateInBatch(tx *bolt.Tx, batchSize int) (bool, error) {
	log.Printf("updateInBatch start")
	done := false

	updatesIter := updates.RecentUpdatesIter(tx, index.Name, 500)
	count, err := index.update(tx, updatesIter)

	if err != nil {
		return false, err
	}

	// In case we processed all batch we need to try process next one,
	// so we keep done = false
	if count < batchSize {
		done = true
	}

	log.Printf("updateInBatch success")
	return done, nil
}

func (index Index) update(tx *bolt.Tx, updatesIter utils.IterKeys) (int, error) {
	updateIndex, cleanIndex := index.Writer(tx)
	bdocs := tx.Bucket([]byte("docs"))

	count, err := updatesIter(func(key []byte) error {
		data := bdocs.Get(key)
		if data != nil {
			ientry, err := index.Fun(data)
			if err != nil {
				log.Printf("Err index Fun %s %s", err, data)
				return err
			}

			return updateIndex(ientry, key)
		}

		//in case doc was deleted
		return cleanIndex(key)
	})

	return count, err
}

func (idx Index) Bucket(tx *bolt.Tx) *bolt.Bucket {
	b := tx.Bucket(indexRoot)
	return b.Bucket([]byte(idx.Name))
}

func (idx Index) Writer(tx *bolt.Tx) (IndexWriterFn, IndexCleanerFn) {
	rbucketName := []byte("reverse/" + idx.Name)
	broot, _ := tx.CreateBucketIfNotExists(indexRoot)
	b, _ := broot.CreateBucketIfNotExists([]byte(idx.Name))
	rb, _ := broot.CreateBucketIfNotExists(rbucketName)

	cleaner := func(docId []byte) error {
		bytes := rb.Get(docId)

		if bytes != nil {
			entry := unmarshalIndexEntry(bytes)
			bx := b
			for _, key := range entry.Scope {
				bx, _ = bx.CreateBucketIfNotExists(key)
			}

			for _, key := range entry.Keys {
				err := bx.Delete(key)
				if err != nil {
					return err
				}
			}
		}

		return nil
	}

	writer := func(entry IndexEntry, docId []byte) error {
		log.Printf("writer")
		bx := b
		cleaner(docId)

		for _, key := range entry.Scope {
			bx, _ = bx.CreateBucketIfNotExists(key)
		}

		for _, key := range entry.Keys {
			err := bx.Put(key, docId)
			if err != nil {
				return err
			}
		}

		return rb.Put(docId, entry.marshal())
	}

	return writer, cleaner
}

func (entry IndexEntry) marshal() []byte {
	buf := new(bytes.Buffer)
	utils.MarshalSlice(buf, entry.Scope)
	utils.MarshalSlice(buf, entry.Keys)

	return buf.Bytes()
}

func unmarshalIndexEntry(bytes []byte) IndexEntry {
	var position int
	entry := IndexEntry{}

	entry.Scope, position = utils.UnmarshalSlice(bytes, 0)
	entry.Keys, _ = utils.UnmarshalSlice(bytes, position)

	return entry
}
