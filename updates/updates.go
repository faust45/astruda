package updates

import (
	utils "github.com/faust45/astruda/utils"
	bolt "go.etcd.io/bbolt"
)

func LogUpdates(tx *bolt.Tx, docId []byte) error {
	b := tx.Bucket([]byte("updates"))
	rb := b.Bucket([]byte("reverse"))

	oldkey := rb.Get(docId)
	err := b.Delete(oldkey)
	if err != nil {
		return err
	}

	nextSeq, err := b.NextSequence()
	if err != nil {
		return err
	}

	key := utils.Uint64ToB(nextSeq)
	err = b.Put(key, docId)
	if err != nil {
		return err
	}

	return rb.Put(docId, key)
}

func RecentUpdatesIter(tx *bolt.Tx, indexName string, batchSize int) utils.IterKeys {
	c := tx.Bucket([]byte("updates")).Cursor()
	lastKey, writeLastUpdate := lastUpdate(tx, indexName)

	var key, id []byte
	if lastKey != nil {
		c.Seek(lastKey)
		key, id = c.Next()
	} else {
		key, id = c.First()
	}

	return func(fn func([]byte) error) (int, error) {
		for i := 1; i != batchSize; i++ {
			if key == nil {
				return i, nil
			}

			err := fn(id)
			if err != nil {
				return i, err
			}

			key, id = c.Next()
		}

		return batchSize, writeLastUpdate(key)
	}
}

func lastUpdate(tx *bolt.Tx, indexName string) ([]byte, func([]byte) error) {
	k := []byte("lastUpdate")
	meta := tx.Bucket([]byte("meta")).Bucket([]byte("index/" + indexName))
	key := meta.Get(k)

	write := func(key []byte) error {
		return meta.Put(k, key)
	}

	return key, write
}
