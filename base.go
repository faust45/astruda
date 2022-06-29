package astruda

import (
	"encoding/binary"
	"encoding/json"
	idx "github.com/faust45/astruda/index"
	query "github.com/faust45/astruda/query"
	"github.com/faust45/astruda/updates"
	utils "github.com/faust45/astruda/utils"
	bolt "go.etcd.io/bbolt"
	"log"
	time "time"
)

var (
	db      *bolt.DB
	indexes map[string]idx.Index
)

type ID uint64
type Timestamp int64
type Order int
type Num int

type Field interface {
	Bytes() []byte
}

type Doc interface {
	BucketName() string
	ID() ID
	MarshalJson() ([]byte, error)
}

type Conf struct {
	File    string
	Indexes []idx.Index
}

func Open(conf Conf) error {
	var err error
	db, err = bolt.Open(conf.File, 0600, nil)
	if err != nil {
		return err
	}

	return nil
}

// func initIndexes(coll []idx.Index) error {
// 	return db.Update(func(tx *bolt.Tx) error {
// 		for _, index := range coll {
// 			if err := index.init(tx); err != nil {
// 				return err
// 			}
// 		}

// 		return nil
// 	})
// }

func Close() {
	db.Close()
}

func DB() *bolt.DB {
	return db
}

func Save(doc Doc) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("docs"))

		data, err := doc.MarshalJson()
		if err != nil {
			log.Printf("db.Save: Marshal %s", err)
			return err
		}

		key := doc.ID().Bytes()
		err = b.Put(key, data)
		if err != nil {
			log.Printf("db.Save: Put data %s", err)
			return err
		}

		return updates.LogUpdates(tx, doc.ID().Bytes())
	})
}

func Search[T Doc](index idx.Index, q query.Query) ([]T, error) {
	var docs []T

	if err := updateIndex(index); err != nil {
		log.Printf("fail to update index %s, %s", index.Name, err)
		return nil, err
	}

	err := db.View(func(tx *bolt.Tx) error {
		bindex := index.Bucket(tx)
		iter := query.Filter(bindex, q)

		bdocs := tx.Bucket([]byte("docs"))
		iter(func(key []byte) error {
			var doc T

			bytes := bdocs.Get(key)
			json.Unmarshal(bytes, &doc)

			docs = append(docs, doc)
			return nil
		})

		return nil
	})

	if err != nil {
		log.Printf("fail to run search index %s, %s", index.Name, err)
		return nil, err
	}

	return docs, nil
}

func updateIndex(index idx.Index) error {
	for done := false; !done; {
		err := db.Update(func(tx *bolt.Tx) (err error) {
			done, err = index.UpdateInBatch(tx, 500)
			return
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func GenId() ID {
	return ID(time.Now().UnixNano())
}

func (a Num) Bytes() []byte {
	return utils.IntToBytes(int(a))
}

func (id ID) Bytes() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))

	return b
}

func (t Timestamp) Bytes() []byte {
	return utils.Int64ToBytes(int64(t))
}

func Bytes(arr ...Field) [][]byte {
	var acc [][]byte
	for _, v := range arr {
		acc = append(acc, v.Bytes())
	}

	return acc
}

func GetTimestamp() Timestamp {
	return Timestamp(time.Now().UnixNano())
}
