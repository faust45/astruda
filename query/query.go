package query

import (
	// "bytes"
	// "github.com/faust45/astruda/utils"
	bolt "go.etcd.io/bbolt"
	"log"
)

type FilterFn func(*bolt.Bucket, Query, func([]byte) error)
type Query []FilterFn

type KV struct {
	key   []byte
	value []byte
}

func Filter(b *bolt.Bucket, q Query, iter func([]byte) error) {
	if 0 < len(q) {
		fn := q[0]
		fn(b, q[1:], iter)
	}
}

// func filter(b *bolt.Bucket, q Query) []KV {
// 	if 0 < len(q) {
// 		fn := q[0]

// 	}

// 	return coll
// }

// func InSet(kcoll [][]byte) FilterFn {
// 	return func(b *bolt.Bucket) (keys []KV) {
// 		for _, k := range kcoll {
// 			keys = append(keys, KV{k, nil})
// 		}

// 		return
// 	}
// }

func Any() FilterFn {
	return func(b *bolt.Bucket, q Query, iter func([]byte) error) {

		if b != nil {
			// log.Printf("Any if")
			c := b.Cursor()
			k, v := c.First()

			// log.Printf("Any: %v, %v", k, v)

			for k != nil {
				// log.Printf("Any: %s, %s", k, v)
				if len(q) != 0 {
					fn := q[0]
					fn(b.Bucket(k), q[1:], iter)
				} else {
					iter(v)
				}

				k, v = c.Next()
			}
		}
		return
	}
}

func Gt(k []byte) FilterFn {
	return func(b *bolt.Bucket, q Query, iter func([]byte) error) {
		if b != nil {
			c := b.Cursor()

			k, v := c.Seek(k)
			for k != nil {
				if len(q) != 0 {
					fn := q[0]
					log.Printf("Gt: %v, %v", k, v)
					fn(b.Bucket(k), q[1:], iter)
				} else {
					iter(v)
				}

				k, v = c.Next()
			}
		}

		return
	}
}

// func Range(k1 []byte, k2 []byte) FilterFn {
// 	return func(b *bolt.Bucket) (keys []KV) {
// 		if b != nil {
// 			c := b.Cursor()

// 			k, v := c.Seek(k1)
// 			for k != nil && bytes.Compare(k, k2) < 1 {
// 				keys = append(keys, KV{k, v})
// 				k, v = c.Next()
// 			}
// 		}

// 		return
// 	}
// }
