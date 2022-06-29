package utils

import (
	"bytes"
	"encoding/binary"
)

type IterKeys func(func([]byte) error) (int, error)

func Int64ToBytes(num int64) []byte {
	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.BigEndian, num)
	if err != nil {
	}

	return buff.Bytes()
}

func Uint64ToB(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func IntToBytes(num int) []byte {
	b := make([]byte, 4)
	b[0] = byte(num)
	b[1] = byte(num >> 8)
	b[2] = byte(num >> 16)
	b[3] = byte(num >> 24)

	return b
}

func BytesToInt(b []byte) int {
	return int(b[0]) | int(b[1])<<8 | int(b[2])<<16 | int(b[3])<<24
}

// func bytesToId(b []byte) ID {
// 	return ID(binary.LittleEndian.Uint64(b))
// }

func MarshalSlice(buf *bytes.Buffer, slice [][]byte) {
	l := IntToBytes(len(slice))
	buf.Write(l)
	for _, key := range slice {
		l = IntToBytes(len(key))
		buf.Write(l)
		buf.Write(key)
	}
}

func UnmarshalSlice(bytes []byte, position int) ([][]byte, int) {
	// log.Printf("debug: %d, %d", position, len(bytes))
	c := BytesToInt(bytes[position : position+4])
	position += 4
	var keys [][]byte
	for i := 0; i < c; i++ {
		l := BytesToInt(bytes[position : position+4])
		position += 4
		key := bytes[position : position+l]
		keys = append(keys, key)
		position += l
	}
	return keys, position
}
