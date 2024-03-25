package main

import (
	"bitcask"
	"errors"
	"fmt"
	"time"
)

func main() {
	db, err := bitcask.Open("./bitcask.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	const N int = 1e7

	t1 := time.Now()

	for i := 0; i < N; i++ {
		err := db.Put([]byte(fmt.Sprintf("key-%v", i)), []byte(fmt.Sprintf("value-%v", i)))
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("put1", time.Now().Sub(t1))
	t1 = time.Now()

	for i := 0; i < N; i++ {
		_, err := db.Get([]byte(fmt.Sprintf("key-%v", i)))
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("get1", time.Now().Sub(t1))
	t1 = time.Now()

	for i := 0; i < N; i++ {
		err := db.Delete([]byte(fmt.Sprintf("key-%v", i)))
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("delete1", time.Now().Sub(t1))
	t1 = time.Now()

	for i := 0; i < N; i++ {
		_, err := db.Get([]byte(fmt.Sprintf("key-%v", i)))
		if !errors.Is(err, bitcask.ErrKeyNotFound) {
			panic(err)
		}
	}

	fmt.Println("get2", time.Now().Sub(t1))
	t1 = time.Now()
}
