package main

import (
	"fmt"
	"math/rand"
	"strings"
	"tarpluck"
	"time"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	tp, err := tarpluck.New("mytar.tar")
	if err != nil {
		panic(err)
	}

	fmt.Println("Keys:", tp.Keys())

	for _, k := range tp.Keys() {
		b, err := tp.Read(k)
		if err != nil {
			panic(err)
		}
		fmt.Println(k, strings.TrimSpace(string(b)))
	}

	tp.Write(RandStringBytes(12), []byte("test"))

	tp.Close()
}
