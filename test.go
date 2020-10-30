package main

import (
	"flag"
	"fmt"
	"go_bitcask/bitcask"
	"os"
	"runtime"
)

// Config ... database path
var config = bitcask.Config{}
var maxFileSize uint = 512 // 512 byte

func init() {
	var dbPath string
	if runtime.GOOS == "windows" {
		dbPath = os.Getenv("TEMP") + "/go_bitcask"
	} else {
		dbPath = "/tmp/go_bitcask"
	}
	flag.StringVar(&config.Path, "path", dbPath, "database location")
	flag.StringVar(&config.BucketName, "name", "0", "database bucket name")
	flag.UintVar(&maxFileSize, "size", maxFileSize, "database max data size")
}

func main() {
	flag.Parse()
	config.DataFileSize = uint32(maxFileSize)
	fmt.Printf("using database: %+v\n", config)
	// open database
	b := bitcask.Bitcask{}
	db, err := b.OpenDB(&config)
	if err != nil {
		fmt.Printf("failed to open database, %s\n", err.Error())
		return
	}

	// 1. set
	err = db.Set("a", []byte("b"))
	if err != nil {
		fmt.Printf("failed to set database: %s\n", err.Error())
		return
	}

	// 2. get
	val, err := db.Get("a")
	if err != nil {
		fmt.Printf("failed to get database: %s\n", err.Error())
		return
	}
	fmt.Printf("get '%s'\n", string(val))

	// 3. del
	err = db.Remove("a")
	if err != nil {
		fmt.Printf("failed to delete: %s\n", err.Error())
		return
	}

	// 4. get again
	val, err = db.Get("a")
	if err != nil {
		fmt.Printf("failed to get database: %s\n", err.Error())
		return
	}
	fmt.Printf("get '%s'\n", string(val))
}
