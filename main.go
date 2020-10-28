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
var maxFileSize uint = 64 * 1024 * 1024

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
	err = db.Set("a", []byte("b"))
	if err != nil {
		fmt.Printf("failed to set database: %s\n", err.Error())
		return
	}
	val, err := db.Get("a")
	if err != nil {
		fmt.Printf("failed to get database: %s\n", err.Error())
		return
	}
	fmt.Printf("get '%s'\n", string(val))
}
