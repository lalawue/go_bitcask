package main

import (
	"flag"
	"fmt"
	"go_bitcask/bitcask"
	"os"
	"runtime"
)

// Config ... database path
type Config struct {
	dbPath string
}

var config = Config{}

func init() {
	var dbPath string
	if runtime.GOOS == "windows" {
		dbPath = os.Getenv("TEMP") + "/go_bitcask"
	} else {
		dbPath = "/tmp/go_bitcask"
	}
	flag.StringVar(&config.dbPath, "db", dbPath, "database location")
}

func main() {
	flag.Parse()
	fmt.Println("using database path:", config.dbPath)
	b := bitcask.Bitcask{}
	b.OpenDB(config.dbPath)
}
