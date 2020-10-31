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

	// test bucket
	err = testBucket(db, &config)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Pass Bucket")

	count := 256
	value := "abcdefghijklmnopqrstuvwxyz"

	// test get set
	err = testGetSet(db, count, value)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("PASS Set/Get")

	// test delete
	err = testDelete(db, count)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("PASS Delete")

	// test GC
	err = testGC(db, &config, count, value)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("PASS GC")
}

func testBucket(db *bitcask.Bitcask, config *bitcask.Config) error {
	// test bucket
	db.ChangeBucket("hello")
	db.Set("a", []byte("b"))

	_, err := os.Stat(config.Path + "/hello/0000000000.dat")
	if err != nil {
		return fmt.Errorf("failed to change bucket")
	}

	val, err := db.Get("a")
	if err != nil || string(val) != "b" {
		return fmt.Errorf("failed to get bucket value")
	}

	db.ChangeBucket("0")
	val, err = db.Get("a")
	if err != nil || val != nil {
		return fmt.Errorf("invalid bucket namespace")
	}

	return nil
}

func testGetSet(db *bitcask.Bitcask, count int, value string) error {

	for i := 0; i < count; i++ {
		name := fmt.Sprintf("%d", i)
		db.Set(name, []byte(value+name))
	}

	for i := 0; i < count; i++ {
		name := fmt.Sprintf("%d", i)
		val, err := db.Get(name)
		if err != nil {
			return err
		}
		if string(val) != (value + name) {
			return fmt.Errorf("Invalid get %d, %s", i, string(val))
		}
	}

	return nil
}

func testDelete(db *bitcask.Bitcask, count int) error {

	for i := 0; i < count; i += 2 {
		name := fmt.Sprintf("%d", i)
		err := db.Remove(name)
		if err != nil {
			return err
		}
	}

	for i := 0; i < count; i += 2 {
		name := fmt.Sprintf("%d", i)
		val, err := db.Get(name)
		if err != nil {
			return err
		} else if val != nil {
			return fmt.Errorf("failed to get %s", name)
		}
	}

	return nil
}

func testGC(db *bitcask.Bitcask, config *bitcask.Config, count int, value string) error {
	err := db.GC("0")
	if err != nil {
		return err
	}
	db.CloseDB()

	b := bitcask.Bitcask{}
	db, err = b.OpenDB(config)
	if err != nil {
		return err
	}

	for i := 0; i < count; i++ {
		name := fmt.Sprintf("%d", i)
		val, err := db.Get(name)
		if err != nil {
			return err
		}
		if i%2 == 0 && val != nil {
			return fmt.Errorf("after gc, got deleted entry %s", name)
		} else if i%2 == 1 && string(val) != (value+name) {
			return fmt.Errorf("after gc, got invalid entry %s", name)
		}
	}
	return nil
}
