

# About

go_bitcask was a Key/Value store for Go, uses [Bitcask](https://en.wikipedia.org/wiki/Bitcask)  on-disk layout.

only test in MacOS/Linux.

# Example

```Go
import (
	"fmt"
	"go_bitcask/bitcask"
)

var config = bitcask.Config{
	path:        "/tmp/go_bitcask",
	BucketName:  "0",
	maxFileSize: 64*1024*1024,
}

// open database
db, err := bitcask.OpenDB(&config)
if err != nil {
	fmt.Printf("failed to open database, %s\n", err.Error())
	return
}

db.ChangeBucket("hello")

err := db.Set("a", []byte("b"))
if err != nil {
	return err
}

val, err := db.Get("a")
if err != nil {
	return err
}

err := db.Remove("a)
if err != nil {
	return err
}

err := db.GC("hello")
if err != nil {
	return err
}

db.closeDB()
```

# Test

```bash
$ go run test.go
PASS Bucket
PASS Set/Get
PASS Delete
PASS GC
```
