package mediacache

import (
	"fmt"
	"io"
	"os"
	"testing"
)

func TestCache(t *testing.T) {
	cache := NewCache(&Config{
		BlockSize: 1024,
		CachePath: "./",
	})

	f, err := cache.OpenFile("output.txt", func(start, end int64) (io.ReadCloser, error) {
		f, err := os.Open("./test.txt")
		if err != nil {
			return nil, err
		}

		f.Seek(start, os.SEEK_SET)

		return f, nil
	}, 5000)
	if err != nil {
		t.Fatalf("failed to open cache: %v", err)
	}

	blk, err := f.GetBlock(0)
	if err != nil {
		t.Fatalf("failed to get block: %v", err)
	}

	blk, err = f.GetBlock(1)
	if err != nil {
		t.Fatalf("failed to get block: %v", err)
	}

	blk, err = f.GetBlock(2)
	if err != nil {
		t.Fatalf("failed to get block: %v", err)
	}

	blk, err = f.GetBlock(3)
	if err != nil {
		t.Fatalf("failed to get block: %v", err)
	}

	blk, err = f.GetBlock(4)
	if err != nil {
		t.Fatalf("failed to get block: %v", err)
	}

	fmt.Println(string(blk.Bytes()))

	_ = blk

	err = f.Remove()
	if err != nil {
		t.Fatalf("failed to remove file: %v", err)
	}
}
