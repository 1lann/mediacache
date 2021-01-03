package mediacache

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edsrzf/mmap-go"
)

type Config struct {
	CachePath string
	BlockSize int64
}

type Block struct {
	mapped  []byte
	written int64
	once    *sync.Once
	err     error
	mutex   *sync.RWMutex
}

type File struct {
	pathToFile  string
	size        int64
	blockSize   int64
	blocks      []*Block
	fetcher     func(start int64, end int64) (io.ReadCloser, error)
	failedBlock int32 // defaults to -1, set to the block number of a permanently failed block.
	mapError    error
	allocated   *sync.Once
	mapping     *mmap.MMap
	handle      *os.File
	mutex       *sync.RWMutex
}

func Open(pathToFile string, blockSize int64, fetcher func(start int64, end int64) (io.ReadCloser, error), size int64) (*File, error) {
	file := &File{
		pathToFile:  pathToFile,
		size:        size,
		blockSize:   blockSize,
		fetcher:     fetcher,
		failedBlock: -1,
		allocated:   new(sync.Once),
		mutex:       new(sync.RWMutex),
		mapError:    nil,
		mapping:     nil,
		handle:      nil,
	}

	err := file.allocate()
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (f *File) Remove() error {
	f.mutex.Lock()
	err1 := f.mapping.Unmap()
	err2 := f.handle.Close()
	f.mapError = errors.New("mediacache: file closed")
	f.mutex.Unlock()

	for _, block := range f.blocks {
		block.mutex.Lock()
		block.mapped = nil
		block.err = errors.New("mediacache: file closed")
		block.mutex.Unlock()
	}

	if err1 != nil {
		return err1
	} else if err2 != nil {
		return err2
	}

	return os.Remove(f.pathToFile)
}

// Returns a copy of the bytes in the block.
// If the file had been removed, returns nil
func (b *Block) Bytes() []byte {
	b.mutex.RLock()
	out := make([]byte, len(b.mapped))
	copy(out, b.mapped)
	b.mutex.RUnlock()

	return out
}

func (f *File) allocate() error {
	f.allocated.Do(func() {
		// allocate the file
		var err error
		f.handle, err = os.Create(f.pathToFile)
		if err != nil {
			f.mapError = fmt.Errorf("mediacache: failed to create file %q: %w", f.pathToFile, err)
			return
		}

		err = f.handle.Truncate(f.size)
		if err != nil {
			f.handle.Close()
			f.mapError = fmt.Errorf("mediacache: failed to allocate file %q: %w", f.pathToFile, err)
			os.Remove(f.pathToFile)
			return
		}

		mapping, err := mmap.Map(f.handle, mmap.RDWR, 0)
		if err != nil {
			f.handle.Close()
			f.mapError = fmt.Errorf("mediacache: failed to map file %q: %w", f.pathToFile, err)
			os.Remove(f.pathToFile)
			return
		}

		f.mapping = &mapping

		numBlocks := f.size / f.blockSize
		if f.size%f.blockSize != 0 {
			numBlocks++
		}

		f.blocks = make([]*Block, numBlocks)

		for i := range f.blocks {
			upper := (int64(i) + 1) * f.blockSize
			if upper > f.size {
				upper = f.size
			}

			f.blocks[i] = &Block{
				mapped:  mapping[int64(i)*f.blockSize : upper],
				written: 0,
				once:    new(sync.Once),
				err:     nil,
				mutex:   new(sync.RWMutex),
			}
		}
	})

	f.mutex.RLock()
	err := f.mapError
	f.mutex.RUnlock()

	return err
}

func (f *File) IsFailed() error {
	err := f.allocate()
	if err != nil {
		return err
	}

	val := atomic.LoadInt32(&f.failedBlock)
	if val < 0 {
		return nil
	}

	return f.blocks[val].err
}

func (f *File) GetBlock(blockID int64) (*Block, error) {
	if blockID >= int64(len(f.blocks)) {
		return nil, errors.New("mediacache: blockID out of bounds")
	}

	block := f.blocks[blockID]
	block.once.Do(func() {
		f.fetchBlock(blockID)
	})

	block.mutex.RLock()
	if block.err != nil {
		block.mutex.RUnlock()
		return nil, block.err
	}
	block.mutex.RUnlock()

	return block, nil
}

func (f *File) fetchBlock(blockID int64) {
	block := f.blocks[blockID]
	block.mutex.Lock()
	defer block.mutex.Unlock()

	if block.err != nil {
		return
	}

	var lastError error

attemptLoop:
	for attempt := 0; attempt < 3; attempt++ {
		var rd io.ReadCloser
		rd, lastError = f.fetcher(blockID*f.blockSize+block.written,
			blockID*f.blockSize+int64(len(block.mapped)))
		if lastError != nil {
			time.Sleep(time.Second * 3)
			continue
		}

		offset := block.written
		for offset < int64(len(block.mapped)) {
			var n int
			n, lastError = rd.Read(block.mapped[offset:])
			block.written += int64(n)
			if lastError != nil {
				rd.Close()
				time.Sleep(time.Second * 3)
				continue attemptLoop
			}

			offset += int64(n)
		}

		block.err = nil
		rd.Close()

		return
	}

	if lastError == nil {
		block.err = errors.New("mediacache: ran out of attempts, unknown failure")
	} else {
		block.err = lastError
	}
}
