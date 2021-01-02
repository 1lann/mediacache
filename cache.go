package mediacache

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edsrzf/mmap-go"
)

type Config struct {
	CachePath string
	BlockSize int64
}

type Cache struct {
	config     *Config
	files      map[string]*File
	filesMutex *sync.Mutex
}

type Block struct {
	mapped  []byte
	written int64
	once    *sync.Once
	err     error
}

type File struct {
	config      *Config
	name        string
	pathToFile  string
	size        int64
	blocks      []*Block
	fetcher     func(start int64, end int64) (io.ReadCloser, error)
	failedBlock int32 // defaults to -1, set to the block number of a permanently failed block.
	mapError    error
	allocated   *sync.Once
	mapping     *mmap.MMap
	handle      *os.File
	mutex       *sync.RWMutex
}

func (c *Cache) OpenFile(name string, fetcher func(start int64, end int64) (io.ReadCloser, error), size int64) (*File, error) {
	c.filesMutex.Lock()
	file, found := c.files[name]
	if found {
		c.filesMutex.Unlock()
		err := file.allocate()
		if err != nil {
			return nil, err
		}

		return file, nil
	}

	file = &File{
		name:        name,
		config:      c.config,
		size:        size,
		fetcher:     fetcher,
		failedBlock: -1,
		allocated:   new(sync.Once),
		mutex:       new(sync.RWMutex),
		mapError:    nil,
		mapping:     nil,
		handle:      nil,
	}

	c.files[name] = file
	c.filesMutex.Unlock()

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
	f.mapError = fmt.Errorf("mediacache: file closed")
	f.mutex.Unlock()

	if err1 != nil {
		return err1
	} else if err2 != nil {
		return err2
	}

	return nil
}

func (b *Block) Bytes() []byte {
	return b.mapped
}

func (f *File) allocate() error {
	f.allocated.Do(func() {
		// allocate the file
		f.pathToFile = filepath.Join(f.config.CachePath, f.name)
		var err error
		f.handle, err = os.Create(f.pathToFile)
		if err != nil {
			f.mapError = fmt.Errorf("mediacache: failed to create file %q: %w", f.name, err)
			return
		}

		err = f.handle.Truncate(f.size)
		if err != nil {
			f.handle.Close()
			f.mapError = fmt.Errorf("mediacache: failed to allocate file %q: %w", f.name, err)
			os.Remove(f.pathToFile)
			return
		}

		mapping, err := mmap.Map(f.handle, mmap.RDWR, 0)
		if err != nil {
			f.handle.Close()
			f.mapError = fmt.Errorf("mediacache: failed to map file %q: %w", f.name, err)
			os.Remove(f.pathToFile)
			return
		}

		f.mapping = &mapping

		numBlocks := f.size / f.config.BlockSize
		if f.size%f.config.BlockSize != 0 {
			numBlocks++
		}

		f.blocks = make([]*Block, numBlocks)

		for i := range f.blocks {
			upper := (int64(i) + 1) * f.config.BlockSize
			if upper > f.size {
				upper = f.size
			}

			f.blocks[i] = &Block{
				mapped:  mapping[int64(i)*f.config.BlockSize : upper],
				written: 0,
				once:    new(sync.Once),
				err:     nil,
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
		return nil, fmt.Errorf("mediacache: blockID out of bounds")
	}

	block := f.blocks[blockID]
	block.once.Do(func() {
		f.fetchBlock(blockID)
	})

	if block.err != nil {
		return nil, block.err
	}

	return block, nil
}

func (f *File) fetchBlock(blockID int64) {
	block := f.blocks[blockID]
	var lastError error

attemptLoop:
	for attempt := 0; attempt < 3; attempt++ {
		var rd io.ReadCloser
		rd, lastError = f.fetcher(blockID*f.config.BlockSize+block.written,
			blockID*f.config.BlockSize+int64(len(block.mapped)))
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
				time.Sleep(time.Second * 3)
				continue attemptLoop
			}

			offset += int64(n)
		}

		block.err = nil

		return
	}

	if lastError == nil {
		block.err = errors.New("mediacache: ran out of attempts, unknown failure")
	} else {
		block.err = lastError
	}
}

func NewCache(config *Config) *Cache {
	return &Cache{
		config:     config,
		files:      make(map[string]*File),
		filesMutex: new(sync.Mutex),
	}
}