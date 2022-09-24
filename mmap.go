package tsdb

import (
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
)

// 实现通过mmap的方式读取文件

type MMapFile struct {
	file *os.File
	data []byte
}

func OpenMMapFile(path string) (mmapFile *MMapFile, err error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file error, path: %s", path)
	}
	defer func() {
		if err != nil {
			file.Close()
		}
	}()

	var size int
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat error, path: %s", path)
	}
	size = int(info.Size())
	data, err := syscallMmap(file, size)
	if err != nil {
		return nil, errors.New("mmap error")
	}
	return &MMapFile{
		file: file,
		data: data,
	}, nil
}

// syscallMmap unix创建内存映射
func syscallMmap(file *os.File, length int) ([]byte, error) {
	return unix.Mmap(int(file.Fd()), 0, length, unix.PROT_READ, unix.MAP_SHARED)
}

// syscallMunmap unix取消内存映射
func syscallMunmap(data []byte) (err error) {
	return unix.Munmap(data)
}

func (mmapFile *MMapFile) Close() error {
	err := syscallMunmap(mmapFile.data)
	err2 := mmapFile.file.Close()
	if err != nil {
		return err
	}
	return err2
}

func (mmapFile *MMapFile) File() *os.File {
	return mmapFile.file
}

func (mmapFile *MMapFile) Bytes() []byte {
	return mmapFile.data
}
