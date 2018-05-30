package tarpluck

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type TarPluck struct {
	tar_filename    string
	filesIndex      map[string]int64
	lastValidRecord int64
	lock            sync.Mutex
	fpReader        *os.File
	fpWriter        *os.File
	tarWriter       *tar.Writer
}

func New(filename string) (*TarPluck, error) {
	var fp *os.File
	var ferr error
	var tpErr *TarPluck = &TarPluck{}

	if _, err := os.Stat(filename); os.IsNotExist(err) {
		fp, ferr = os.Create(filename)
	} else {
		fp, ferr = os.Open(filename)
	}

	if ferr != nil {
		return tpErr, ferr
	}

	var serr error
	var currentIndex int64 = 0
	var fileMap map[string]int64 = make(map[string]int64)
	var lastValidRecord int64 = 0

	tr := tar.NewReader(fp)

	for {
		if serr != nil {
			return tpErr, serr
		}

		currentIndex, serr = fp.Seek(0, os.SEEK_CUR)
		hdr, err := tr.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			return tpErr, err
		}

		if _, found := fileMap[hdr.Name]; found == true {
			fmt.Printf("squashing previous version of %s\n", hdr.Name)
		}

		fileMap[hdr.Name] = currentIndex
		fmt.Println("loading", hdr.Name, hdr.Size, currentIndex)
		lastValidRecord = currentIndex + hdr.Size
	}

	return &TarPluck{
		tar_filename:    filename,
		filesIndex:      fileMap,
		lastValidRecord: lastValidRecord,
		fpReader:        fp,
		fpWriter:        nil,
	}, nil
}

func (tp *TarPluck) Exists(h string) bool {
	tp.lock.Lock()
	defer tp.lock.Unlock()

	_, found := tp.filesIndex[h]
	return found
}

func (tp *TarPluck) Keys() []string {
	tp.lock.Lock()
	defer tp.lock.Unlock()

	var keys []string = []string{}
	for k, _ := range tp.filesIndex {
		keys = append(keys, k)
	}
	return keys
}

func (tp *TarPluck) Read(s string) ([]byte, error) {
	tp.lock.Lock()
	defer tp.lock.Unlock()

	if index, found := tp.filesIndex[s]; found == true {
		var err error
		var serr error
		_, serr = tp.fpReader.Seek(0, os.SEEK_CUR)
		if serr != nil {

		}
		tr := tar.NewReader(tp.fpReader)

		var header *tar.Header
		var currentIndex int64 = 0

		for {
			header, err = tr.Next()
			if err == io.EOF {
				return []byte{}, nil
			}

			if err != nil {
				return []byte{}, err
			}

			if currentIndex == index {
				break
			}

			currentIndex, serr = tp.fpReader.Seek(0, os.SEEK_CUR)
			if serr != nil {
				return []byte{}, serr
			}
		}

		chunk := make([]byte, header.Size)
		buf := bytes.NewBuffer(chunk)
		io.Copy(buf, tr)

		return buf.Bytes(), err
	} else {
		return []byte{}, errors.New("Not in this tar file.")
	}
}

func (tp *TarPluck) Close() error {
	tp.lock.Lock()
	defer tp.lock.Unlock()

	tp.fpReader.Close()
	if tp.tarWriter != nil {
		tp.tarWriter.Close()
	}
	if tp.fpWriter != nil {
		tp.fpWriter.Close()
	}

	return nil
}

func (tp *TarPluck) Write(s string, b []byte) error {
	tp.lock.Lock()
	defer tp.lock.Unlock()

	var currentIndex int64 = 0
	var serr error

	if tp.fpWriter == nil {
		writer, err := os.OpenFile(tp.tar_filename, os.O_RDWR, os.ModePerm)
		if err != nil {
			return err
		}
		if currentIndex, serr = writer.Seek(-2<<9, os.SEEK_END); serr != nil {
			if len(tp.filesIndex) >= 0 {
				return serr
			}
		}
		tp.fpWriter = writer
		tp.tarWriter = tar.NewWriter(writer)
	}

	hdr := &tar.Header{
		Name: s,
		Mode: 0600,
		Size: int64(len(b)),
	}

	if err := tp.tarWriter.WriteHeader(hdr); err != nil {
		return err
	}

	if _, err := tp.tarWriter.Write(b); err != nil {
		return err
	}

	tp.filesIndex[s] = currentIndex
	return nil
}
