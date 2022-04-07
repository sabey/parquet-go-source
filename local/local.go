package local

import (
	"os"

	"github.com/pkg/errors"
	"github.com/sabey/parquet-go/source"
)

type LocalFile struct {
	FilePath string
	File     *os.File
}

func NewLocalFileWriter(name string) (source.ParquetFile, error) {
	pf, err := (&LocalFile{}).Create(name)
	if err != nil {
		return pf, errors.Wrap(err, "(&LocalFile{}).Create")
	}
	return pf, nil
}

func NewLocalFileReader(name string) (source.ParquetFile, error) {
	pf, err := (&LocalFile{}).Open(name)
	if err != nil {
		return pf, errors.Wrap(err, "(&LocalFile{}).Open")
	}
	return pf, nil
}

func (self *LocalFile) Create(name string) (source.ParquetFile, error) {
	file, err := os.Create(name)
	myFile := new(LocalFile)
	myFile.FilePath = name
	myFile.File = file
	if err != nil {
		return myFile, errors.Wrap(err, "os.Create")
	}
	return myFile, nil
}

func (self *LocalFile) Open(name string) (source.ParquetFile, error) {
	var (
		err error
	)
	if name == "" {
		name = self.FilePath
	}

	myFile := new(LocalFile)
	myFile.FilePath = name
	myFile.File, err = os.Open(name)
	if err != nil {
		return myFile, errors.Wrap(err, "os.Open")
	}
	return myFile, nil
}
func (self *LocalFile) Seek(offset int64, pos int) (int64, error) {
	n, err := self.File.Seek(offset, pos)
	if err != nil {
		return n, errors.Wrap(err, "self.File.Seek")
	}
	return n, nil
}

func (self *LocalFile) Read(b []byte) (cnt int, err error) {
	var n int
	ln := len(b)
	for cnt < ln {
		n, err = self.File.Read(b[cnt:])
		cnt += n
		if err != nil {
			break
		}
	}
	if err != nil {
		return cnt, errors.Wrap(err, "self.File.Read")
	}
	return cnt, nil
}

func (self *LocalFile) Write(b []byte) (n int, err error) {
	n, err = self.File.Write(b)
	if err != nil {
		return n, errors.Wrap(err, "self.File.Write")
	}
	return n, nil
}

func (self *LocalFile) Close() error {
	if err := self.File.Close(); err != nil {
		return errors.Wrap(err, "self.File.Close")
	}
	return nil
}
