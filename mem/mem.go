package mem

import (
	"io"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/sabey/parquet-go/source"
	"github.com/spf13/afero"
)

// desclare unexported in-memory file-system
var memFs afero.Fs

// SetInMemFileFs - overrides local in-memory fileSystem
// NOTE: this is set by NewMemFileWriter is created
// and memFs is still nil
func SetInMemFileFs(fs *afero.Fs) {
	memFs = *fs
}

// GetMemFileFs - returns the current memory file-system
// being used by ParquetFile
func GetMemFileFs() afero.Fs {
	return memFs
}

// OnCloseFunc function type, handles what to do
// after converted file is closed in-memory.
// Close() will pass the filename string and data as io.reader
type OnCloseFunc func(string, io.Reader) error

// MemFile - ParquetFile type for in-memory file operations
type MemFile struct {
	FilePath string
	File     afero.File
	OnClose  OnCloseFunc
}

// NewMemFileWriter - intiates and creates an instance of MemFiles
// NOTE: there is no NewMemFileReader as this particular type was written
// to handle in-memory conversions and offloading. The results of
// conversion can then be stored and read via HDFS, LocalFS, etc without
// the need for loading the file back into memory directly
func NewMemFileWriter(name string, f OnCloseFunc) (source.ParquetFile, error) {
	if memFs == nil {
		memFs = afero.NewMemMapFs()
	}

	var m MemFile
	m.OnClose = f
	pf, err := m.Create(name)
	if err != nil {
		return pf, errors.Wrap(err, "m.Create")
	}
	return pf, nil
}

// Create - create in-memory file
func (fs *MemFile) Create(name string) (source.ParquetFile, error) {
	file, err := memFs.Create(name)
	if err != nil {
		return fs, errors.Wrap(err, "memFs.Create")
	}

	fs.File = file
	fs.FilePath = name
	return fs, nil
}

// Open - open file in-memory
func (fs *MemFile) Open(name string) (source.ParquetFile, error) {
	var (
		err error
	)
	if name == "" {
		name = fs.FilePath
	}

	fs.FilePath = name
	fs.File, err = memFs.Open(name)
	if err != nil {
		return fs, errors.Wrap(err, "memFs.Open")
	}
	return fs, nil
}

// Seek - seek function
func (fs *MemFile) Seek(offset int64, pos int) (int64, error) {
	n, err := fs.File.Seek(offset, pos)
	if err != nil {
		return n, errors.Wrap(err, "fs.File.Seek")
	}
	return n, nil
}

// Read - read file
func (fs *MemFile) Read(b []byte) (cnt int, err error) {
	var n int
	ln := len(b)
	for cnt < ln {
		n, err = fs.File.Read(b[cnt:])
		cnt += n
		if err != nil {
			break
		}
	}
	if err != nil {
		return cnt, errors.Wrap(err, "fs.File.Read")
	}
	return cnt, nil
}

// Write - write file in-memory
func (fs *MemFile) Write(b []byte) (n int, err error) {
	n, err = fs.File.Write(b)
	if err != nil {
		return n, errors.Wrap(err, "fs.File.Write")
	}
	return n, nil
}

// Close - close file and execute OnCloseFunc
func (fs *MemFile) Close() error {
	if err := fs.File.Close(); err != nil {
		return errors.Wrap(err, "fs.File.Close")
	}
	if fs.OnClose != nil {
		f, _ := fs.Open(fs.FilePath)
		if err := fs.OnClose(filepath.Base(fs.FilePath), f); err != nil {
			return errors.Wrap(err, "fs.OnClose")
		}
	}
	return nil
}
