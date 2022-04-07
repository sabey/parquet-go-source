package http

import (
	"mime/multipart"

	"github.com/pkg/errors"
	"github.com/sabey/parquet-go/source"
)

type MultipartFileWrapper struct {
	FH *multipart.FileHeader
	F  multipart.File
}

func NewMultipartFileWrapper(fh *multipart.FileHeader, f multipart.File) source.ParquetFile {
	return &MultipartFileWrapper{FH: fh, F: f}
}

func (mfw *MultipartFileWrapper) Create(_ string) (source.ParquetFile, error) {
	return nil, errors.New("cannot create a new multipart file")
}

// this method is called multiple times on one file to open parallel readers
func (mfw *MultipartFileWrapper) Open(_ string) (source.ParquetFile, error) {
	file, err := mfw.FH.Open()
	if err != nil {
		return nil, errors.Wrap(err, "mfw.FH.Open")
	}
	return NewMultipartFileWrapper(mfw.FH, file), nil
}

func (mfw *MultipartFileWrapper) Seek(offset int64, pos int) (int64, error) {
	n, err := mfw.F.Seek(offset, pos)
	if err != nil {
		return n, errors.Wrap(err, "mfw.F.Seek")
	}
	return n, nil
}

func (mfw *MultipartFileWrapper) Read(p []byte) (int, error) {
	n, err := mfw.F.Read(p)
	if err != nil {
		return n, errors.Wrap(err, "mfw.F.Read")
	}
	return n, nil
}

func (mfw *MultipartFileWrapper) Write(_ []byte) (int, error) {
	return 0, errors.New("cannot write to request file")
}

func (mfw *MultipartFileWrapper) Close() error {
	if err := mfw.F.Close(); err != nil {
		return errors.Wrap(err, "mfw.F.Close")
	}
	return nil
}
