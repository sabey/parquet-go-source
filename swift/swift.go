package swiftsource

import (
	"github.com/ncw/swift"
	"github.com/pkg/errors"
	"github.com/sabey/parquet-go/source"
)

type SwiftFile struct {
	Connection *swift.Connection

	Container string
	FilePath  string

	FileReader *swift.ObjectOpenFile
	FileWriter *swift.ObjectCreateFile
}

func newSwiftFile(containerName string, filePath string, conn *swift.Connection) *SwiftFile {
	return &SwiftFile{
		Connection: conn,
		Container:  containerName,
		FilePath:   filePath,
	}
}

func NewSwiftFileReader(container string, filePath string, conn *swift.Connection) (source.ParquetFile, error) {
	res := newSwiftFile(container, filePath, conn)
	pf, err := res.Open(filePath)
	if err != nil {
		return pf, errors.Wrap(err, "res.Open")
	}
	return pf, nil
}

func NewSwiftFileWriter(container string, filePath string, conn *swift.Connection) (source.ParquetFile, error) {
	res := newSwiftFile(container, filePath, conn)
	pf, err := res.Create(filePath)
	if err != nil {
		return pf, errors.Wrap(err, "res.Create")
	}
	return pf, nil
}

func (file *SwiftFile) Open(name string) (source.ParquetFile, error) {
	if name == "" {
		name = file.FilePath
	}

	fr, _, err := file.Connection.ObjectOpen(file.Container, name, false, nil)
	if err != nil {
		return nil, errors.Wrap(err, "file.Connection.ObjectOpen")
	}

	res := &SwiftFile{
		Connection: file.Connection,
		Container:  file.Container,
		FilePath:   name,
		FileReader: fr,
	}

	return res, nil
}

func (file *SwiftFile) Create(name string) (source.ParquetFile, error) {
	if name == "" {
		name = file.FilePath
	}

	fw, err := file.Connection.ObjectCreate(file.Container, name, false, "", "", nil)
	if err != nil {
		return nil, errors.Wrap(err, "file.Connection.ObjectCreate")
	}

	res := &SwiftFile{
		Connection: file.Connection,
		Container:  file.Container,
		FilePath:   name,
		FileWriter: fw,
	}

	return res, nil
}

func (file *SwiftFile) Read(b []byte) (n int, err error) {
	n, err = file.FileReader.Read(b)
	if err != nil {
		return n, errors.Wrap(err, "file.FileReader.Read")
	}
	return n, nil
}

func (file *SwiftFile) Seek(offset int64, whence int) (int64, error) {
	n, err := file.FileReader.Seek(offset, whence)
	if err != nil {
		return n, errors.Wrap(err, "file.FileReader.Seek")
	}
	return n, nil
}

func (file *SwiftFile) Write(p []byte) (n int, err error) {
	n, err = file.FileWriter.Write(p)
	if err != nil {
		return n, errors.Wrap(err, "file.FileReader.Write")
	}
	return n, nil
}

func (file *SwiftFile) Close() error {
	if file.FileWriter != nil {
		if err := file.FileWriter.Close(); err != nil {
			return errors.Wrap(err, "file.FileWriter.Close")
		}
	}
	if file.FileReader != nil {
		if err := file.FileReader.Close(); err != nil {
			return errors.Wrap(err, "file.FileReader.Close")
		}
	}
	return nil
}
