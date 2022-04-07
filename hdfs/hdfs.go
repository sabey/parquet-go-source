package hdfs

import (
	"github.com/colinmarc/hdfs/v2"
	"github.com/pkg/errors"
	"github.com/sabey/parquet-go/source"
)

type HdfsFile struct {
	Hosts []string
	User  string

	Client     *hdfs.Client
	FilePath   string
	FileReader *hdfs.FileReader
	FileWriter *hdfs.FileWriter
}

func NewHdfsFileWriter(hosts []string, user string, name string) (source.ParquetFile, error) {
	res := &HdfsFile{
		Hosts:    hosts,
		User:     user,
		FilePath: name,
	}
	pf, err := res.Create(name)
	if err != nil {
		return pf, errors.Wrap(err, "res.Create")
	}
	return pf, nil
}

func NewHdfsFileReader(hosts []string, user string, name string) (source.ParquetFile, error) {
	res := &HdfsFile{
		Hosts:    hosts,
		User:     user,
		FilePath: name,
	}
	pf, err := res.Open(name)
	if err != nil {
		return pf, errors.Wrap(err, "res.Open")
	}
	return pf, nil
}

func (self *HdfsFile) Create(name string) (source.ParquetFile, error) {
	var err error
	hf := new(HdfsFile)
	hf.Hosts = self.Hosts
	hf.User = self.User
	hf.Client, err = hdfs.NewClient(hdfs.ClientOptions{
		Addresses: hf.Hosts,
		User:      hf.User,
	})
	hf.FilePath = name
	if err != nil {
		return hf, errors.Wrap(err, "hdfs.NewClient")
	}
	hf.FileWriter, err = hf.Client.Create(name)
	if err != nil {
		return hf, errors.Wrap(err, "hf.Client.Create")
	}
	return hf, nil

}
func (self *HdfsFile) Open(name string) (source.ParquetFile, error) {
	var (
		err error
	)
	if name == "" {
		name = self.FilePath
	}

	hf := new(HdfsFile)
	hf.Hosts = self.Hosts
	hf.User = self.User
	hf.Client, err = hdfs.NewClient(hdfs.ClientOptions{
		Addresses: hf.Hosts,
		User:      hf.User,
	})
	hf.FilePath = name
	if err != nil {
		return hf, errors.Wrap(err, "hdfs.NewClient")
	}
	hf.FileReader, err = hf.Client.Open(name)
	if err != nil {
		return hf, errors.Wrap(err, "hf.Client.Open")
	}
	return hf, nil
}
func (self *HdfsFile) Seek(offset int64, pos int) (int64, error) {
	n, err := self.FileReader.Seek(offset, pos)
	if err != nil {
		return n, errors.Wrap(err, "self.FileReader.Seek")
	}
	return n, nil
}

func (self *HdfsFile) Read(b []byte) (cnt int, err error) {
	var n int
	ln := len(b)
	for cnt < ln {
		n, err = self.FileReader.Read(b[cnt:])
		cnt += n
		if err != nil {
			break
		}
	}
	if err != nil {
		return cnt, errors.Wrap(err, "self.FileReader.Read")
	}
	return cnt, nil
}

func (self *HdfsFile) Write(b []byte) (n int, err error) {
	n, err = self.FileWriter.Write(b)
	if err != nil {
		return n, errors.Wrap(err, "self.FileWriter.Write")
	}
	return n, nil
}

func (self *HdfsFile) Close() error {
	if self.FileReader != nil {
		self.FileReader.Close()
	}
	if self.FileWriter != nil {
		self.FileWriter.Close()
	}
	if self.Client != nil {
		self.Client.Close()
	}
	return nil
}
