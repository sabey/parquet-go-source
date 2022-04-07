package s3v2

//go:generate mockgen -destination=./mocks/mock_s3.go -package=mocks github.com/sabey/parquet-go-source/s3v2 S3API

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"github.com/sabey/parquet-go/source"
)

type S3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// S3File is ParquetFile for AWS S3
type S3File struct {
	ctx    context.Context
	client S3API
	offset int64
	whence int

	// write-related fields
	writeOpened     bool
	writeDone       chan error
	pipeReader      *io.PipeReader
	pipeWriter      *io.PipeWriter
	uploader        *manager.Uploader
	uploaderOptions []func(*manager.Uploader)

	// read-related fields
	readOpened bool
	fileSize   int64
	downloader *manager.Downloader

	lock       sync.RWMutex
	err        error
	BucketName string
	Key        string
}

const (
	rangeHeader       = "bytes=%d-%d"
	rangeHeaderSuffix = "bytes=%d"
)

var (
	errWhence        = errors.New("Seek: invalid whence")
	errInvalidOffset = errors.New("Seek: invalid offset")
	errFailedUpload  = errors.New("Write: failed upload")
)

// NewS3FileWriter creates an S3 FileWriter, to be used with NewParquetWriter
func NewS3FileWriter(
	ctx context.Context,
	bucket string,
	key string,
	uploaderOptions []func(*manager.Uploader),
	cfgs ...*aws.Config,
) (source.ParquetFile, error) {
	pf, err := NewS3FileWriterWithClient(
		ctx,
		s3.NewFromConfig(getConfig()),
		bucket,
		key,
		uploaderOptions,
	)
	if err != nil {
		return pf, errors.Wrap(err, "NewS3FileWriterWithClient")
	}
	return pf, nil
}

// NewS3FileWriterWithClient is the same as NewS3FileWriter but allows passing
// your own S3 client.
func NewS3FileWriterWithClient(
	ctx context.Context,
	s3Client S3API,
	bucket string,
	key string,
	uploaderOptions []func(*manager.Uploader),
) (source.ParquetFile, error) {
	file := &S3File{
		ctx:             ctx,
		client:          s3Client,
		writeDone:       make(chan error),
		uploaderOptions: uploaderOptions,
		BucketName:      bucket,
		Key:             key,
	}

	pf, err := file.Create(key)
	if err != nil {
		return pf, errors.Wrap(err, "file.Create")
	}
	return pf, nil
}

// NewS3FileReader creates an S3 FileReader, to be used with NewParquetReader
func NewS3FileReader(ctx context.Context, bucket string, key string, cfgs ...*aws.Config) (source.ParquetFile, error) {
	pf, err := NewS3FileReaderWithClient(ctx, s3.NewFromConfig(getConfig()), bucket, key)
	if err != nil {
		return pf, errors.Wrap(err, "NewS3FileReaderWithClient")
	}
	return pf, nil
}

// NewS3FileReaderWithClient is the same as NewS3FileReader but allows passing
// your own S3 client
func NewS3FileReaderWithClient(ctx context.Context, s3Client S3API, bucket string, key string) (source.ParquetFile, error) {
	s3Downloader := manager.NewDownloader(s3Client)

	file := &S3File{
		ctx:        ctx,
		client:     s3Client,
		downloader: s3Downloader,
		BucketName: bucket,
		Key:        key,
	}

	pf, err := file.Open(key)
	if err != nil {
		return pf, errors.Wrap(err, "file.Open")
	}
	return pf, nil
}

// Seek tracks the offset for the next Read. Has no effect on Write.
func (s *S3File) Seek(offset int64, whence int) (int64, error) {
	if whence < io.SeekStart || whence > io.SeekEnd {
		return 0, errors.Wrap(errWhence, "errWhence")
	}

	if s.fileSize > 0 {
		switch whence {
		case io.SeekStart:
			if offset < 0 || offset > s.fileSize {
				return 0, errors.Wrap(errInvalidOffset, "errInvalidOffset")
			}
		case io.SeekCurrent:
			offset += s.offset
			if offset < 0 || offset > s.fileSize {
				return 0, errors.Wrap(errInvalidOffset, "errInvalidOffset")
			}
		case io.SeekEnd:
			if offset > -1 || -offset > s.fileSize {
				return 0, errors.Wrap(errInvalidOffset, "errInvalidOffset")
			}
		}
	}

	s.offset = offset
	s.whence = whence
	return s.offset, nil
}

// Read up to len(p) bytes into p and return the number of bytes read
func (s *S3File) Read(p []byte) (n int, err error) {
	if s.fileSize > 0 && s.offset >= s.fileSize {
		return 0, errors.Wrap(io.EOF, "io.EOF")
	}

	numBytes := len(p)
	getObjRange := s.getBytesRange(numBytes)
	getObj := &s3.GetObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(s.Key),
	}
	if len(getObjRange) > 0 {
		getObj.Range = aws.String(getObjRange)
	}

	wab := manager.NewWriteAtBuffer(p)
	bytesDownloaded, err := s.downloader.Download(s.ctx, wab, getObj)
	if err != nil {
		return 0, errors.Wrap(err, "s.downloader.Download")
	}

	s.offset += bytesDownloaded
	if buf := wab.Bytes(); len(buf) > numBytes {
		// backing buffer reassigned, copy over some of the data
		copy(p, buf)
		bytesDownloaded = int64(len(p))
	}

	return int(bytesDownloaded), nil
}

// Write len(p) bytes from p to the S3 data stream
func (s *S3File) Write(p []byte) (n int, err error) {
	s.lock.RLock()
	writeOpened := s.writeOpened
	s.lock.RUnlock()
	if !writeOpened {
		s.openWrite()
	}

	s.lock.RLock()
	writeError := s.err
	s.lock.RUnlock()
	if writeError != nil {
		return 0, errors.Wrap(writeError, "writeError")
	}

	// prevent further writes upon error
	bytesWritten, writeError := s.pipeWriter.Write(p)
	if writeError != nil {
		writeError = errors.Wrap(writeError, "s.pipeWriter.Write")
		s.lock.Lock()
		s.err = writeError
		s.lock.Unlock()

		s.pipeWriter.CloseWithError(err)
		return 0, writeError
	}

	return bytesWritten, nil
}

// Close signals write completion and cleans up any
// open streams. Will block until pending uploads are complete.
func (s *S3File) Close() error {
	var err error

	if s.pipeWriter != nil {
		if err = s.pipeWriter.Close(); err != nil {
			return errors.Wrap(err, "s.pipeWriter.Close")
		}
	}

	// wait for pending uploads
	if s.writeDone != nil {
		err = <-s.writeDone
	}

	if err != nil {
		return errors.Wrap(err, "<-s.writeDone")
	}
	return nil
}

// Open creates a new S3 File instance to perform concurrent reads
func (s *S3File) Open(name string) (source.ParquetFile, error) {
	s.lock.RLock()
	readOpened := s.readOpened
	s.lock.RUnlock()
	if !readOpened {
		if err := s.openRead(); err != nil {
			return nil, errors.Wrap(err, "s.openRead")
		}
	}

	// ColumBuffer passes in an empty string for name
	if len(name) == 0 {
		name = s.Key
	}

	// create a new instance
	pf := &S3File{
		ctx:        s.ctx,
		client:     s.client,
		downloader: s.downloader,
		BucketName: s.BucketName,
		Key:        name,
		readOpened: s.readOpened,
		fileSize:   s.fileSize,
		offset:     0,
	}
	return pf, nil
}

// Create creates a new S3 File instance to perform writes
func (s *S3File) Create(key string) (source.ParquetFile, error) {
	pf := &S3File{
		ctx:             s.ctx,
		client:          s.client,
		uploaderOptions: s.uploaderOptions,
		BucketName:      s.BucketName,
		Key:             key,
		writeDone:       make(chan error),
	}
	pf.openWrite()
	return pf, nil
}

// openWrite creates an S3 uploader that consumes the Reader end of an io.Pipe.
// Calling Close signals write completion.
func (s *S3File) openWrite() {
	pr, pw := io.Pipe()
	uploader := manager.NewUploader(s.client, s.uploaderOptions...)
	s.lock.Lock()
	s.pipeReader = pr
	s.pipeWriter = pw
	s.writeOpened = true
	s.uploader = uploader
	s.lock.Unlock()

	uploadParams := &s3.PutObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(s.Key),
		Body:   s.pipeReader,
	}

	go func(uploader *manager.Uploader, params *s3.PutObjectInput, done chan error) {
		defer close(done)

		// upload data and signal done when complete
		_, err := uploader.Upload(s.ctx, params)
		if err != nil {
			err = errors.Wrap(err, "uploader.Upload")
			s.lock.Lock()
			s.err = err
			s.lock.Unlock()

			if s.writeOpened {
				s.pipeWriter.CloseWithError(err)
			}
		}

		done <- err
	}(s.uploader, uploadParams, s.writeDone)
}

// openRead verifies the requested file is accessible and
// tracks the file size
func (s *S3File) openRead() error {
	hoi := &s3.HeadObjectInput{
		Bucket: aws.String(s.BucketName),
		Key:    aws.String(s.Key),
	}

	hoo, err := s.client.HeadObject(s.ctx, hoi)
	if err != nil {
		return errors.Wrap(err, "s.client.HeadObject")
	}

	s.lock.Lock()
	s.readOpened = true
	if hoo.ContentLength != 0 {
		s.fileSize = hoo.ContentLength
	}
	s.lock.Unlock()

	return nil
}

// getBytesRange returns the range request header string
func (s *S3File) getBytesRange(numBytes int) string {
	var (
		byteRange string
		begin     int64
		end       int64
	)

	// Processing for unknown file size relies on the requestor to
	// know which ranges are valid. May occur if caller is missing HEAD permissions.
	if s.fileSize < 1 {
		switch s.whence {
		case io.SeekStart, io.SeekCurrent:
			byteRange = fmt.Sprintf(rangeHeader, s.offset, s.offset+int64(numBytes)-1)
		case io.SeekEnd:
			byteRange = fmt.Sprintf(rangeHeaderSuffix, s.offset)
		}
		return byteRange
	}

	switch s.whence {
	case io.SeekStart, io.SeekCurrent:
		begin = s.offset
	case io.SeekEnd:
		begin = s.fileSize + s.offset
	default:
		return byteRange
	}

	endIndex := s.fileSize - 1
	if begin < 0 {
		begin = 0
	}
	end = begin + int64(numBytes) - 1
	if end > endIndex {
		end = endIndex
	}

	byteRange = fmt.Sprintf(rangeHeader, begin, end)
	return byteRange
}
