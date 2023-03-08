package storage

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/google/uuid"
	"github.com/milvus-io/milvus-storage-format/internal/format"
	"github.com/milvus-io/milvus-storage-format/internal/format/parquet"
	"github.com/milvus-io/milvus-storage-format/internal/fs"
	"github.com/milvus-io/milvus-storage-format/internal/manifest"
	"github.com/milvus-io/milvus-storage-format/options"
)

var (
	ErrSchemaNotMatch error = errors.New("schema not match")
	ErrColumnNotExist error = errors.New("column not exist")
)

type Space interface {
	Write(reader array.RecordReader, options *options.WriteOptions) error
	Read(options *options.ReadOptions) (array.RecordReader, error)
}

type DefaultSpace struct {
	schema   *arrow.Schema
	fs       fs.Fs
	options  *options.SpaceOptions
	manifest *manifest.ManifestV1
}

func (s *DefaultSpace) Write(reader array.RecordReader, options *options.WriteOptions) error {
	// check schema consistency
	if !s.schema.Equal(reader.Schema()) {
		return ErrSchemaNotMatch
	}

	var dataFiles []*manifest.DataFile
	var writer format.Writer
	var err error
	// write data
	for reader.Next() {
		rec := reader.Record()

		if rec.NumRows() == 0 {
			continue
		}

		if writer == nil {
			filePath := uuid.NewString() + ".parquet"
			writer, err = parquet.NewFileWriter(s.schema, s.fs, filePath)
			if err != nil {
				return err
			}
			dataFiles = append(dataFiles, manifest.NewDataFile(filePath))
		}

		if err := writer.Write(rec); err != nil {
			return err
		}

		if writer.Count() >= options.MaxRowsPerFile {
			if err := writer.Close(); err != nil {
				return err
			}
			writer = nil
		}
	}

	if writer != nil {
		if err := writer.Close(); err != nil {
			return err
		}
	}

	// update manifest
	if len(dataFiles) != 0 {
		s.manifest.AddDataFiles(dataFiles...)
		if err := manifest.WriteManifestFile(s.fs, s.manifest); err != nil {
			return err
		}
	}

	return nil
}

// Read return a RecordReader. Remember to call Release after using the RecordReader
func (s *DefaultSpace) Read(options *options.ReadOptions) (array.RecordReader, error) {
	// check read options
	for _, col := range options.Columns {
		if !s.schema.HasField(col) {
			return nil, fmt.Errorf("%w: %s", ErrColumnNotExist, col)
		}
	}

	return NewDefaultRecordReader(s, options), nil
}

func NewDefaultSpace(schema *arrow.Schema, options *options.SpaceOptions) *DefaultSpace {
	fsFactory := fs.NewFsFactory()
	fs := fsFactory.Create(options.Fs)
	return &DefaultSpace{
		schema:   schema,
		fs:       fs,
		options:  options,
		manifest: manifest.NewManifest(),
	}
}
