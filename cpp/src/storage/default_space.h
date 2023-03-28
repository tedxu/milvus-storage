#pragma once
#include <arrow/filesystem/filesystem.h>
#include <arrow/type_fwd.h>

#include <memory>
#include <unordered_map>

#include "arrow/record_batch.h"
#include "manifest.h"
#include "options.h"
#include "space.h"

const std::string kOffsetFieldName = "__offset";

class MergeRecordReader;
class ScanRecordReader;
class FilterQueryRecordReader;
class RecordReader;
class DeleteSet;

class DefaultSpace : public Space {
  public:
  void
  Write(arrow::RecordBatchReader* reader, WriteOption* option) override;

  std::unique_ptr<arrow::RecordBatchReader>
  Read(std::shared_ptr<ReadOption> option) override;

  void
  Delete(arrow::RecordBatchReader* reader) override;

  static std::unique_ptr<DefaultSpace>
  Open(std::shared_ptr<arrow::fs::FileSystem> fs, std::string path);

  static std::unique_ptr<DefaultSpace>
  Open();
  static std::unique_ptr<DefaultSpace>
  Create();

  private:
  // bool CheckSchemaAndOption(std::shared_ptr<arrow::Schema> schema, std::shared_ptr<SpaceOption> &options);

  // std::shared_ptr<arrow::Schema> &CreateScalarSchema();

  // std::shared_ptr<arrow::Schema> &CreateVectorSchema();

  // std::shared_ptr<arrow::Schema> &CreateDeleteSchema();

  private:
  DefaultSpace(std::shared_ptr<arrow::Schema> schema, std::shared_ptr<SpaceOption>& options);

  std::shared_ptr<ReadOption>
  ValidateReadOption(std::shared_ptr<ReadOption> option);

  std::unique_ptr<Manifest> manifest_;
  std::unique_ptr<arrow::fs::FileSystem> fs_;
  std::shared_ptr<DeleteSet> delete_set_;

  friend MergeRecordReader;
  friend ScanRecordReader;
  friend FilterQueryRecordReader;
  friend RecordReader;
  friend DeleteSet;
};
