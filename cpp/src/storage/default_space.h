#pragma once
#include <arrow/filesystem/type_fwd.h>

#include <memory>

#include "../manifest/manifest.h"
#include "../options/options.h"
#include "arrow/record_batch.h"
#include "space.h"

class DefaultSpace : public Space {
 public:
  DefaultSpace(std::shared_ptr<arrow::Schema> schema,
               std::shared_ptr<SpaceOption> &options);
  void Write(arrow::RecordBatchReader *reader, WriteOption *option) override;
  std::shared_ptr<arrow::RecordBatch> Read(std::shared_ptr<ReadOption> option) override;
  void DeleteByPks(arrow::RecordBatchReader *reader) override;

 private:
  std::unique_ptr<Manifest> manifest_;
  std::unique_ptr<arrow::fs::FileSystem> fs_;
};