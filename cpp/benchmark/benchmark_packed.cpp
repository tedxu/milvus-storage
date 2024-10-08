// Copyright 2024 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <benchmark/benchmark.h>
#include "common/macro.h"

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/api.h>
#include <packed/writer.h>
#include <parquet/properties.h>
#include <packed/reader.h>
#include <iostream>
#include <memory>
#include <ratio>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/util/key_value_metadata.h>
#include "filesystem/fs.h"

namespace milvus_storage {

// Environment variables to configure the S3 test environment
static const char* kEnvAccessKey = "ACCESS_KEY";
static const char* kEnvSecretKey = "SECRET_KEY";
static const char* kEnvS3EndpointUrl = "S3_ENDPOINT_URL";
static const char* kEnvFilePath = "FILE_PATH";

class S3Fixture : public benchmark::Fixture {
  protected:
  void SetUp(::benchmark::State& state) override {
    const char* access_key = std::getenv(kEnvAccessKey);
    const char* secret_key = std::getenv(kEnvSecretKey);
    const char* endpoint_url = std::getenv(kEnvS3EndpointUrl);
    const char* file_path = std::getenv(kEnvFilePath);
    std::string uri = "file:///tmp";
    if (access_key != nullptr && secret_key != nullptr && endpoint_url != nullptr && file_path != nullptr) {
      uri = endpoint_url;
    }
    auto base = std::string();
    auto factory = std::make_shared<FileSystemFactory>();
    auto result = factory->BuildFileSystem(uri, &base);
    if (!result.ok()) {
      state.SkipWithError("Failed to build file system!");
    }
    fs_ = std::move(result).value();
  }

  std::shared_ptr<arrow::fs::FileSystem> fs_;
};

static void PackedRead(benchmark::State& st, arrow::fs::FileSystem* fs, const std::string& path, size_t buffer_size) {
  std::set<int> needed_columns = {0, 1, 2};
  std::vector<ColumnOffset> column_offsets = {
      ColumnOffset(0, 0),
      ColumnOffset(1, 0),
      ColumnOffset(1, 1),
  };

  auto paths = std::vector<std::string>{path + "/0", path + "/1"};

  // after writing, the column of large_str is in 0th file, and the last int64 columns are in 1st file
  std::vector<std::shared_ptr<arrow::Field>> fields = {
      arrow::field("str", arrow::utf8()),
      arrow::field("int32", arrow::int32()),
      arrow::field("int64", arrow::int64()),
  };
  auto schema = arrow::schema(fields);

  for (auto _ : st) {
    PackedRecordBatchReader pr(*fs, paths, schema, column_offsets, needed_columns, buffer_size);
    auto r = pr.ToTable();
    if (!r.ok()) {
      st.SkipWithError(r.status().ToString());
    }
    auto table = r.ValueOrDie();
    r = pr.Close();
    if (!r.ok()) {
      st.SkipWithError(r.status().ToString());
    }
  }
}

static void PackedWrite(benchmark::State& st, arrow::fs::FileSystem* fs, const std::string& path, size_t buffer_size) {
  auto schema = arrow::schema({arrow::field("int32", arrow::int32()), arrow::field("int64", arrow::int64()),
                               arrow::field("str", arrow::utf8())});
  arrow::Int32Builder int_builder;
  arrow::Int64Builder int64_builder;
  arrow::StringBuilder str_builder;

  int_builder.AppendValues({1, 2, 3});
  int64_builder.AppendValues({4, 5, 6});
  str_builder.AppendValues({"foo", "bar", "baz"});

  std::shared_ptr<arrow::Array> int_array;
  std::shared_ptr<arrow::Array> int64_array;
  std::shared_ptr<arrow::Array> str_array;

  int_builder.Finish(&int_array);
  int64_builder.Finish(&int64_array);
  str_builder.Finish(&str_array);

  std::vector<std::shared_ptr<arrow::Array>> arrays = {int_array, int64_array, str_array};
  auto record_batch = arrow::RecordBatch::Make(schema, 3, arrays);

  for (auto _ : st) {
    PackedRecordBatchWriter writer(buffer_size, schema, *fs, path, *parquet::default_writer_properties());
    for (int i = 0; i < 500000; ++i) {
      auto r = writer.Write(record_batch);
      if (!r.ok()) {
        st.SkipWithError(r.ToString());
        std::cerr << "exit on iteration " << i << std::endl;
        break;
      }
    }
    auto r = writer.Close();
    if (!r.ok()) {
      st.SkipWithError(r.ToString());
    }
  }
}

BENCHMARK_DEFINE_F(S3Fixture, Write32MB)(benchmark::State& st) {
  PackedWrite(st, fs_.get(), "/tmp/bench/foo", 12 * 1024 * 1024);
}
BENCHMARK_REGISTER_F(S3Fixture, Write32MB)->UseRealTime();

BENCHMARK_DEFINE_F(S3Fixture, Read32MB)(benchmark::State& st) {
  PackedRead(st, fs_.get(), "/tmp/bench/foo", 12 * 1024 * 1024);
}
BENCHMARK_REGISTER_F(S3Fixture, Read32MB)->UseRealTime();

}  // namespace milvus_storage