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

#pragma once

#include <arrow/filesystem/s3fs.h>
#include <arrow/util/uri.h>
#include <cstdlib>
#include "common/log.h"
#include "common/macro.h"
#include "filesystem/fs.h"

namespace milvus_storage {

class S3FileSystemProducer : public FileSystemProducer {
  public:
  S3FileSystemProducer() {};

  Result<std::shared_ptr<arrow::fs::FileSystem>> Make(const std::string& uri, std::string* out_path) override {
    arrow::util::Uri uri_parser;
    RETURN_ARROW_NOT_OK(uri_parser.Parse(uri));

    if (!arrow::fs::IsS3Initialized()) {
      arrow::fs::S3GlobalOptions global_options;
      RETURN_ARROW_NOT_OK(arrow::fs::InitializeS3(global_options));
      std::atexit([]() {
        auto status = arrow::fs::EnsureS3Finalized();
        if (!status.ok()) {
          LOG_STORAGE_WARNING_ << "Failed to finalize S3: " << status.message();
        }
      });
    }

    arrow::fs::S3Options options;
    options.endpoint_override = uri_parser.ToString();
    options.ConfigureAccessKey(std::getenv("ACCESS_KEY"), std::getenv("SECRET_KEY"));
    *out_path = std::getenv("FILE_PATH");

    if (std::getenv("REGION") != nullptr) {
      options.region = std::getenv("REGION");
    }

    ASSIGN_OR_RETURN_ARROW_NOT_OK(auto fs, arrow::fs::S3FileSystem::Make(options));
    return std::shared_ptr<arrow::fs::FileSystem>(fs);
  }
};

}  // namespace milvus_storage