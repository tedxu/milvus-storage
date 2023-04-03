#include "utils.h"
#include <arrow/result.h>
#include <arrow/type_fwd.h>
#include <arrow/util/key_value_metadata.h>
#include <memory>
#include "common/exception.h"
#include "parquet/exception.h"

schema::LogicType
ToProtobufType(arrow::Type::type type) {
  auto type_id = static_cast<int>(type);
  if (type_id < 0 || type_id >= static_cast<int>(schema::LogicType::MAX_ID)) {
    throw StorageException("invalid type");
  }
  return static_cast<schema::LogicType>(type_id);
}

schema::KeyValueMetadata*
ToProtobufMetadata(const arrow::KeyValueMetadata* metadata) {
  auto proto_metadata = new schema::KeyValueMetadata();
  for (const auto& key : metadata->keys()) {
    proto_metadata->add_keys(key);
  }
  for (const auto& value : metadata->values()) {
    proto_metadata->add_values(value);
  }
  return proto_metadata;
}

schema::DataType*
ToProtobufDataType(const arrow::DataType* type);

schema::Field*
ToProtobufField(const arrow::Field* field) {
  auto proto_field = new schema::Field();
  proto_field->set_name(field->name());
  proto_field->set_nullable(field->nullable());
  proto_field->set_allocated_metadata(ToProtobufMetadata(field->metadata().get()));
  proto_field->set_allocated_data_type(ToProtobufDataType(field->type().get()));
  return proto_field;
}

void
SetTypeValues(schema::DataType* proto_type, const arrow::DataType* type) {
  switch (type->id()) {
    case arrow::Type::FIXED_SIZE_BINARY: {
      auto real_type = dynamic_cast<const arrow::FixedSizeBinaryType*>(type);
      auto fixed_size_binary_type = new schema::FixedSizeBinaryType();
      fixed_size_binary_type->set_byte_width(real_type->byte_width());
      proto_type->set_allocated_fixed_size_binary_type(fixed_size_binary_type);
      return;
    }
    case arrow::Type::FIXED_SIZE_LIST: {
      auto real_type = dynamic_cast<const arrow::FixedSizeListType*>(type);
      auto fixed_size_list_type = new schema::FixedSizeListType();
      fixed_size_list_type->set_list_size(real_type->list_size());
      proto_type->set_allocated_fixed_size_list_type(fixed_size_list_type);
      return;
    }
    case arrow::Type::DICTIONARY: {
      auto real_type = dynamic_cast<const arrow::DictionaryType*>(type);
      auto dictionary_type = new schema::DictionaryType();
      dictionary_type->set_allocated_index_type(ToProtobufDataType(real_type->index_type().get()));
      dictionary_type->set_allocated_index_type(ToProtobufDataType(real_type->value_type().get()));
      dictionary_type->set_ordered(real_type->ordered());
      proto_type->set_allocated_dictionary_type(dictionary_type);
      return;
    }
    case arrow::Type::MAP: {
      auto real_type = dynamic_cast<const arrow::MapType*>(type);
      auto map_type = new schema::MapType();
      map_type->set_keys_sorted(real_type->keys_sorted());
      proto_type->set_allocated_map_type(map_type);
      return;
    }
    default:
      return;
  }
}

schema::DataType*
ToProtobufDataType(const arrow::DataType* type) {
  auto proto_type = new schema::DataType();
  SetTypeValues(proto_type, type);
  proto_type->set_logic_type(ToProtobufType(type->id()));
  for (const auto& field : type->fields()) {
    proto_type->mutable_children()->AddAllocated(ToProtobufField(field.get()));
  }

  return proto_type;
}

std::unique_ptr<schema::Schema>
ToProtobufSchema(const arrow::Schema* schema) {
  auto proto_schema = std::make_unique<schema::Schema>();

  for (const auto& field : schema->fields()) {
    proto_schema->mutable_fields()->AddAllocated(ToProtobufField(field.get()));
  }

  proto_schema->set_endianness(schema->endianness() == arrow::Endianness::Little ? schema::Endianness::Little
                                                                                 : schema::Endianness::Big);

  for (const auto& key : schema->metadata()->keys()) {
    proto_schema->mutable_metadata()->add_keys(key);
  }
  for (const auto& value : schema->metadata()->values()) {
    proto_schema->mutable_metadata()->add_values(value);
  }
  return proto_schema;
}

arrow::Type::type
FromProtobufType(schema::LogicType type) {
  auto type_id = static_cast<int>(type);
  if (type_id < 0 || type_id >= static_cast<int>(arrow::Type::MAX_ID)) {
    throw StorageException("invalid type");
  }
  return static_cast<arrow::Type::type>(type_id);
}

std::shared_ptr<arrow::KeyValueMetadata>
FromProtobufKeyValueMetadata(const schema::KeyValueMetadata& metadata) {
  std::vector<std::string> keys(metadata.keys().begin(), metadata.keys().end());
  std::vector<std::string> values(metadata.values().begin(), metadata.values().end());
  return arrow::KeyValueMetadata::Make(keys, values);
}

std::shared_ptr<arrow::DataType>
FromProtobufDataType(const schema::DataType& type);

std::shared_ptr<arrow::Field>
FromProtobufField(const schema::Field& field) {
  auto data_type = FromProtobufDataType(field.data_type());
  auto metadata = FromProtobufKeyValueMetadata(field.metadata());
  return std::make_shared<arrow::Field>(field.name(), data_type, field.nullable(), metadata);
}

std::shared_ptr<arrow::DataType>
FromProtobufDataType(const schema::DataType& type) {
  switch (type.logic_type()) {
    case schema::NA:
      return std::make_shared<arrow::DataType>(arrow::NullType());
    case schema::BOOL:
      return std::make_shared<arrow::DataType>(arrow::BooleanType());
    case schema::UINT8:
      return std::make_shared<arrow::DataType>(arrow::UInt8Type());
    case schema::INT8:
      return std::make_shared<arrow::DataType>(arrow::Int8Type());
    case schema::UINT16:
      return std::make_shared<arrow::DataType>(arrow::UInt16Type());
    case schema::INT16:
      return std::make_shared<arrow::DataType>(arrow::Int16Type());
    case schema::UINT32:
      return std::make_shared<arrow::DataType>(arrow::UInt32Type());
    case schema::INT32:
      return std::make_shared<arrow::DataType>(arrow::Int32Type());
    case schema::UINT64:
      return std::make_shared<arrow::DataType>(arrow::UInt64Type());
    case schema::INT64:
      return std::make_shared<arrow::DataType>(arrow::Int64Type());
    case schema::HALF_FLOAT:
      return std::make_shared<arrow::DataType>(arrow::HalfFloatType());
    case schema::FLOAT:
      return std::make_shared<arrow::DataType>(arrow::FloatType());
    case schema::DOUBLE:
      return std::make_shared<arrow::DataType>(arrow::DoubleType());
    case schema::STRING:
      return std::make_shared<arrow::DataType>(arrow::StringType());
    case schema::BINARY:
      return std::make_shared<arrow::DataType>(arrow::BinaryType());
    case schema::LIST: {
      auto field = FromProtobufField(type.children(0));
      return std::make_shared<arrow::DataType>(arrow::ListType(field));
    }
    case schema::STRUCT: {
      std::vector<std::shared_ptr<arrow::Field>> fields;
      for (const auto& child : type.children()) {
        fields.push_back(FromProtobufField(child));
      }
      return std::make_shared<arrow::DataType>(arrow::StructType(fields));
    }
    case schema::DICTIONARY: {
      auto index_type = FromProtobufDataType(type.dictionary_type().index_type());
      auto value_type = FromProtobufDataType(type.dictionary_type().value_type());
      return std::make_shared<arrow::DataType>(
          arrow::DictionaryType(index_type, value_type, type.dictionary_type().ordered()));
    }
    case schema::MAP: {
      auto value_field = FromProtobufField(type.children(0));
      return std::make_shared<arrow::DataType>(arrow::MapType(value_field, type.map_type().keys_sorted()));
    }
    case schema::FIXED_SIZE_BINARY:
      return std::make_shared<arrow::DataType>(arrow::FixedSizeBinaryType(type.fixed_size_binary_type().byte_width()));

    case schema::FIXED_SIZE_LIST: {
      auto field = FromProtobufField(type.children(0));
      return std::make_shared<arrow::DataType>(
          arrow::FixedSizeListType(field, type.fixed_size_list_type().list_size()));
    }
    default:
      throw StorageException("invalid type");
  }
}

std::shared_ptr<arrow::Schema>
FromProtobufSchema(schema::Schema* schema) {
  arrow::SchemaBuilder schema_builder;
  for (const auto& field : schema->fields()) {
    PARQUET_THROW_NOT_OK(schema_builder.AddField(FromProtobufField(field)));
  }
  PARQUET_ASSIGN_OR_THROW(auto res, schema_builder.Finish());
  return res;
}