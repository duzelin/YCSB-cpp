#include "lsm2lix_db.h"

#include "core/core_workload.h"
#include "core/db_factory.h"
#include "core/properties.h"
#include "core/utils.h"

namespace {
  std::string kLSMPath = "/tmp/LSM2LIX/lsm";
  std::string kLIXPath = "/tmp/LSM2LIX/lix";
  std::string kDBPath = "/tmp/LSM2LIX";
} // anonymous

namespace ycsbc {

LSM2LIX::LSM2LIX *LSM2LIXDB::db_ = nullptr;
int LSM2LIXDB::ref_cnt_ = 0;
std::mutex LSM2LIXDB::mu_;

void LSM2LIXDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties &props = *props_;
  method_read_ = &LSM2LIXDB::ReadSingle;
  method_scan_ = &LSM2LIXDB::ScanSingle;
  method_update_ = &LSM2LIXDB::UpdateSingle;
  method_insert_ = &LSM2LIXDB::InsertSingle;
  method_delete_ = &LSM2LIXDB::DeleteSingle;

  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY, CoreWorkload::FIELD_COUNT_DEFAULT));
  ref_cnt_++;
  if (db_) {
    return;
  }

  LSM2LIX::Status s;
  s = LSM2LIX::LSM2LIX::Open(kDBPath, &db_);
  if (!s.ok()) {
    throw utils::Exception(std::string("LSM2LIX Open: ") + s.ToString());
  }
}

void LSM2LIXDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);
  if (--ref_cnt_) {
    return;
  }
  delete db_;
}

void LSM2LIXDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.name.data(), field.name.size());
    len = field.value.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.value.data(), field.value.size());
  }
}

void LSM2LIXDB::DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
                                     const std::vector<std::string> &fields) {
  std::vector<std::string>::const_iterator filter_iter = fields.begin();
  while (p != lim && filter_iter != fields.end()) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    if (*filter_iter == field) {
      values.push_back({field, value});
      filter_iter++;
    }
  }
  assert(values.size() == fields.size());
}

void LSM2LIXDB::DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                     const std::vector<std::string> &fields) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRowFilter(values, p, lim, fields);
}

void LSM2LIXDB::DeserializeRow(std::vector<Field> &values, const char *p, const char *lim) {
  while (p != lim) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    values.push_back({field, value});
  }
}

void LSM2LIXDB::DeserializeRow(std::vector<Field> &values, const std::string &data) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRow(values, p, lim);
}

DB::Status LSM2LIXDB::ReadSingle(const std::string &table, const std::string &key,
                                 const std::vector<std::string> *fields,
                                 std::vector<Field> &result) {
  std::string data;
  LSM2LIX::Status s = db_->Get(key, &data);
  if (s.IsNotFound()) {
    return kNotFound;
  } else if (!s.ok()) {
    throw utils::Exception(std::string("LSMLIXDB Get: ") + s.ToString());
  }
  if (fields != nullptr) {
    DeserializeRowFilter(result, data, *fields);
  } else {
    DeserializeRow(result, data);
    assert(result.size() == static_cast<size_t>(fieldcount_));
  }
  return kOK;
}

DB::Status LSM2LIXDB::ScanSingle(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result)
{
  return kOK;
}

DB::Status LSM2LIXDB::UpdateSingle(const std::string &table, const std::string &key, std::vector<Field> &values)
{
  return kOK;
}

DB::Status LSM2LIXDB::InsertSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
  std::string data;
  SerializeRow(values, data);
  LSM2LIX::Status s = db_->Put(key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("LSM2LIX Put: ") + s.ToString());
  }
  return kOK;
}

DB::Status LSM2LIXDB::DeleteSingle(const std::string &table, const std::string &key)
{
  return kOK;
}

DB *NewLSM2LIXDB() {
  return new LSM2LIXDB;
}

const bool registered = DBFactory::RegisterDB("lsm2lixdb", NewLSM2LIXDB);

} // ycsbc
