/*
 *  treeline_db.h
 *  YCSB-cpp
*/

#ifndef YCSB_C_TREELINE_DB_H
#define YCSB_C_TREELINE_DB_H

#include <string>
#include <mutex>

#include "core/db.h"
#include "core/properties.h"

#include <LSM2LIX.h>

namespace ycsbc {

class LSM2LIXDB : public DB {
 public:
  LSM2LIXDB() {}
  ~LSM2LIXDB() {}

  void Init();

  void Cleanup();

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result) {
    return (this->*(method_read_))(table, key, fields, result);
  }

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
    return (this->*(method_scan_))(table, key, len, fields, result);
  }

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return (this->*(method_update_))(table, key, values);
  }

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return (this->*(method_insert_))(table, key, values);
  }

  Status Delete(const std::string &table, const std::string &key) {
    return (this->*(method_delete_))(table, key);
  }

 private:

  //void GetOptions(const utils::Properties &props, tl::pg::PageGroupedDBOptions *opts);

  static void SerializeRow(const std::vector<Field> &values, std::string &data);
  static void DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
                                   const std::vector<std::string> &fields);
  static void DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                   const std::vector<std::string> &fields);
  static void DeserializeRow(std::vector<Field> &values, const char *p, const char *lim);
  static void DeserializeRow(std::vector<Field> &values, const std::string &data);

  Status ReadSingle(const std::string &table, const std::string &key,
                    const std::vector<std::string> *fields, std::vector<Field> &result);
  Status ScanSingle(const std::string &table, const std::string &key, int len,
                    const std::vector<std::string> *fields,
                    std::vector<std::vector<Field>> &result);
  Status UpdateSingle(const std::string &table, const std::string &key,
                      std::vector<Field> &values);
  Status InsertSingle(const std::string &table, const std::string &key,
                      std::vector<Field> &values);
  Status DeleteSingle(const std::string &table, const std::string &key);

  Status (LSM2LIXDB::*method_read_)(const std::string &, const std:: string &,
                                    const std::vector<std::string> *, std::vector<Field> &);
  Status (LSM2LIXDB::*method_scan_)(const std::string &, const std::string &,
                                    int, const std::vector<std::string> *,
                                    std::vector<std::vector<Field>> &);
  Status (LSM2LIXDB::*method_update_)(const std::string &, const std::string &,
                                      std::vector<Field> &);
  Status (LSM2LIXDB::*method_insert_)(const std::string &, const std::string &,
                                      std::vector<Field> &);
  Status (LSM2LIXDB::*method_delete_)(const std::string &, const std::string &);

  int fieldcount_;

  static LSM2LIX::LSM2LIX *db_;
  static int ref_cnt_;
  static std::mutex mu_;
};

DB *NewTreeLineDB();

} // ycsbc

#endif // YCSB_C_ROCKSDB_DB_H_