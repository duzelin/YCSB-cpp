#include "treeline_db.h"

#include "core/core_workload.h"
#include "core/db_factory.h"
#include "core/properties.h"
#include "core/utils.h"

namespace {
  const std::string PROP_NAME = "treeline.dbname";
  const std::string PROP_NAME_DEFAULT = "/tmp/ycsb-treelinedb";

  const std::string PROP_FORMAT = "treeline.format";
  const std::string PROP_FORMAT_DEFAULT = "single";

  const std::string PROP_DESTROY = "treeline.destroy";
  const std::string PROP_DESTROY_DEFAULT = "false";

  const std::string PROP_CACHE_SIZE = "treeline.cache_size_mib";
  const std::string PROP_CACHE_SIZE_DEFAULT = "64";

  const std::string PROP_MEMTABLE_FLUSH_TD = "treeline.memtable_size_mib";
  const std::string PROP_MEMTABLE_FLUSH_TD_DEFAULT = "64";

  const std::string PROP_DIRECT_IO = "treeline.use_direct_io";
  const std::string PROP_DIRECT_IO_DEFAULT = "1";

  const std::string PROP_BG_THREADS = "treeline.bg_threads";
  const std::string PROP_BG_THREADS_DEFAULT = "2"; 
  
  const std::string PROP_HINTS_RECORD_SIZE = "treeline.record_size_bytes"; // The record size equals to (key.size + value.size).
  const std::string PROP_HINTS_RECORD_SIZE_DEFAULT = "32";

  const std::string PROP_PG_FILL_PCT = "treeline.page_fill_pct";
  const std::string PROP_PG_FILL_PCT_DEFAULT = "50";

  const std::string PROP_PIN_THREADS = "treeline.pin_threads";
  const std::string PROP_PIN_THREADS_DEFAULT = "1";

  const std::string PROP_DEFERRED_IO_BATCH_SIZE = "treeline.deferred_io_batch_size";
  const std::string PROP_DEFERRED_IO_BATCH_SIZE_DEFAULT = "1";

  const std::string PROP_DEFERRED_IO_MAX_DEFERRALS = "treeline.deferred_io_max_deferrals";
  const std::string PROP_DEFERRED_IO_MAX_DEFERRALS_DEFAULT = "0";

  const std::string PROP_DEFERRAL_AUTOTUNING = "treeline.deferral_autotuning";
  const std::string PROP_DEFERRAL_AUTOTUNING_DEFAULT = "0";

  const std::string PROP_MEMORY_AUTOTUNING = "treeline.memory_autotuning";
  const std::string PROP_MEMORY_AUTOTUNING_DEFAULT = "0";

  const std::string PROP_REORG_LENGTH = "treeline.reorg_length";
  const std::string PROP_REORG_LENGTH_DEFAULT = "5";

  const std::string PROP_REC_CACHE_BATCH_WRITEOUT = "treeline.rec_cache_batch_writeout";
  const std::string PROP_REC_CACHE_BATCH_WRITEOUT_DEFAULT = "1";

  const std::string PROP_OPTIMISTIC_CACHING = "treeline.optimistic_caching";
  const std::string PROP_OPTIMISTIC_CACHING_DEFAULT = "0";

  const std::string PROP_REC_CACHE_USE_LRU = "treeline.rec_cache_use_lru";
  const std::string PROP_REC_CACHE_USE_LRU_DEFAULT = "0";
} // anonymous

namespace ycsbc {
  tl::pg::PageGroupedDB *TreeLineDB::db_ = nullptr;
  int TreeLineDB::ref_cnt_ = 0;
  std::mutex TreeLineDB::mu_;

  typedef union{
      uint64_t key_num;
      char key_char[8];
  } convert;

  void TreeLineDB::Init(){
    const std::lock_guard<std::mutex> lock(mu_);

    const utils::Properties &props = *props_;
    const std::string format = props.GetProperty(PROP_FORMAT, PROP_FORMAT_DEFAULT);
    if (format == "single") {
      format_ = kSingleRow;
      method_read_ = &TreeLineDB::ReadSingle;
      method_scan_ = &TreeLineDB::ScanSingle;
      method_update_ = &TreeLineDB::UpdateSingle;
      method_insert_ = &TreeLineDB::InsertSingle;
      method_delete_ = &TreeLineDB::DeleteSingle;
    } else {
      throw utils::Exception("unknown format");
    }
    fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY, CoreWorkload::FIELD_COUNT_DEFAULT));

    ref_cnt_++;
    if (db_) {
      return;
    }

    tl::pg::PageGroupedDBOptions opts;
    const std::string &db_path = props.GetProperty(PROP_NAME, PROP_NAME_DEFAULT);
    if (db_path == "") {
      throw utils::Exception("TreeLine db path is missing");
    }
    GetOptions(props, &opts);

    tl::Status s;
    if (props.GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true") {
      delete db_;
    }

    bool empty = true;
    if (std::filesystem::exists(db_path) && std::filesystem::is_directory(db_path) && !std::filesystem::is_empty(db_path)) {
      empty = false;
    }
    //s = tl::DB::Open(opts, db_path, &db_);
    tl::pg::PageGroupedDBStats::Local().Reset();
    tl::pg::PageGroupedDBStats::RunOnGlobal([](auto& global_stats) { global_stats.Reset(); });
    s = tl::pg::PageGroupedDB::Open(opts, db_path, &db_);

    if (empty) {
      std::vector<tl::pg::Record> records;
      records.reserve(1024*1024);
      std::cout << "Start generating" << std::endl;
      for (uint64_t i = 1; i < 1024*1024 - 1; i++) {
        tl::pg::Key k = i*16*1024*1024*1024*1024;
        tl::Slice s(std::string(24, '0'));
        records.emplace_back(k, s);
      }
      std::cout << "Finish generating" << std::endl;
      s = db_->BulkLoad(records);
    }

    if (!s.ok()) {
      throw utils::Exception(std::string("TreeLine Open: ") + s.ToString());
    }
  }

  void TreeLineDB::Cleanup() {
    const std::lock_guard<std::mutex> lock(mu_);
    if (--ref_cnt_) {
      return;
    }
    delete db_;
    tl::pg::PageGroupedDBStats::Local().PostToGlobal();
    tl::pg::PageGroupedDBStats::RunOnGlobal([](const auto& stats) {
      // clang-format off
      std::cout << "cache_hits," << stats.GetCacheHits() << std::endl;
      std::cout << "cache_misses," << stats.GetCacheMisses() << std::endl;
      std::cout << "cache_clean_evictions," << stats.GetCacheCleanEvictions() << std::endl;
      std::cout << "cache_dirty_evictions," << stats.GetCacheDirtyEvictions() << std::endl;

      std::cout<< "overflows_created," << stats.GetOverflowsCreated() << std::endl;
      std::cout << "rewrites," << stats.GetRewrites() << std::endl;
      std::cout << "rewrite_input_pages," << stats.GetRewriteInputPages() << std::endl;
      std::cout << "rewrite_output_pages," << stats.GetRewriteOutputPages() << std::endl;

      std::cout << "segments," << stats.GetSegments() << std::endl;
      std::cout << "segment_index_bytes," << stats.GetSegmentIndexBytes() << std::endl;
      std::cout << "free_list_entries," << stats.GetFreeListEntries() << std::endl;
      std::cout << "free_list_bytes," << stats.GetFreeListBytes() << std::endl;
      std::cout << "cache_bytes," << stats.GetCacheBytes() << std::endl;

      std::cout << "overfetched_pages," << stats.GetOverfetchedPages() << std::endl;
    });
  }

  void TreeLineDB::GetOptions(const utils::Properties &props, tl::pg::PageGroupedDBOptions *opts){
    opts->use_segments = true;
    opts->records_per_page_goal = 44;
    opts->records_per_page_epsilon = 5;
    opts->num_bg_threads = 8;
    // Each record cache entry takes 96 bytes of space (metadata).
    opts->record_cache_capacity = (64 * 1024ULL * 1024ULL) /
                                    (32 + 96ULL);
    opts->use_memory_based_io = true;
    opts->bypass_cache = true;
    opts->rec_cache_batch_writeout = true;
    opts->parallelize_final_flush = false;
    opts->optimistic_caching = false;
    opts->rec_cache_use_lru = false;
    opts->use_pgm_builder = true;
    opts->disable_overflow_creation = true;
    opts->rewrite_search_radius = 5;

    opts->forecasting.use_insert_forecasting = false;
    opts->forecasting.num_inserts_per_epoch = 10000;
    opts->forecasting.num_partitions = 10;
    opts->forecasting.sample_size = 1000;
    opts->forecasting.random_seed = 42;
    opts->forecasting.overestimation_factor = 1.5;
    opts->forecasting.num_future_epochs = 1;
  }

  void TreeLineDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
    for (const Field &field : values) {
      uint32_t len = field.name.size();
      data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
      data.append(field.name.data(), field.name.size());
      len = field.value.size();
      data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
      data.append(field.value.data(), field.value.size());
    }
  }

  void TreeLineDB::DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
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

  void TreeLineDB::DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                      const std::vector<std::string> &fields) {
    const char *p = data.data();
    const char *lim = p + data.size();
    DeserializeRowFilter(values, p, lim, fields);
  }

  void TreeLineDB::DeserializeRow(std::vector<Field> &values, const char *p, const char *lim) {
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

  void TreeLineDB::DeserializeRow(std::vector<Field> &values, const std::string &data) {
    const char *p = data.data();
    const char *lim = p + data.size();
    DeserializeRow(values, p, lim);
  }

  DB::Status TreeLineDB::ReadSingle(const std::string &table, const std::string &key, const std::vector<std::string> *fields, std::vector<Field> &result)
  {

    std::string data;
    //tl::ReadOptions options;
    convert c;
    for (int i = 0; i < 8; i++) {
      c.key_char[i] = key.c_str()[i];
    }
    tl::Status s = db_->Get(c.key_num, &data);
    if (s.IsNotFound()) {
      std::cout << c.key_num << std::endl;
      return kNotFound;
    } else if (!s.ok()) {
      throw utils::Exception(std::string("TreeLineDB Get: ") + s.ToString());
    }
    if (fields != nullptr) {
      DeserializeRowFilter(result, data, *fields);
    } else {
      DeserializeRow(result, data);
      assert(result.size() == static_cast<size_t>(fieldcount_));
    }
    return kOK;
  }

  DB::Status TreeLineDB::ScanSingle(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result)
  {
    std::vector<std::pair<tl::pg::Key, std::string>> scan_out;
    convert c;
    for (int i = 0; i < 8; i++) {
      c.key_char[i] = key.c_str()[i];
    }
    tl::Status status = db_->GetRange(c.key_num, len, &scan_out);
    for (auto& record : scan_out) {
      std::string data = record.second;
      result.push_back(std::vector<Field>());
      std::vector<Field> &values = result.back();
      if (fields != nullptr) {
        DeserializeRowFilter(values, data, *fields);
      } else {
        DeserializeRow(values, data);
        assert(values.size() == static_cast<size_t>(fieldcount_));
      }
    }
    return kOK;
  }

  DB::Status TreeLineDB::UpdateSingle(const std::string &table, const std::string &key, std::vector<Field> &values)
  {
    std::string data;
    convert c;
    for (int i = 0; i < 8; i++) {
      c.key_char[i] = key.c_str()[i];
    }
    // tl::Status s = db_->Get(c.key_num, &data);
    // if (s.IsNotFound()) {
    //   return kNotFound;
    // } else if (!s.ok()) {
    //   throw utils::Exception(std::string("TreeLineDB Get: ") + s.ToString());
    // }
    // std::vector<Field> current_values;
    // DeserializeRow(current_values, data);
    // for (Field &new_field : values) {
    //   bool found MAYBE_UNUSED = false;
    //   for (Field &cur_field : current_values) {
    //     if (cur_field.name == new_field.name) {
    //       found = true;
    //       cur_field.value = new_field.value;
    //       break;
    //     }
    //   }
    //   assert(found);
    // }
    tl::pg::WriteOptions write_options;
    write_options.is_update = true;
    SerializeRow(values, data);
    tl::Status s = db_->Put(write_options, c.key_num, data);
    if (!s.ok()) {
      throw utils::Exception(std::string("TreeLine Put: ") + s.ToString());
    }
    return kOK;
  }

  DB::Status TreeLineDB::InsertSingle(const std::string &table, const std::string &key, std::vector<Field> &values)
  {
    std::string data;
    convert c;
    for (int i = 0; i < 8; i++) {
      c.key_char[i] = key.c_str()[i];
    }
    SerializeRow(values, data);
    size_t value_len = data.size();
    tl::pg::WriteOptions write_options;
    write_options.is_update = false;
    tl::Status s = db_->Put(write_options, c.key_num, data);
    if (!s.ok()) {
      throw utils::Exception(std::string("TreeLine Put: ") + s.ToString());
    }
    return kOK;
  }

  DB::Status TreeLineDB::DeleteSingle(const std::string &table, const std::string &key)
  {
    // tl::WriteOptions write_options;
    // write_options.bypass_wal = true;
    // tl::Status s = db_->Delete(write_options, key);
    // if (!s.ok()) {
    //   throw utils::Exception(std::string("TreeLine Delete: ") + s.ToString());
    // }
    return kOK;
  }

  DB *NewTreeLineDB() {
    return new TreeLineDB;
  }

  const bool registered = DBFactory::RegisterDB("treeline", NewTreeLineDB);
}