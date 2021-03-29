//
//  rocksdb_db.h
//  YCSB-C
//

#ifndef YCSB_C_PiKV_DB_H_
#define YCSB_C_PiKV_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include <cstdint>
#include <vector>
#include <memory>
#include <mutex>
#include <atomic>
#include "core/properties.h"
#include "tbb/concurrent_unordered_map.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"

namespace ycsbc {

// PiDB: Partial Index key value database based on RocksDB
class PiDB : public DB {
 public:
  void Init() override;
  
  void Close() override;

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) override;

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) override;

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) override;

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) override;

  int Delete(const std::string &table, const std::string &key) override;

 private:
  
  struct ColumnFamily
  {
    rocksdb::ColumnFamilyHandle *handle;
    rocksdb::ColumnFamilyOptions options;
    ColumnFamily() = default;
    ColumnFamily(rocksdb::ColumnFamilyHandle *handle, rocksdb::ColumnFamilyOptions options)
      : handle(handle), options(options) {}
  };
  
  // Google C++ style discourage the use of static string, but YCSB-C has
  // used in property name (see core/core_workload.h), so we still use it here.
  static inline const std::string kPropertyRocksdbDir = "rocksdb.dir";
  static inline const std::string kPropertyRocksdbOptionsFile = "rocksdb.optionsfile";
  static inline const std::string kPropertyEncodeFieldNames = "rocksdb.encodefieldnames";
  static inline const std::string kPropertyBatchSize = "pidb.batchsize";
  static inline const std::string kColumnFamilyNamesFilename = "CF_NAMES";
  static inline std::string rocksdb_dir_ = "";
  static inline std::string option_file_ = "";
  static inline bool encode_field_names_ = true;
  static inline size_t batch_size_ = 0;
  static inline int field_len_ = 0;
  static inline int field_count_ = 0;
  static inline rocksdb::DBOptions db_options_{}; 
  static inline rocksdb::DB *rocksdb_ = nullptr;
  static inline int references_ = 0;
  // Mutex for open RocksDB, construtor of mutex is constexpr.
  static inline std::mutex mutex_{};

  // Column families info and locks, thread should get the lock before add column family. 
  // Before adding CF, thread should create and get the lock.
  // Use reference to avoid unexpected destructing, see:
  // https://google.github.io/styleguide/cppguide.html#Static_and_Global_Variables
  static inline auto &column_families_ = 
        *new tbb::concurrent_unordered_map<std::string, ColumnFamily>{};
  static inline auto &column_family_locks_ = 
        *new tbb::concurrent_unordered_map<std::string, std::recursive_mutex*>{};

  static inline const rocksdb::FilterPolicy *filter_policy_ = nullptr;

  static inline std::atomic<uint32_t> sequence_id_ = 0;

  static inline std::atomic<uint32_t> thread_count_ = 0;
  uint32_t thread_id_;
  
  // Unflush data
  std::unique_ptr<rocksdb::FilterBitsBuilder> filter_builder_;
  std::string current_table_;
  // buffer for unwrite key value pairs.
  std::string buf_;
  // start position of every KV pairs in buf_.
  std::vector<size_t> starts_;

  // simulated SST data

  // batch number of every SST file, if size of every KV is 32B,
  // 1024 kSstSize and 1024 batch_size_ indicate 32 * 1024 * 1024 = 32MB SST file.
  static inline const size_t kSstSize = 1024;
  static inline std::atomic<uint32_t> sst_count_ = 0;
  uint32_t current_sst_id_;
  std::unique_ptr<rocksdb::FilterBitsBuilder> sst_filter_builder_;
  int sst_batch_count_;

  static inline const size_t kMaxSstFiles = 10240;
  // simulate SST read
  static inline uint32_t sst_key_caches[kMaxSstFiles][kSstSize];
  static inline std::string sst_filter_caches[kMaxSstFiles * kSstSize];


  ///
  /// Initializes and opens the RocksDB database.
  /// Should only be called by the thread owns mutex_.
  ///
  /// @return The initialized and open RocksDB instance.
  ///
  rocksdb::DB* InitRocksDBWithOptionsFile();

  ///
  /// Initializes and opens the RocksDB database.
  /// Should only be called by the thread owns mutex_.
  ///
  /// @return The initialized and open RocksDB instance.
  ///
  rocksdb::DB* InitRocksDB();

  void SaveColumnFamilyNames();
  std::vector<std::string> LoadColumnFamilyNames();

  void CreateColumnFamily(const std::string & name);
  rocksdb::ColumnFamilyOptions GetDefaultColumnFamilyOptions(const std::string & name);

  std::string SerializeValues(const std::vector<KVPair> & values);
  void DeserializeValues(const rocksdb::Slice & values, const std::vector<std::string> * fields, 
           std::vector<KVPair> * result);
  
  rocksdb::FilterBitsBuilder * CreateFilterBitsBuilder(const std::string & name);
  int Flush();
  int FlushSST();

};

} // ycsbc

#endif // YCSB_C_REDIS_DB_H_

