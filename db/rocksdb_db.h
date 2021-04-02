//
//  rocksdb_db.h
//  YCSB-C
//

#ifndef YCSB_C_ROCKSDB_DB_H_
#define YCSB_C_ROCKSDB_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include <vector>
#include <mutex>
#include "core/properties.h"
#include "tbb/concurrent_unordered_map.h"
#include "rocksdb/db.h"

namespace ycsbc {

class RocksDB : public DB {
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
  static inline const std::string kPropertyDisableWal = "rocksdb.disableWAL";
  static inline const std::string kColumnFamilyNamesFilename = "CF_NAMES";
  static inline std::string rocksdb_dir_ = "";
  static inline std::string option_file_ = "";
  static inline bool encode_field_names_ = true;
  static inline int field_len_ = 0;
  static inline rocksdb::DBOptions db_options_{};
  static inline rocksdb::WriteOptions write_options_{};
  static inline rocksdb::DB *rocksdb_ = nullptr;
  static inline int references_ = 0;
  // Mutex for open RocksDB, construtor of mutex is constexpr.
  static inline std::mutex mutex_{};

  // Column families info and locks, thread should get the lock before add column family. 
  // Before adding CF, thread should create and get the lock.
  // Use reference to avoid unexpected destructing, see:
  // https://google.github.io/styleguide/cppguide.html#Static_and_Global_Variables
  static inline auto & column_families_ = 
        *new tbb::concurrent_unordered_map<std::string, ColumnFamily>{};
  static inline auto & column_family_locks_ = 
        *new tbb::concurrent_unordered_map<std::string, std::recursive_mutex*>{};

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
  
};

} // ycsbc

#endif // YCSB_C_REDIS_DB_H_

