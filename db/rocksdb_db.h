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
  RocksDB(utils::Properties &props);

  void Init();
  
  void Close();

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result);

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result);

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Delete(const std::string &table, const std::string &key);

 private:
  
  struct ColumnFamily
  {
    rocksdb::ColumnFamilyHandle *handle;
    rocksdb::ColumnFamilyOptions options;
    ColumnFamily() = default;
    ColumnFamily(rocksdb::ColumnFamilyHandle *handle, rocksdb::ColumnFamilyOptions options)
      : handle(handle), options(options) {}
  };
  
  
  static inline const std::string kPropertyRocksdbDir = "rocksdb.dir";
  static inline const std::string kPropertyRocksdbOptionsFile = "rocksdb.optionsfile";
  static inline const std::string kColumnFamilyNamesFilename = "CF_NAMES";
  static inline std::string rocksdb_dir_ = "";
  static inline std::string option_file_ = "";
  static inline rocksdb::DBOptions db_options_{};
  static inline rocksdb::DB *rocksdb_ = nullptr;
  static inline int references_ = 0;
  static inline std::mutex mutex_{};

  static inline tbb::concurrent_unordered_map<std::string, ColumnFamily> column_families_{};
  static inline tbb::concurrent_unordered_map<std::string, std::recursive_mutex*> column_family_locks_{};

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

