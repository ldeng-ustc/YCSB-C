//
//  rocksdb_db.cc
//  YCSB-C
//

#include "rocksdb_db.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <vector>
#include <mutex>
#include <cstring>


#include "core/core_workload.h"
#include "rocksdb/utilities/options_util.h"

using namespace std;

namespace ycsbc {

void RocksDB::Init() {
  unique_lock<mutex> lock(mutex_);
  if(rocksdb_ == nullptr) {
    const auto & props = properties();
    rocksdb_dir_ = props.GetProperty(kPropertyRocksdbDir, "/tmp/db");
    cout << "RocksDB data dir: " << rocksdb_dir_ << endl;
    option_file_ = props.GetProperty(kPropertyRocksdbOptionsFile, "");
    if(option_file_ != "") {
      cout << "RocksDB options file: " << option_file_ << endl;
    }
    // if rocksdb.encodefieldnames == false, just joint all fields values and supose
    // field_len_dist is const, otherwise, encode both fields keys and values (and their length).
    encode_field_names_ = utils::StrToBool(props.GetProperty(kPropertyEncodeFieldNames, "true"));
    cout << "encode: " << encode_field_names_ << endl;
    field_len_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_LENGTH_PROPERTY,
                                            CoreWorkload::FIELD_LENGTH_DEFAULT));
    
    // set write_options
    bool disable_wal = utils::StrToBool(props.GetProperty(kPropertyDisableWal, "false"));
    if(disable_wal) {
      write_options_.disableWAL = true;
    }
    try {
      cout << "Initializing RocksDB..." << endl;
      if (option_file_ != "") {
        rocksdb_ = InitRocksDBWithOptionsFile();
      } else {
        rocksdb_ = InitRocksDB();
      }
      cout << "RocksDB Initialized." << endl;
    } catch (const exception & e) {
      cout << "Exception when initializing RocksDB" << endl;
      cout << "message: " << e.what() << endl;
      throw new utils::Exception(e.what());
    }
  }
  references_++;
}

rocksdb::DB* RocksDB::InitRocksDBWithOptionsFile() {
  rocksdb::Status s;
  rocksdb::DB *db;
  rocksdb::DBOptions options;
  vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
  vector<rocksdb::ColumnFamilyHandle*> cf_handles;

  s = rocksdb::LoadOptionsFromFile(rocksdb::ConfigOptions(), option_file_, &options, &cf_descriptors);
  if(!s.ok()) {
    throw utils::Exception(s.ToString());
  }
  db_options_ = options;
  cout << "CF numbers: " << cf_descriptors.size() << endl;
  s = rocksdb::DB::Open(options, rocksdb_dir_, cf_descriptors, &cf_handles, &db);
  if(!s.ok()) {
    throw utils::Exception(s.ToString());
  }
  for( size_t i=0; i < cf_descriptors.size(); i++ ) {
    string & cf_name = cf_descriptors[i].name;
    rocksdb::ColumnFamilyOptions & cf_options = cf_descriptors[i].options;
    rocksdb::ColumnFamilyHandle *cf_handle = cf_handles[i];
    column_families_[cf_name] = ColumnFamily(cf_handle, cf_options);
  }
  return db;
}

rocksdb::DB* RocksDB::InitRocksDB() {
  rocksdb::Status s;
  rocksdb::DB *db = nullptr;
  const vector<string> && cf_names = LoadColumnFamilyNames();
  vector<rocksdb::ColumnFamilyOptions> cf_optionss;
  vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
  
  for(const string & cf_name: cf_names) {
    auto && cf_options = rocksdb::ColumnFamilyOptions();
    cf_options.OptimizeLevelStyleCompaction();
    auto && cf_descriptor = rocksdb::ColumnFamilyDescriptor(cf_name, cf_options);
    cf_optionss.push_back(cf_options);
    cf_descriptors.push_back(cf_descriptor);
  }

  const int rocks_threads = thread::hardware_concurrency() * 2;

  if(cf_descriptors.empty()) {
    rocksdb::Options options = rocksdb::Options();
    options.OptimizeLevelStyleCompaction();
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.IncreaseParallelism(rocks_threads);
    options.max_background_compactions = rocks_threads;
    options.info_log_level = rocksdb::INFO_LEVEL;
    db_options_ = options;
    s = rocksdb::DB::Open(options, rocksdb_dir_, &db);
    if(!s.ok()) {
      throw utils::Exception(s.ToString());
    }
    return db;
  } else {
    rocksdb::DBOptions options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.IncreaseParallelism(rocks_threads);
    options.max_background_compactions = rocks_threads;
    options.info_log_level = rocksdb::INFO_LEVEL;
    db_options_ = options;

    vector<rocksdb::ColumnFamilyHandle*> cf_handles;
    s = rocksdb::DB::Open(options, rocksdb_dir_, cf_descriptors, &cf_handles, &db);
    if(!s.ok()) {
      throw utils::Exception(s.ToString());
    }
    for( size_t i=0; i < cf_names.size(); i++ ) {
      column_families_[cf_names[i]] = ColumnFamily(cf_handles[i], cf_optionss[i]);
    }
    return db;
  }
}

void RocksDB::Close() {
  DB::Close();

  unique_lock<mutex> lock(mutex_);
  rocksdb::Status s = rocksdb::Status::OK();
  if (references_ == 1) {
    for (auto & cf_pair : column_families_) {
      ColumnFamily cf = cf_pair.second;
      delete cf_pair.second.handle;
    }

    rocksdb::Status s = rocksdb_->Close();
    rocksdb_ = nullptr;

    SaveColumnFamilyNames();
    column_families_.clear();
  }
  references_ --;
  if( !s.ok() ) {
    throw utils::Exception(s.ToString());
  }
}

int RocksDB::Read(const string &table, const string &key,
         const vector<string> *fields,
         vector<KVPair> &result) {
  // cout << "Reading [" << table << "](" << key << ")" << endl;
  // cout << "count: " << column_families_.count(table) << endl;
  // cout << "addr: " << &column_families_ << endl;
  if (column_families_.count(table) == 0) {
    CreateColumnFamily(table);
  }
  // cout << "Creating CF OK!" << endl;
  rocksdb::ColumnFamilyHandle *cf = column_families_[table].handle;
  rocksdb::Status s;
  string val;
  s = rocksdb_->Get(rocksdb::ReadOptions(), cf, key, &val);
  if(s.IsNotFound()) {
    cout << "Not Found Key: " << key << endl;
    return DB::kErrorNoData;
  }
  if(!s.ok()) {
    throw utils::Exception(s.ToString());
  }
  // cout << "Get OK!" << endl;
  DeserializeValues(val, fields, &result);
  return DB::kOK;
}

int RocksDB::Scan(const std::string &table, const std::string &key,
         int len, const std::vector<std::string> *fields,
         std::vector<std::vector<KVPair>> &result) {
  if (!column_families_.count(table)) {
    CreateColumnFamily(table);
  }  
  rocksdb::ColumnFamilyHandle *cf = column_families_[table].handle;
  rocksdb::Iterator *iterator = rocksdb_->NewIterator(rocksdb::ReadOptions(), cf);
  
  iterator->Seek(key);
  for( int i = 0; iterator->Valid() && i < len; iterator->Next(), i++ ) {
    vector<KVPair> values;
    DeserializeValues(iterator->value(), fields, &values);
    result.push_back(move(values));
  }
  return DB::kOK;
}

int RocksDB::Update(const string &table, const string &key,
           vector<KVPair> &values) {
  // cout << "Updating [" << table << "](" << key << ", " << values.size() << ")" << endl;
  if (!column_families_.count(table)) {
    CreateColumnFamily(table);
  }

  rocksdb::ColumnFamilyHandle *cf = column_families_[table].handle;
  rocksdb::Status s;
  vector<KVPair> result;
  string current_values;
  s = rocksdb_->Get(rocksdb::ReadOptions(), cf, key, &current_values);
  if(s == rocksdb::Status::NotFound()) {
    return DB::kErrorNoData;
  }
  if(!s.ok()) {
    throw utils::Exception(s.ToString());
  }
  DeserializeValues(current_values, nullptr, &result);

  // update
  result.reserve(result.size() + distance(values.begin(), values.end()));
  result.insert(result.end(), values.begin(), values.end());

  // store
  s = rocksdb_->Put(write_options_, cf, key, SerializeValues(result));
  if(!s.ok()) {
    throw utils::Exception(s.ToString());
  }
  return DB::kOK;
}

int RocksDB::Insert(const std::string &table, const std::string &key,
           std::vector<KVPair> &values) {
  // cout << "Inserting [" << table << "](" << key << ", " << values.size() << ")" << endl;
  if (!column_families_.count(table)) {
    CreateColumnFamily(table);
  }

  rocksdb::ColumnFamilyHandle *cf = column_families_[table].handle;
  rocksdb::Status s;
  s = rocksdb_->Put(write_options_, cf, key, SerializeValues(values));
  if(!s.ok()) {
    cout << "RocksDB Error: " << s.ToString() << endl;
    throw utils::Exception(s.ToString());
  }
  return DB::kOK;
}

int RocksDB::Delete(const std::string &table, const std::string &key) {
  cout << "Deleting [" << table << "](" << key << ")" << endl;
  if (!column_families_.count(table)) {
    CreateColumnFamily(table);
  }

  rocksdb::ColumnFamilyHandle *cf = column_families_[table].handle;
  rocksdb::Status s;
  s = rocksdb_->Delete(rocksdb::WriteOptions(), cf, key);
  if(s == rocksdb::Status::NotFound()) {
    return DB::kErrorNoData;
  }
  if(!s.ok()) {
    throw utils::Exception(s.ToString());
  }
  return DB::kOK;
}

void RocksDB::SaveColumnFamilyNames() {
  try {
    ofstream fout(rocksdb_dir_ + "/" + kColumnFamilyNamesFilename);
    if(column_families_.count(rocksdb::kDefaultColumnFamilyName) == 0) {
      fout << rocksdb::kDefaultColumnFamilyName << endl;
    }
    for(const auto & cf_pairs: column_families_) {
      fout << cf_pairs.first << endl;
    }
  } catch(const exception & e) {
    throw utils::Exception(e.what());
  }
}

vector<string> RocksDB::LoadColumnFamilyNames() {
  vector<string> result;
  try {
    ifstream fin(rocksdb_dir_ + "/" + kColumnFamilyNamesFilename);
    string cf_name;
    while(fin >> cf_name) {
      result.push_back(move(cf_name));
    }
  } catch(const exception & e) {
    throw utils::Exception(e.what());
  }
  return result;
}

rocksdb::ColumnFamilyOptions RocksDB::GetDefaultColumnFamilyOptions(const std::string & name) {
  rocksdb::ColumnFamilyOptions cf_options;
  if(column_families_.count(rocksdb::kDefaultColumnFamilyName)) {
    cout << "no column family options for \"" + name + "\"" +
                  " in options file - using options from " +
                  "\"" + rocksdb::kDefaultColumnFamilyName + "\"" << endl;
    cf_options = column_families_[rocksdb::kDefaultColumnFamilyName].options;
  } else {
    cout << "no column family options for either \"" + name + "\" or " +
                  "\"" + rocksdb::kDefaultColumnFamilyName + "\"" + 
                  " in options file - initializing with empty configuration" << endl;
  }
  cout << "Add a cf_options section for \"" + name + "\" to the options file, " +
                "or subsequent runs on this DB will fail." << endl;
  return cf_options;
}

void RocksDB::CreateColumnFamily(const std::string & name) {
  cout << "Creating column family: " << name << endl;
  // Concurrent insertion of new mutex will be ignore by concurrent_unordered_map, 
  // and insert function will return false.
  // See https://www.threadingbuildingblocks.org/docs/help/reference/containers_overview/concurrent_unordered_map_cls.html
  recursive_mutex *r_mutex = new recursive_mutex();
  auto [_, res] = column_family_locks_.insert(make_pair(name, r_mutex));
  if(! res) {
    cout << "Delete failed r_mutex" << endl;
    delete r_mutex;
  }
  lock_guard<recursive_mutex> lock(*column_family_locks_[name]);
  cout << "Lock OK!" << endl;
  if( !column_families_.count(name) ) {
    cout << "Start creating." << endl;
    rocksdb::ColumnFamilyOptions cf_options;
    if( option_file_ != "" ) {
      // RocksDB requires all options files to include options for the "default" column family;
      // apply those options to this column family
      cf_options = GetDefaultColumnFamilyOptions(name);
    } else {
      cf_options.OptimizeLevelStyleCompaction();
    }
    cout << "Option OK!" << endl;
    rocksdb::ColumnFamilyHandle *cf_handle = nullptr;
    rocksdb::Status s = rocksdb_->CreateColumnFamily(cf_options, name, &cf_handle);
    if( !s.ok() ) {
      cout << "Create Column Family failed!" << endl;
      cout << "message: " << s.ToString() << endl;
      throw utils::Exception (s.ToString());
    }
    column_families_[name] = ColumnFamily(cf_handle, cf_options);
    cout << "Column Family created!" << endl;
    cout << "count: " << column_families_.count(name) << endl;
    cout << "addr: " << &column_families_ << endl;
  }
}

string RocksDB::SerializeValues(const vector<KVPair> & values) {
  // if rocksdb.encodefieldnames == false, just joint all fields values and supose
  // field_len_dist is const, otherwise, encode both fields keys and values (and their length).
  string result;
  size_t len = 0;
  for(const KVPair & kv_pair: values) {
    len += kv_pair.second.length();
    if(encode_field_names_) {
      // space to encode key and value length and field key.
      len += 8 + kv_pair.first.length();
    }
  }
  result.reserve(len);
  for(const KVPair & kv_pair: values) {
    const string & key = kv_pair.first;
    const string & val = kv_pair.second;
    if(encode_field_names_) {
      // encode key length
      for(int i=0; i<4; i++) {
        result += static_cast<char>( key.length() >> (8 * i));
      }
      // append key
      result.append(key);
      //encode val length
      for(int i=0; i<4; i++) {
        result += static_cast<char>( val.length() >> (8 * i));
      }
    }
    // append val
    result.append(val);
  }
  return result;
}

void RocksDB::DeserializeValues(const rocksdb::Slice & values, const vector<string> * fields, 
         vector<KVPair> * result) {
  // cout << "Deserializing" << endl;
  const char *data = values.data();
  size_t len = values.size();
  size_t offset = 0;
  while(offset < len) {
    string key;
    string val;
    if(encode_field_names_){
      int key_length = 0;
      // decode key length
      for(int i=0; i<4; i++) {
        key_length += static_cast<unsigned char>((data + offset)[i]) << (8 * i);
      }
      offset += 4;
      // decode key
      key = string(data + offset, key_length);
      offset += key_length;
      // decode value length
      int val_length = 0;
      for(int i=0; i<4; i++) {
        val_length += static_cast<unsigned char>((data + offset)[i]) << (8 * i);
      }
      // cout << "vallen: " << val_length << endl;
      offset += 4;
      // decode value
      val = string(data + offset, val_length);
      offset += val_length;
    } else {
      key = "field" + to_string(result->size() + 1);
      val = string(data + offset, field_len_);
      offset += field_len_;
    }
    // if fields is null or contains key
    if(fields == nullptr || find(fields->begin(), fields->end(), key) != fields->end()) {
      result->push_back(make_pair(move(key), move(val)));
    }
  }
}


} // namespace ycsbc
