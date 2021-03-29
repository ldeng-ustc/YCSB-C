//
//  rocksdb_db.cc
//  YCSB-C
//

#include "pidb.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <vector>
#include <mutex>
#include <thread>
#include <cstring>


#include "core/core_workload.h"
#include "rocksdb/utilities/options_util.h"

using namespace std;

namespace ycsbc {

void PiDB::Init() {
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
    field_len_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_LENGTH_PROPERTY,
                                            CoreWorkload::FIELD_LENGTH_DEFAULT));
    field_count_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));
    batch_size_ = std::stoul(props.GetProperty(kPropertyBatchSize, "1024"));

    // Initialize filter_policy_
    filter_policy_ = rocksdb::NewBloomFilterPolicy(9.9);
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
  filter_builder_ = nullptr;
  references_++;
  thread_id_ = thread_count_.fetch_add(1);
  cout << "Thread " << thread_id_ << " initialized." << endl;
}

rocksdb::DB* PiDB::InitRocksDBWithOptionsFile() {
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

rocksdb::DB* PiDB::InitRocksDB() {
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

int PiDB::Read(const string &table, const string &key,
         const vector<string> *fields,
         vector<KVPair> &result) {
  if (!column_families_.count(table)) {
    CreateColumnFamily(table);
  }
  rocksdb::ColumnFamilyHandle *cf = column_families_[table].handle;
  rocksdb::Iterator *iter = rocksdb_->NewIterator(rocksdb::ReadOptions());
  iter->Seek("B0");
  for(; iter->Valid() && iter->key()[0] == 'B'; iter->Next()) {
    auto r = filter_policy_->GetFilterBitsReader(iter->value());
    if (r->MayMatch(key)) {
      rocksdb::Slice sst_key = iter->key();
      sst_key.remove_prefix(1);
      uint32_t sst_id = stoul(sst_key.ToString());
      for(uint32_t seq_id: sst_key_caches[sst_id]) {
        auto r2 = filter_policy_->GetFilterBitsReader(iter->value());
        if(r2->MayMatch(key)) {
          rocksdb::Slice batch_key(reinterpret_cast<char*>(&seq_id), 4);
          string batch;
          rocksdb::Status s = rocksdb_->Get(rocksdb::ReadOptions(), cf, batch_key, &batch);
          if(!s.ok()) {
            cout << "RocksDB Error: " << s.ToString() << endl;
            throw utils::Exception(s.ToString());
          }
          auto pos = batch.find(key);
          if(pos != string::npos) {
            string val = batch.substr(pos, field_len_ * field_count_);
            DeserializeValues(val, fields, &result);
            return DB::kOK;
          }
        }
      }
    }
  }
  return DB::kErrorNoData;
}

int PiDB::Scan(const std::string &table, const std::string &key,
         int len, const std::vector<std::string> *fields,
         std::vector<std::vector<KVPair>> &result) {
  return DB::kOK;
}

int PiDB::Update(const string &table, const string &key,
           vector<KVPair> &values) {
  return DB::kOK;
}

rocksdb::FilterBitsBuilder* PiDB::CreateFilterBitsBuilder(const std::string & name) {
    rocksdb::TableFactory* tf = column_families_[name].options.table_factory.get();
    auto opts = tf->GetOptions<rocksdb::BlockBasedTableOptions>();
    auto context = rocksdb::FilterBuildingContext(*opts);
    return filter_policy_->GetBuilderWithContext(context);
}

int PiDB::FlushSST() {
  rocksdb::Slice sst_key("B" + std::to_string(current_sst_id_));
  cout << "sst key: " << sst_key.ToStringView() << endl;
  std::unique_ptr<const char []> filter_bits;
  rocksdb::Slice filter_slice = sst_filter_builder_->Finish(&filter_bits);
  rocksdb::Status s = rocksdb_->Put(rocksdb::WriteOptions(), sst_key, filter_slice);
  cout << "write filter len: " << filter_slice.size() << endl;
  if(!s.ok()) {
    cout << "RocksDB Error: " << s.ToString() << endl;
    throw utils::Exception(s.ToString());
  }
  sst_filter_builder_.reset();
  return DB::kOK;
}

int PiDB::Flush() {
  rocksdb::ColumnFamilyHandle *cf = column_families_[current_table_].handle;
  rocksdb::Status s;
  uint32_t seq_id = sequence_id_.fetch_add(1);
  rocksdb::Slice batch_key(reinterpret_cast<char*>(&seq_id), 4);

  s = rocksdb_->Put(rocksdb::WriteOptions(), cf, batch_key, buf_);
  if(!s.ok()) {
    cout << "RocksDB Error: " << s.ToString() << endl;
    throw utils::Exception(s.ToString());
  }

  // Build filters and put into default CF.
  std::unique_ptr<const char []> filter_bits;
  rocksdb::Slice filter_slice = filter_builder_->Finish(&filter_bits);
  s = rocksdb_->Put(rocksdb::WriteOptions(), batch_key, filter_slice);
  if(!s.ok()) {
    cout << "RocksDB Error: " << s.ToString() << endl;
    throw utils::Exception(s.ToString());
  }

  sst_filter_caches[seq_id] = buf_;
  sst_key_caches[current_sst_id_][sst_batch_count_] = seq_id;
  sst_batch_count_ ++;

  // Clear buffer
  starts_.clear();
  buf_.clear();
  filter_builder_.reset();

  if(sst_batch_count_ == kSstSize) {
    return FlushSST();
  }

  return DB::kOK;
}

int PiDB::Insert(const std::string &table, const std::string &key,
           std::vector<KVPair> &values) {
  // cout << "Inserting [" << table << "](" << key << ", " << values.size() << ")" << endl;
  if (!column_families_.count(table)) {
    CreateColumnFamily(table);
  }

  if (sst_filter_builder_ == nullptr) {
    sst_filter_builder_.reset(CreateFilterBitsBuilder(table));
    sst_batch_count_ = 0;
    current_sst_id_ = sst_count_.fetch_add(1);
  }

  if (filter_builder_ == nullptr) {
    filter_builder_.reset(CreateFilterBitsBuilder(table));
    current_table_ = table;
  }

  starts_.push_back(buf_.size());
  buf_.append(key);
  buf_.append(SerializeValues(values));
  filter_builder_->AddKey(key);
  sst_filter_builder_->AddKey(key);

  if(starts_.size() == batch_size_) {
    return Flush();
  }
  return DB::kOK;
}

int PiDB::Delete(const std::string &table, const std::string &key) {
  return DB::kOK;
}

void PiDB::Close() {
  unique_lock<mutex> lock(mutex_);
  if(!buf_.empty()){
    Flush();
  }
  if(sst_batch_count_) {
    FlushSST();
  }
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

  DB::Close();
}

void PiDB::SaveColumnFamilyNames() {
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

vector<string> PiDB::LoadColumnFamilyNames() {
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

rocksdb::ColumnFamilyOptions PiDB::GetDefaultColumnFamilyOptions(const std::string & name) {
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

void PiDB::CreateColumnFamily(const std::string & name) {
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

string PiDB::SerializeValues(const vector<KVPair> & values) {
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

void PiDB::DeserializeValues(const rocksdb::Slice & values, const vector<string> * fields, 
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
      // cout << "keylen: " << key_length << endl;
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
