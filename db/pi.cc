#include "core/core_workload.h"
#include "fmt/format.h"
#include "pi.h"
using namespace std;

using rocksdb::Status;
using fmt::format;

namespace ycsbc{

void PiTest::Init() {
  unique_lock<mutex> lock(mutex_);
  if(pi_ == nullptr) {
    const auto & props = properties();
    string dir = props.GetProperty(kPropertyPiDir, "/tmp/db");
    cout << "Pi data dir: " << dir << endl;
    string opt_file = props.GetProperty(kPropertyPiOptionsFile, "");
    if(opt_file != "") {
      cout << "Pi options file: " << opt_file << endl;
    }

    PiOptions opt;
    opt.index_threshold = stoul(props.GetProperty(kPropertyPiThreshold, "100"));
    opt.options_file = opt_file;
    opt.remove_existing_db = true;
    opt.sketch_height = 8;
    opt.sketch_width = 3000 * 1000;
    size_t key_prefix_len = CoreWorkload::kSecondaryKeyPrefix.length();
    size_t number_len = std::stoul(props.GetProperty(
              CoreWorkload::ZERO_PADDING_PROPERTY,
              CoreWorkload::ZERO_PADDING_DEFAULT)); 
    size_t sk_len = key_prefix_len + number_len;
    opt.parser = new Parser(sk_len);
    pi_ = new Pi(dir, opt);
  }
}

void PiTest::Close() {
  PiStat stat = pi_->stat();
  cout << format("sec. index count: {}", stat.sec_index_count) << endl;
  cout << format("sec. item count: {}", stat.sec_item_count) << endl;
}

int PiTest::Read(const std::string &table, const std::string &key,
        const std::vector<std::string> *fields,
        std::vector<KVPair> &result) {
  string val;
  Status s = pi_->Get(key, &val);
  if(!s.ok()){
    return s.code();
  }
  result.push_back(KVPair("", val));
  return kOK;
}

int PiTest::Scan(const std::string &table, const std::string &key,
        int len, const std::vector<std::string> *fields,
        std::vector<std::vector<KVPair>> &result) {
  return 0;
}

int PiTest::Update(const std::string &table, const std::string &key,
            std::vector<KVPair> &values) {
  return 0;
}

string PiTest::BuildValue(std::vector<KVPair> &values) {
  string result;
  for(const KVPair & kv_pair: values) {
    const string & val = kv_pair.second;
    result.append(val);
  }
  return result;
}

int PiTest::Insert(const std::string &table, const std::string &key,
            std::vector<KVPair> &values) {
  Status s = pi_->Put(key, BuildValue(values));
  return s.code();
}

int PiTest::Delete(const std::string &table, const std::string &key) {
  return kOK;
}

int PiTest::Read2(const std::string &table, const std::string &sec_key_field,
            const std::string &sec_key,
            const std::vector<std::string> *fields,
            std::vector<std::vector<KVPair>> &result) {
  vector<string> vals;
  Status s = pi_->Get2(sec_key, &vals);
  if(!s.ok()){
    return s.code();
  }
  for(auto s: vals) {
    result.push_back({KVPair("", s)});
  }
  //cout << result.size() <<endl;
  return kOK;
}

};