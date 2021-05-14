#pragma once

#include <mutex>
#include "pi/pi.h"
#include "core/db.h"

namespace ycsbc {

class PiTest: public DB {
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

  int Read2(const std::string &table, const std::string &sec_key_field,
                const std::string &sec_key,
                const std::vector<std::string> *fields,
                std::vector<std::vector<KVPair>> &result) override;

 private:
  static inline const std::string kPropertyPiDir = "pi.dir";
  static inline const std::string kPropertyPiOptionsFile = "pi.optionsfile";
  static inline const std::string kPropertyPiThreshold = "pi.threshold";
  static inline Pi *pi_=nullptr;
  static inline std::mutex mutex_{};

  std::string BuildValue(std::vector<KVPair> &values);
};

}