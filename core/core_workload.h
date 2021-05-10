//
//  core_workload.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CORE_WORKLOAD_H_
#define YCSB_C_CORE_WORKLOAD_H_

#include <vector>
#include <string>
#include "db.h"
#include "properties.h"
#include "generator.h"
#include "discrete_generator.h"
#include "counter_generator.h"
#include "utils.h"

namespace ycsbc {

enum Operation {
  INSERT,
  READ,
  UPDATE,
  SCAN,
  READMODIFYWRITE,
  READBYSECONDARY,
};

class CoreWorkload {
 public:
  /// 
  /// The name of the database table to run queries against.
  ///
  static const std::string TABLENAME_PROPERTY;
  static const std::string TABLENAME_DEFAULT;
  
  /// 
  /// The name of the property for the number of fields in a record.
  ///
  static const std::string FIELD_COUNT_PROPERTY;
  static const std::string FIELD_COUNT_DEFAULT;
  
  /// 
  /// The name of the property for the field length distribution.
  /// Options are "uniform", "zipfian" (favoring short records), and "constant".
  ///
  static const std::string FIELD_LENGTH_DISTRIBUTION_PROPERTY;
  static const std::string FIELD_LENGTH_DISTRIBUTION_DEFAULT;
  
  /// 
  /// The name of the property for the length of a field in bytes.
  ///
  static const std::string FIELD_LENGTH_PROPERTY;
  static const std::string FIELD_LENGTH_DEFAULT;
  
  /// 
  /// The name of the property for deciding whether to read one field (false)
  /// or all fields (true) of a record.
  ///
  static const std::string READ_ALL_FIELDS_PROPERTY;
  static const std::string READ_ALL_FIELDS_DEFAULT;

  /// 
  /// The name of the property for deciding whether to write one field (false)
  /// or all fields (true) of a record.
  ///
  static const std::string WRITE_ALL_FIELDS_PROPERTY;
  static const std::string WRITE_ALL_FIELDS_DEFAULT;
  
  /// 
  /// The name of the property for the proportion of read transactions.
  ///
  static const std::string READ_PROPORTION_PROPERTY;
  static const std::string READ_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the proportion of update transactions.
  ///
  static const std::string UPDATE_PROPORTION_PROPERTY;
  static const std::string UPDATE_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the proportion of insert transactions.
  ///
  static const std::string INSERT_PROPORTION_PROPERTY;
  static const std::string INSERT_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the proportion of scan transactions.
  ///
  static const std::string SCAN_PROPORTION_PROPERTY;
  static const std::string SCAN_PROPORTION_DEFAULT;
  
  ///
  /// The name of the property for the proportion of
  /// read-modify-write transactions.
  ///
  static const std::string READMODIFYWRITE_PROPORTION_PROPERTY;
  static const std::string READMODIFYWRITE_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of
  /// read-by-secondary-key transactions.
  ///
  static const std::string READBYSECONDARY_PROPORTION_PROPERTY;
  static const std::string READBYSECONDARY_PROPORTION_DEFAULT;
  
  /// 
  /// The name of the property for the the distribution of request keys.
  /// Options are "uniform", "zipfian" and "latest".
  ///
  static const std::string REQUEST_DISTRIBUTION_PROPERTY;
  static const std::string REQUEST_DISTRIBUTION_DEFAULT;
  
  ///
  /// The name of the property for adding zero padding to record numbers in order to match 
  /// string sort order. Controls the number of 0s to left pad with.
  ///
  static const std::string ZERO_PADDING_PROPERTY;
  static const std::string ZERO_PADDING_DEFAULT;

  /// 
  /// The name of the property for the max scan length (number of records).
  ///
  static const std::string MAX_SCAN_LENGTH_PROPERTY;
  static const std::string MAX_SCAN_LENGTH_DEFAULT;
  
  /// 
  /// The name of the property for the scan length distribution.
  /// Options are "uniform" and "zipfian" (favoring short scans).
  ///
  static const std::string SCAN_LENGTH_DISTRIBUTION_PROPERTY;
  static const std::string SCAN_LENGTH_DISTRIBUTION_DEFAULT;

  /// 
  /// The name of the property for the order to insert records.
  /// Options are "ordered" or "hashed".
  ///
  static const std::string INSERT_ORDER_PROPERTY;
  static const std::string INSERT_ORDER_DEFAULT;

  static const std::string INSERT_START_PROPERTY;
  static const std::string INSERT_START_DEFAULT;
  
  static const std::string RECORD_COUNT_PROPERTY;
  static const std::string OPERATION_COUNT_PROPERTY;

  ///
  /// The name of the property for the count of secondary key fields.
  /// Must less than or equal fieldcount.
  ///
  static const std::string SECONDARY_KEY_FIELD_COUNT_PROPERTY;
  static const std::string SECONDARY_KEY_FIELD_COUNT_DEFAULT;

  /// 
  /// The name of the property for the distribution of secondary keys.
  /// Options are "uniform", "zipfian".
  ///
  static const std::string SECONDARY_KEY_DISTRIBUTION_PROPERTY;
  static const std::string SECONDARY_KEY_DISTRIBUTION_DEFAULT;

  /// 
  /// The name of the property for the unique of secondary keys count of every
  /// secondary key field. When generating, secondary keys will be chosen from this
  /// number of keys and follow the secondary key distribution.
  /// If zero, number of sec. key will equal to recordcount.
  ///
  static const std::string UNIQUE_SECONDARY_KEY_COUNT_PROPERTY;
  static const std::string UNIQUE_SECONDARY_KEY_COUNT_DEFAULT;

  /// 
  /// The name of the property for the request distribution of secondary keys.
  /// Secondary keys used by secondary operations (i.e. read-by-secondary-key) will
  /// follow this distribution. 
  /// Options are "uniform", "zipfian".
  ///
  static const std::string SECONDARY_REQUEST_DISTRIBUTION_PROPERTY;
  static const std::string SECONDARY_REQUEST_DISTRIBUTION_DEFAULT;

  static const std::string kKeyPrefix;
  static const std::string kSecondaryKeyPrefix;
  static const std::string kFieldNamePrefix;
  static const std::string kSecondaryKeyFieldNamePrefix;

  ///
  /// Initialize the scenario.
  /// Called once, in the main client thread, before any operations are started.
  ///
  virtual void Init(const utils::Properties &p);
  
  virtual void BuildValues(std::vector<ycsbc::DB::KVPair> &values);
  virtual void BuildUpdate(std::vector<ycsbc::DB::KVPair> &update);
  
  virtual std::string NextTable() { return table_name_; }
  virtual std::string NextSequenceKey(); /// Used for loading data
  virtual std::string NextTransactionKey(); /// Used for transactions
  /// Used for transactions of secondary operations
  virtual std::string NextSecondaryKey(size_t key_field);
  virtual Operation NextOperation() { return op_chooser_.Next(); }
  virtual std::string NextFieldName();
  virtual size_t NextSecondaryKeyField();
  virtual std::string GetKeyFieldName(size_t key_field);
  virtual size_t NextScanLength() { return scan_len_chooser_->Next(); }
  
  bool read_all_fields() const { return read_all_fields_; }
  bool write_all_fields() const { return write_all_fields_; }

  CoreWorkload() :
      field_count_(0), secondary_key_field_count_(0), read_all_fields_(false), write_all_fields_(false),
      field_len_generator_(NULL), key_generator_(NULL), key_chooser_(NULL),
      field_chooser_(NULL), scan_len_chooser_(NULL), insert_key_sequence_(3),
      secondary_key_field_chooser_(NULL), ordered_inserts_(true), record_count_(0) {
  }
  
  virtual ~CoreWorkload() {
    if (field_len_generator_) delete field_len_generator_;
    if (key_generator_) delete key_generator_;
    if (key_chooser_) delete key_chooser_;
    if (field_chooser_) delete field_chooser_;
    if (scan_len_chooser_) delete scan_len_chooser_;
  }
  
 protected:
  static Generator<uint64_t> *GetFieldLenGenerator(const utils::Properties &p);
  std::string BuildKeyName(uint64_t key_num);
  std::string BuildKeyName(uint64_t key_num, const std::string prefix,
                                  bool ordered,int zero_padding);

  std::string table_name_;
  int field_count_;
  int secondary_key_field_count_;
  bool read_all_fields_;
  bool write_all_fields_;
  Generator<uint64_t> *field_len_generator_;
  Generator<uint64_t> *key_generator_;
  DiscreteGenerator<Operation> op_chooser_;
  Generator<uint64_t> *key_chooser_;
  Generator<uint64_t> *field_chooser_;
  Generator<uint64_t> *scan_len_chooser_;
  CounterGenerator insert_key_sequence_;
  Generator<uint64_t> *secondary_key_field_chooser_;
  std::vector<Generator<uint64_t>*> secondary_key_generators_;
  std::vector<Generator<uint64_t>*> secondary_key_choosers_;
  bool ordered_inserts_;
  size_t record_count_;
  int zero_padding_;
};

inline std::string CoreWorkload::NextSequenceKey() {
  uint64_t key_num = key_generator_->Next();
  return BuildKeyName(key_num);
}

inline std::string CoreWorkload::NextTransactionKey() {
  uint64_t key_num;
  do {
    key_num = key_chooser_->Next();
  } while (key_num > insert_key_sequence_.Last());
  return BuildKeyName(key_num);
}

inline std::string CoreWorkload::NextSecondaryKey(size_t key_field) {
  uint64_t key_num = secondary_key_choosers_[key_field]->Next();
  return BuildKeyName(key_num, kSecondaryKeyPrefix, true, zero_padding_);
}

inline std::string CoreWorkload::BuildKeyName(uint64_t key_num, 
            const std::string prefix, bool ordered, int zero_padding) {
  if (!ordered) {
    key_num = utils::Hash(key_num);
  }
  std::string key_num_str = std::to_string(key_num);
  int zeros = zero_padding - key_num_str.length();
  zeros = std::max(0, zeros);
  return std::string(prefix).append(zeros, '0').append(key_num_str);
}

inline std::string CoreWorkload::BuildKeyName(uint64_t key_num) {
  return BuildKeyName(key_num, kKeyPrefix, ordered_inserts_, zero_padding_);
}

inline std::string CoreWorkload::NextFieldName() {
  return std::string(kFieldNamePrefix).append(std::to_string(field_chooser_->Next()));
}

inline size_t CoreWorkload::NextSecondaryKeyField() {
  assert(secondary_key_field_count_ > 0);
  return secondary_key_field_chooser_->Next();
}

inline std::string CoreWorkload::GetKeyFieldName(size_t key_field) {
  assert(secondary_key_field_count_ > 0);
  return kSecondaryKeyFieldNamePrefix + std::to_string(key_field);
}
  
} // ycsbc

#endif // YCSB_C_CORE_WORKLOAD_H_
