//
//  db.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_DB_H_
#define YCSB_C_DB_H_

#include <vector>
#include <string>
#include <memory>
#include <iostream>

#include "utils.h"
#include "properties.h"

namespace ycsbc {

class DB {
 public:
  typedef std::pair<std::string, std::string> KVPair;
  static const int kOK = 0;
  static const int kErrorNoData = 1;
  static const int kErrorConflict = 2;

  ///
  /// Set the properties for this DB.
  ///
  virtual void set_properties(const utils::Properties & p) {
    properties_.reset(&p);
  }

  ///
  /// Get the set of properties for this DB.
  ///
  virtual const utils::Properties & properties() {
    return *properties_;
  }


  ///
  /// Initializes any state for accessing this DB.
  /// Called once per DB client (thread); there is a single DB instance globally.
  ///
  virtual void Init() { }
  ///
  /// Clears any state for accessing this DB.
  /// Called once per DB client (thread); there is a single DB instance globally.
  ///
  virtual void Close() { }
  ///
  /// Reads a record from the database.
  /// Field/value pairs from the result are stored in a vector.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to read.
  /// @param fields The list of fields to read, or NULL for all of them.
  /// @param result A vector of field/value pairs for the result.
  /// @return Zero on success, or a non-zero error code on error/record-miss.
  ///
  virtual int Read(const std::string &table, const std::string &key,
                   const std::vector<std::string> *fields,
                   std::vector<KVPair> &result) = 0;
  ///
  /// Performs a range scan for a set of records in the database.
  /// Field/value pairs from the result are stored in a vector.
  ///
  /// @param table The name of the table.
  /// @param key The key of the first record to read.
  /// @param record_count The number of records to read.
  /// @param fields The list of fields to read, or NULL for all of them.
  /// @param result A vector of vector, where each vector contains field/value
  ///        pairs for one record
  /// @return Zero on success, or a non-zero error code on error.
  ///
  virtual int Scan(const std::string &table, const std::string &key,
                   int record_count, const std::vector<std::string> *fields,
                   std::vector<std::vector<KVPair>> &result) = 0;
  ///
  /// Updates a record in the database.
  /// Field/value pairs in the specified vector are written to the record,
  /// overwriting any existing values with the same field names.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to write.
  /// @param values A vector of field/value pairs to update in the record.
  /// @return Zero on success, a non-zero error code on error.
  ///
  virtual int Update(const std::string &table, const std::string &key,
                     std::vector<KVPair> &values) = 0;
  ///
  /// Inserts a record into the database.
  /// Field/value pairs in the specified vector are written into the record.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to insert.
  /// @param values A vector of field/value pairs to insert in the record.
  /// @return Zero on success, a non-zero error code on error.
  ///
  virtual int Insert(const std::string &table, const std::string &key,
                     std::vector<KVPair> &values) = 0;
  ///
  /// Deletes a record from the database.
  ///
  /// @param table The name of the table.
  /// @param key The key of the record to delete.
  /// @return Zero on success, a non-zero error code on error.
  ///
  virtual int Delete(const std::string &table, const std::string &key) = 0;
  ///
  /// Reads all records with specified secondary key from the database.
  /// Field/value pairs from the result are stored in a vector.
  ///
  /// @param table The name of the table.
  /// @param sec_key_field The key field name of the secondary key to search.
  /// @param sec_key The secondary key name to read.
  /// @param fields The list of fields to read, or NULL for all of them.
  /// @param result A vector of results, every result is a vector of field/value pairs.
  /// @return Zero on success, or a non-zero error code on error/record-miss.
  ///
  virtual int Read2(const std::string &table, const std::string &sec_key_field,
                   const std::string &sec_key,
                   const std::vector<std::string> *fields,
                   std::vector<std::vector<KVPair>> &result) {
    throw utils::Exception("Operation 'Read2' unimplement.");
  }
  
  virtual ~DB() { }

 private:

  ///
  /// Properties for configuring this DB.
  ///
  std::shared_ptr<const utils::Properties> properties_;

};

} // ycsbc

#endif // YCSB_C_DB_H_
