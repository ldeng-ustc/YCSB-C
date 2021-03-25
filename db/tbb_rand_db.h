//
//  tbb_rand_db.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/26/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_TBB_RAND_DB_H_
#define YCSB_C_TBB_RAND_DB_H_

#include "db/hashtable_db.h"

#include <string>
#include <vector>
#include "lib/tbb_rand_hashtable.h"

namespace ycsbc {

typedef vmp::TbbRandHashtable<HashtableDB::FieldHashtable *> TbbRandKeyTable;
class TbbRandDB : public StaticHashtableDB<TbbRandKeyTable> {
 protected:
  HashtableDB::FieldHashtable *NewFieldHashtable() {
    return new vmp::TbbRandHashtable<const char *>;
  }

  void DeleteFieldHashtable(HashtableDB::FieldHashtable *table) {
    std::vector<FieldHashtable::KVPair> pairs = table->Entries();
    for (auto &pair : pairs) {
      DeleteString(pair.second);
    }
    delete table;
  }

  const char *CopyString(const std::string &str) {
    char *value = new char[str.length() + 1];
    strcpy(value, str.c_str());
    return value;
  }

  void DeleteString(const char *str) {
    delete[] str;
  }
};

} // ycsbc

#endif // YCSB_C_TBB_RAND_DB_H_
