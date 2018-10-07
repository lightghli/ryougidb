#ifndef RYOUGIDB_H_
#define RYOUGIDB_H_

#include <io.h>
#include <direct.h>

#include <memory>
#include <mutex>
#include <functional>
#include <string>
#include <iostream>
#include <fstream>
#include <list>
#include <vector>
#include <queue>
#include <unordered_map>

namespace ryougidb {

// Status handle errors
class Status {
 public:
  Status();
  
  bool IsOK() const;

  bool IsNotFound() const;
  
  bool IsCorruption() const;

  bool IsIOError() const;

  bool IsNotSupported() const;

  std::string ToString() const;
  
  std::string GetMessage() const;

  static Status OK();

  static Status NotFound(const std::string &msg);

  static Status Corruption(const std::string &msg);

  static Status IOError(const std::string &msg);

  static Status NotSupported(const std::string &msg);
 private:
  enum Code {
    kOK = 0,
    kNotFound = 1,
    kCorruption = 2,
    kIOError = 3,
    kNotSupported = 4,
  };

  Code code_;
  std::string msg_;
  
  Status(const Code &code, const std::string &msg);
};

// LRUCache accelerate RyougiDB::Get(const std::string &, const std::string &)
class LRUCache {
 public:
  LRUCache();
  
  Status Get(const std::string &key, std::string &value);

  Status Put(const std::string &key, const std::string &value);
 private:
  struct CacheNode {
    std::string key;
    std::string value;
  };

  static const uint32_t kDefaultCacheSize;

  uint32_t cache_size_;
  std::mutex mutex_;
  std::list<CacheNode> lru_;
  std::unordered_map<std::string, std::list<CacheNode>::iterator> table_;

  LRUCache(const LRUCache &cache);
  void operator=(const LRUCache &cache);
};

// RyougiDB implement readonly key-value storage
class RyougiDB {
 public:
  RyougiDB();

  ~RyougiDB();

  Status Open(const std::string &dbname);

  Status Create(const std::string &dbname,
                const std::string &pathname,
                uint32_t table_size = kDefaultTableSize,
                uint32_t buffer_size = kDefaultBufferSize);

  Status Get(const std::string &key, std::string &value);

  Status Persist();

  Status Close();
 private:
  struct RyougiDataItem {
    uint32_t key_size;
    std::string key;
    uint32_t value_size;
    std::string value;
  };

  static const std::string kTableDir;
  static const std::string kTableName;

  static const std::string kDataDir;
  static const std::string kDataName;

  static const uint32_t kDefaultTableSize;
  static const uint32_t kDefaultBufferSize;

  static const uint32_t kMaxKeySize;
  static const uint32_t kMaxValueSize;

  uint32_t table_size_;
  uint32_t buffer_size_;
  std::string dbname_;
  LRUCache cache_;

  Status Retrieve(const std::string &key, const uint32_t &file_id,
              std::string &value);

  Status Write(const uint32_t &file_id, const std::vector<RyougiDataItem> &data);

  uint32_t Hash(const std::string &str);

  static Status CreateDir(const std::string &pathname);

  static bool IsFileExist(const std::string &pathname);
};

} // namespace ryougidb

#endif // RYOUGIDB_H_