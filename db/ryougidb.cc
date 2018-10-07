#include "ryougidb.h"

namespace ryougidb {

const std::string RyougiDB::kTableDir = "/table";
const std::string RyougiDB::kTableName = "table";

const std::string RyougiDB::kDataDir = "/data";
const std::string RyougiDB::kDataName = "data";

// Default in-memory hash table size
const uint32_t RyougiDB::kDefaultTableSize = 1048576;

// Default buffer for writing data
const uint32_t RyougiDB::kDefaultBufferSize = 4096;

const uint32_t RyougiDB::kMaxKeySize = 32;
const uint32_t RyougiDB::kMaxValueSize = 256;

RyougiDB::RyougiDB(): table_size_(kDefaultTableSize),
                      buffer_size_(kDefaultBufferSize) {}

RyougiDB::~RyougiDB() {
  Close();
}

// Open the database named dbname which has been persisted
Status RyougiDB::Open(const std::string &dbname) {
  Status status;
  dbname_ = dbname;
  std::ifstream byte_stream(dbname_ + kTableDir + "/" + kTableName,
                            std::ios::in | std::ios::binary);

  if (!byte_stream.good()) {
    return status.IOError("Database open error, dbname=" + dbname_);
  }

  if (!byte_stream.read(reinterpret_cast<char *>(&table_size_),
                        sizeof(uint32_t))) {
    return status.IOError("Database read table_size error, dbname=" + dbname);
  }
  
  if (!byte_stream.read(reinterpret_cast<char *>(&buffer_size_),
                        sizeof(uint32_t))) {
    return status.IOError("Database read buffer_size error, dbname=" + dbname);
  }

  return status;
}

// Create the database named dbname,
// use the data of pathname with table_size and buffer_size
Status RyougiDB::Create(const std::string &dbname,
                        const std::string &pathname,
                        uint32_t table_size,
                        uint32_t buffer_size) {
  Status status;
  dbname_ = dbname;
  table_size_ = table_size;
  buffer_size_ = buffer_size;
  std::ifstream byte_stream(pathname, std::ios::in | std::ios::binary);

  if (!byte_stream.good()) {
    return status.IOError("Data file read error, pathname=" + pathname);
  }

  if (!IsFileExist(dbname_)) {
    status = CreateDir(dbname_);
    if (!status.IsOK()) {
      return status;
    }
  }

  if (!IsFileExist(dbname_ + kDataDir)) {
    status = CreateDir(dbname_ + kDataDir);
    if (!status.IsOK()) {
      return status;
    }
  }

  uint32_t key_size = 0, value_size = 0;
  std::unique_ptr<char[]> ukey(new char[kMaxKeySize]);
  std::unique_ptr<char[]> uvalue(new char[kMaxValueSize]);
  char *key = ukey.get();
  char *value = uvalue.get();

  // The space in buffer of files
  std::vector<uint32_t> space(table_size_, 0);
  
  // The items in buffer of files
  std::vector<std::vector<RyougiDataItem>> table(table_size_,
                                            std::vector<RyougiDataItem>{});

  while (byte_stream.read(reinterpret_cast<char *>(&key_size),
                          sizeof(uint32_t))) {
    if (key_size >= kMaxKeySize) {
      return status.NotSupported("Key size is overflow");
    }

    if (!byte_stream.read(key, key_size)) {
      return status.IOError("Data file read key error, pathname="
                            + pathname);
    }

    if (!byte_stream.read(reinterpret_cast<char *>(&value_size),
                          sizeof(uint32_t))) {
      return status.IOError("Data file read value_size error, pathname="
                            + pathname);
    }

    if (value_size >= kMaxValueSize) {
      return status.NotSupported("Value size is overflow");
    }

    if (!byte_stream.read(value, value_size)) {
      return status.IOError("Data file read value error, pathname="
                            + pathname);
    }

    key[key_size] = '\0';
    value[value_size] = '\0';

    std::string read_key(key);
    std::string read_value(value);
    
    uint32_t file_id = Hash(read_key) % table_size_;
    uint32_t item_size = sizeof(uint32_t) + sizeof(uint32_t) +
                        key_size * sizeof(char) +
                        value_size * sizeof(char);

    if (space[file_id] + item_size <= buffer_size_) {
      table[file_id].push_back(RyougiDataItem{key_size, read_key,
                                     value_size, read_value});
      space[file_id] += item_size;
    } else {
      status = Write(file_id, table[file_id]);
      space[file_id] = 0;
      table[file_id].clear();
    }
  }

  for (uint32_t file_id = 0; file_id < table_size_; file_id++) {
    if (table[file_id].size() > 0) {
      status = Write(file_id, table[file_id]);

      if (!status.IsOK()) {
        return status;
      }

      space[file_id] = 0;
      table[file_id].clear();
    }
  }

  return status;
}

// Get the value through the key from the database
Status RyougiDB::Get(const std::string &key, std::string &value) {
  Status status;

  status = cache_.Get(key, value);

  if (status.IsOK()) {
    return status;
  }

  uint32_t file_id = Hash(key) % table_size_;

  status = Retrieve(key, file_id, value);

  if (status.IsOK()) {
    cache_.Put(key, value);
  }

  return status;
}

// Persist the database with some parameters
Status RyougiDB::Persist() {
  Status status;
  
  if (!IsFileExist(dbname_)) {
    status = CreateDir(dbname_);
    if (!status.IsOK()) {
      return status;
    }
  }

  if (!IsFileExist(dbname_ + kTableDir)) {
    status = CreateDir(dbname_ + kTableDir);
    if (!status.IsOK()) {
      return status;
    }
  }
  
  std::string pathname = dbname_ + kTableDir + "/" + kTableName;
  std::ofstream byte_stream(pathname, std::ios::out | std::ios::binary);

  if (!byte_stream.good()) {
    return status.IOError("Persist error, pathname=" + pathname);
  }

  if (!byte_stream.write(reinterpret_cast<const char *>(&table_size_),
                           sizeof(uint32_t))) {
    return status.IOError("Persist error, pathname=" + pathname);
  }
  
  if (!byte_stream.write(reinterpret_cast<const char *>(&buffer_size_),
                           sizeof(uint32_t))) {
    return status.IOError("Persist error, pathname=" + pathname);
  }

  return status;
}

// Retrieve the value through the key from the file
Status RyougiDB::Retrieve(const std::string &key, const uint32_t &file_id,
                      std::string &value) {
  Status status;
  std::string pathname = dbname_ + kDataDir + "/" + kDataName +
                         std::to_string(file_id);

  std::ifstream byte_stream(pathname, std::ios::in | std::ios::binary);

  if (!byte_stream.good()) {
    return status.IOError("Database open error, pathname=" + pathname);
  }

  uint32_t key_size = 0, value_size = 0;
  std::unique_ptr<char[]> ukey(new char[kMaxKeySize]);
  std::unique_ptr<char[]> uvalue(new char[kMaxValueSize]);
  char *current_key = ukey.get();
  char *current_value = uvalue.get();

  while (byte_stream.read(reinterpret_cast<char *>(&key_size),
                          sizeof(uint32_t))) {
    if (key_size >= kMaxKeySize) {
      return status.Corruption("Key size is overflow");
    }
    
    if (!byte_stream.read(current_key, key_size)) {
      return status.IOError("Database read error, pathname=" + pathname);
    }

    if (!byte_stream.read(reinterpret_cast<char *>(&value_size),
                          sizeof(uint32_t))) {
      return status.IOError("Database read error, pathname=" + pathname);
    }
    
    if (value_size >= kMaxValueSize) {
      return status.Corruption("Value size is overflow");
    }

    if (!byte_stream.read(current_value, value_size)) {
      return status.IOError("Database read error, pathname=" + pathname);
    }

    current_key[key_size] = '\0';
    current_value[value_size] = '\0';

    std::string read_key(current_key);

    if (read_key == key) {
      value = std::string(current_value);
      return status.OK();
    }
  }

  return status.NotFound("Key not found, key=" + key);
}

// Write vector of key-value to the file
Status RyougiDB::Write(const uint32_t &file_id,
                       const std::vector<RyougiDataItem> &data) {
  Status status;
  std::string pathname = dbname_ + kDataDir + "/" + kDataName +
                         std::to_string(file_id);
  std::ofstream byte_stream(pathname, std::ios::app | std::ios::binary);

  if (!byte_stream.good()) {
    return status.IOError("Write data error, pathname=" + pathname);
  }

  for (const auto &item : data) {
    if (!byte_stream.write(reinterpret_cast<const char *>(&item.key_size),
                           sizeof(uint32_t))) {
      return status.IOError("Write data error, pathname=" + pathname);
    }
    
    if (!byte_stream.write(item.key.c_str(), sizeof(char) * item.key_size)) {
      return status.IOError("Write data error, pathname=" + pathname);
    }
    
    if (!byte_stream.write(reinterpret_cast<const char *>(&item.value_size),
                           sizeof(uint32_t))) {
      return status.IOError("Write data error, pathname=" + pathname);
    }
    
    if (!byte_stream.write(item.value.c_str(),
                           sizeof(char) * item.value_size)) {
      return status.IOError("Write data error, pathname=" + pathname);
    }
  }

  return status;
}

Status RyougiDB::Close() {
  Status status;
  return status;
}

// Hash returns the BKDRHash of str
uint32_t RyougiDB::Hash(const std::string &str) {
  uint32_t hash = 0;
  for (const auto &ch : str) {
    hash = hash * 131 + ch;
  }
  return hash;
}

// CreateDir creates the pathname
Status RyougiDB::CreateDir(const std::string &pathname) {
  Status status;
  if (mkdir(pathname.c_str()) != 0) {
    return status.IOError("Create directory error, pathname=" + pathname);
  }
  return status;
}

// IsFileExist returns true if the pathname exists,
// false if the pathname doesn't exist
bool RyougiDB::IsFileExist(const std::string &pathname) {
  return access(pathname.c_str(), 0) == 0;
}

Status::Status(): code_(kOK) {}

bool Status::IsOK() const {
  return code_ == kOK;
}

bool Status::IsNotFound() const {
  return code_ == kNotFound;
}

bool Status::IsCorruption() const {
  return code_ == kCorruption;
}

bool Status::IsIOError() const {
  return code_ == kIOError;
}

bool Status::IsNotSupported() const {
  return code_ == kNotSupported;
}

// ToString returns status code type and message
std::string Status::ToString() const {
  std::string type;
  switch (code_) {
    case kOK: {
      type = "OK";
      break;
    }
    case kNotFound: {
      type = "Not found: ";
      break;
    }
    case kIOError: {
      type = "IO error: ";
      break;
    }
    case kNotSupported: {
      type = "Not supported: ";
      break;
    }
    default: {
      type = "Unknown code(" + std::to_string(code_) + "): ";
      break;
    }
  }
  return type + msg_;
}

std::string Status::GetMessage() const {
  return msg_;
}

Status Status::OK() {
  return Status();
}

Status Status::NotFound(const std::string &msg) {
  return Status(kNotFound, msg);
}

Status Status::Corruption(const std::string &msg) {
  return Status(kCorruption, msg);
}

Status Status::IOError(const std::string &msg) {
  return Status(kIOError, msg);
}

Status Status::NotSupported(const std::string &msg) {
  return Status(kNotSupported, msg);
}

Status::Status(const Code &code, const std::string &msg):
  code_(code), msg_(msg) {}

const uint32_t LRUCache::kDefaultCacheSize = 4096;

LRUCache::LRUCache(): cache_size_(kDefaultCacheSize) {}

// Get value through key from LRUCache and it's thread-safe
Status LRUCache::Get(const std::string &key, std::string &value) {
  Status status;
  
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (table_.find(key) == table_.end()) {
    return status.NotFound("LRUCache not found, key=" + key);
  }

  lru_.splice(lru_.begin(), lru_, table_[key]);
  table_[key] = lru_.begin();

  value = table_[key]->value;

  return status;
}

// Put key-value in LRUCache and it's thread-safe
Status LRUCache::Put(const std::string &key, const std::string &value) {
  Status status;
  
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (table_.find(key) == table_.end()) {
    if (lru_.size() == cache_size_) {
      table_.erase(lru_.back().key);
      lru_.pop_back();
    }
    lru_.push_front(CacheNode{key, value});
    table_[key] = lru_.begin();
  } else {
    table_[key]->value = value;
    lru_.splice(lru_.begin(), lru_, table_[key]);
    table_[key] = lru_.begin();
  }

  return status;
}

} // namespace ryougidb