#include <time.h>
#include <assert.h>

#include <utility>
#include <thread>
#include <random>

#include "ryougidb.h"

// Generate random key-value data and save in key_value_pairs
void GenerateData(std::vector<std::pair<std::string, std::string>> 
                  &key_value_pairs) {
  std::cout << "Start GenerateData()" << std::endl;

  uint32_t data_test_size = 1000;
  uint32_t max_key_size = 30;
  uint32_t max_value_size = 200;
  
  char letter[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  uint32_t len = strlen(letter);
  std::mt19937 gen;
  std::uniform_int_distribution<uint32_t> dis_key_size(1, max_key_size);
  std::uniform_int_distribution<uint32_t> dis_value_size(1, max_value_size);
  std::uniform_int_distribution<uint32_t> dis_letter(0, len - 1);

  for (uint32_t i = 0; i < data_test_size; i++) {
    uint32_t key_size = dis_key_size(gen);
    uint32_t value_size = dis_value_size(gen);
    std::string key;
    std::string value;
    for (uint32_t j = 0; j < key_size; j++) {
      key.push_back(letter[dis_letter(gen)]);
    }
    for (uint32_t j = 0; j < key_size; j++) {
      value.push_back(letter[dis_letter(gen)]);
    }
    key_value_pairs.push_back(make_pair(key, value));
  }

  std::ofstream byte_stream("data_test", std::ios::out | std::ios::binary);

  for (const auto &kv : key_value_pairs) {
    uint32_t key_size = kv.first.size();
    uint32_t value_size = kv.second.size();
    byte_stream.write(reinterpret_cast<const char *>(&key_size),
                      sizeof(uint32_t));
    byte_stream.write(kv.first.c_str(), sizeof(char) * key_size);
    byte_stream.write(reinterpret_cast<const char *>(&value_size),
                      sizeof(uint32_t));
    byte_stream.write(kv.second.c_str(), sizeof(char) * value_size);
  }

  std::cout << "Finish GenerateData()" << std::endl;
}

// Test Create, Get, Persist, Open, Close and
// difference between using LRUCache frequently or not
void RunAllTests(std::vector<std::pair<std::string, std::string>> 
                 &key_value_pairs) {
  std::cout << "Start RunAllTests()" << std::endl;

  ryougidb::RyougiDB db;
  ryougidb::Status status;
  
  status = db.Create("db_test", "data_test");
  assert(status.IsOK());

  uint32_t get_times = 10;
  clock_t start_time;
  clock_t end_time;
  std::mt19937 gen;
  std::uniform_int_distribution<uint32_t>
    dis_index(0, key_value_pairs.size() - 1);
  std::uniform_int_distribution<uint32_t> dis_lru(0, 5);
  std::unique_ptr<std::thread[]> th(new std::thread[get_times]);
  
  start_time = clock();
  for (uint32_t i = 0; i < get_times; i++) {
    th[i] = std::thread([&]() {
      std::string key;
      std::string value;
      key = key_value_pairs[dis_index(gen)].first;
      ryougidb::Status status = db.Get(key, value);
      if (!status.IsOK()) {
        std::cout << status.ToString() << std::endl;
      }
    });
  }
  for (uint32_t i = 0; i < get_times; i++) {
    th[i].join();
  }
  end_time = clock();
  std::cout << "Time without LRUCache: " << end_time - start_time << std::endl;

  std::string value;
  status = db.Get("PingCAP", value);
  assert(!status.IsOK());

  status = db.Persist();
  assert(status.IsOK());

  status = db.Close();
  assert(status.IsOK());

  status = db.Open("db_test");
  assert(status.IsOK());

  start_time = clock();
  for (uint32_t i = 0; i < get_times; i++) {
    th[i] = std::thread([&]() {
      std::string key;
      std::string value;
      key = key_value_pairs[dis_lru(gen)].first;
      ryougidb::Status status = db.Get(key, value);
      if (!status.IsOK()) {
        std::cout << status.ToString() << std::endl;
      }
    });
  }
  for (uint32_t i = 0; i < get_times; i++) {
    th[i].join();
  }
  end_time = clock();
  std::cout << "Time with LRUCache: " << end_time - start_time << std::endl;

  std::cout << "Finish RunAllTests()" << std::endl;
}

int main() {
  std::vector<std::pair<std::string, std::string>> key_value_pairs;
  GenerateData(key_value_pairs);
  RunAllTests(key_value_pairs);
  return 0;
}