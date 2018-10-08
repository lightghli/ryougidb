# RyougiDB
The homework for PingCAP 2019 campus recruitment.

# Problem
The configuration of a machine is CPU 8 cores, MEM 4G, HDD 4T. This machine has an unordered data file of 1T (key_size, key, value_size, value).

**Requirements:**
- Design an index structure to minimize the cost of reading each key-value randomly and concurrently.
- Data files are allowed to be arbitrarily preprocessed, but the preprocessing time is counted at the cost of the entire read process.

**Hints:**
- Note the readability of the code and add the necessary notes.
- Pay attention to code style and specification, add the necessary unit testing and documentation.
- Pay attention to exception handling and try to optimize performance.

# Solution
The main idea is to sharding the data according to key's hash value. That is to say, write key-value to the data file whose index equals to hash(key) % table_size. Every Get operation searchs the specified file to get the value or get nothing. Use LRUCache to accelerate Get operation if hot keys included.

Guide to files:
- **db/ryougidb.h:** Interface to RyougiDB.
- **db/ryougidb.cc:** Implement of RyougiDB.
- **db/ryougidb_test.cc:** Test Create, Get, Persist, Open, Close of RyougiDB and difference between using LRUCache frequently or not.

Building:
```
make
```

Test result:
- **System:** Windows 10 64-bit
- **Hardware:** Intel Core i5-6300HQ CPU @ 2.30 GHz, 8.0GB RAM
- **Parameters:** table_size(32768), buffer_size(32768), cache_size(32768)
- **Create database time:** 1,000,000 random key-value pairs, 150,385 ms
- **Get without LRUCache time:** 1,000 random Get operations, 112 ms
- **Get with LRUCache time:** 1,000 random Get operations, 80 ms