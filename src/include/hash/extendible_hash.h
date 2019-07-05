/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <mutex>
#include <vector>
#include <string>

#include "hash/hash_table.h"

namespace cmudb {

template <typename K, typename V>
struct Bucket {
	int depth;
	int len;
	K *keys;
	V *values;
	Bucket(int depth, int max_size);
	void SafeAppend(const K &key, const V &value);
	~Bucket();
};
template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
public:
  // constructor
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;

private:
  void InsertInternal(const K &key, const V &value);
  void splitLocal(int bucket_id);
  void splitGlobal(int bucket_id);
  int BucketID(size_t hash) const;
  void Expand(int bucket_id);
  // add your own member variables here
  std::vector<Bucket<K,V>*> buckets;
  std::mutex mutex;
  int globalDepth;
  int maxSize;
};
} // namespace cmudb
