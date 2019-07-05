#include <assert.h>
#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

template <typename K, typename V>
Bucket<K,V>::Bucket(int depth, int max_size)
{
  this->depth = depth;
  this->len = 0;
  this->keys = new K[max_size];
  this->values = new V[max_size];
}
template <typename K, typename V>
Bucket<K, V>::~Bucket() {
  delete []keys;
  delete []values;
}
template <typename K, typename V>
void Bucket<K,V>::SafeAppend(const K &key, const V &value)
{
  int i = len++;
  keys[i] = key;
  values[i] = value;
}

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) {
  globalDepth = 0;
  maxSize = size;
  buckets.push_back(new Bucket<K, V>(0, maxSize));
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  return std::hash<K>()(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  return globalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  assert(bucket_id >= 0 && bucket_id < (int)buckets.size());
//  mutex.lock();
  auto dep = buckets[bucket_id]->depth;
//  mutex.unlock();
  return dep;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
//  std::lock_guard<std::mutex> guard(mutex);

  return buckets.size();
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  std::lock_guard<std::mutex> guard(mutex);

  int bucket_id = BucketID(HashKey(key));
  auto b = buckets[bucket_id];
  for(int i=0; i<b->len; i++) {
    if (key == b->keys[i]) {
      value = b->values[i];
      return true;
    }
  }
  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  std::lock_guard<std::mutex> guard(mutex);

  int bucket_id = BucketID(HashKey(key));
  int i;
  auto b = buckets[bucket_id];
  for (i=0; i<b->len; i++) {
    if (key == b->keys[i])
      break;
  }
  if (i == b->len)
    return false;

  if (i+1 != b->len) {
    // not the last one, just override by the last one
    b->keys[i] = b->keys[b->len-1];
    b->values[i] = b->values[b->len-1];
  }
  b->len--;
  return true;
}

template <typename K, typename V>
int ExtendibleHash<K, V>::BucketID(size_t hash) const {
//  const int nbits = sizeof(size_t) * 8;
  const size_t mask = ((((size_t)1)<<globalDepth)-1);
  return hash & mask;
}
/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> guard(mutex);
  InsertInternal(key, value);
}

template <typename K, typename V>
void ExtendibleHash<K, V>::InsertInternal(const K &key, const V &value) {
  int bucket_id = BucketID(HashKey(key));
  assert(bucket_id < (int)buckets.size());
  auto b = buckets[bucket_id];
  if (b->len < maxSize) {
    b->SafeAppend(key, value);
  } else {
    Expand(bucket_id);
    InsertInternal(key, value);
  }
}
template <typename K, typename V>
void ExtendibleHash<K, V>::splitLocal(int bucket_id) {
  auto b = buckets[bucket_id];
  int dep = b->depth + 1;
  auto x = new Bucket<K,V>(dep, maxSize);
  auto y = new Bucket<K,V>(dep, maxSize);
  size_t mask = (((size_t)1)<<b->depth);
  for (int i=0; i<b->len; i++) {
    size_t hash = HashKey(b->keys[i]);
    if (hash & mask)
      y->SafeAppend(b->keys[i], b->values[i]);
    else
      x->SafeAppend(b->keys[i], b->values[i]);
  }
  for (size_t index = bucket_id & (mask-1), N=buckets.size(); index<N; ) {
    buckets[index] = x; index += mask;
    buckets[index] = y; index += mask;
  }
  delete b;
}
template <typename K, typename V>
void ExtendibleHash<K,V>::splitGlobal(int bucket_id) {
  std::vector<Bucket<K,V>*> vv;
  const size_t N = buckets.size();
  vv.reserve(N*2);
  vv.resize(N*2);
  int j=0;
  int dep;
  for (size_t i=0; i<buckets.size(); i++, j++) {
    vv[j] = buckets[i];
    vv[j+N] = buckets[i];
  }
  // try split
  auto b = buckets[bucket_id];
  dep = b->depth+1;
  assert(dep <= 64);
  // x: zzzz0 y: zzzz1
  auto x = new Bucket<K,V>(dep, maxSize);
  auto y = new Bucket<K,V>(dep, maxSize);
  size_t mask = (((size_t)1) << (dep-1));
  for (j=0; j<b->len; j++) {
    size_t hash = HashKey(b->keys[j]);
    if (hash & mask)
      y->SafeAppend(b->keys[j], b->values[j]);
    else
      x->SafeAppend(b->keys[j], b->values[j]);
  }

  vv[bucket_id] = x;
  vv[bucket_id + N] = y;
  delete b;
  this->buckets = std::move(vv);
  globalDepth++;
}
// this function used internallly, guard by mutex already
template <typename K, typename V>
void ExtendibleHash<K, V>::Expand(int bucket_id) {
  // if expended bucket is the innerest, inc global depth,
  // otherwise split local buckets.
  auto bb = buckets[bucket_id];
  assert(bb->depth <= globalDepth);
  if (bb->depth < globalDepth) {
    splitLocal(bucket_id);
  } else{
    splitGlobal(bucket_id);
  }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
