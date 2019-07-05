/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  std::lock_guard<std::mutex> guard(mutex);
  // make sure that value is unique in the list
  for (auto it=List.begin(); it!=List.end(); ++it) {
    if (*it == value) {
      it = List.erase(it);
      break;
    }
  }
  List.push_back(value);
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  std::lock_guard<std::mutex> guard(mutex);
  if (List.size() == 0)
    return false;
  value = List.front();
  List.pop_front();
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  std::lock_guard<std::mutex> guard(mutex);
  for (auto it=List.begin(); it!=List.end(); ++it) {
    if (*it == value) {
      List.erase(it);
      return true;
    }
  }
  return false;
}

template <typename T> size_t LRUReplacer<T>::Size() {
  std::lock_guard<std::mutex> guard(mutex);
  return List.size();
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
