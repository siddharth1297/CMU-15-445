#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) : bucketSizeLimit(size), globalDepth(0) {
	bucketDirectory.push_back(std::make_shared<Bucket>(0));
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  return std::hash<K>{}(key);
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
  return bucketDirectory[bucket_id]->localDepth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  int nBuckets = 0;
  
  for(size_t i=0; i<bucketDirectory.size(); i++) {
    if((bucketDirectory[i]->localDepth == globalDepth) || (!((1 << bucketDirectory[i]->localDepth) & i))) {
      nBuckets++;
    }
  }
  return nBuckets;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  std::lock_guard<std::mutex> guard(mtx);
  std::shared_ptr<Bucket> targetBucket = getBucket(key);
  if(targetBucket->items.find(key) == targetBucket->items.end()) {
    return false;
  }
  value = targetBucket->items[key];
  return true;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  std::lock_guard<std::mutex> guard(mtx);
  std::shared_ptr<Bucket> targetBucket = getBucket(key);
  if(targetBucket->items.find(key) == targetBucket->items.end()) {
    return false;
  }
  targetBucket->items.erase(key);
  return true;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
	std::lock_guard<std::mutex> guard(mtx);

  std::shared_ptr<Bucket> targetBucket = getBucket(key);

  while(targetBucket->items.size() == bucketSizeLimit) {
    if(targetBucket->localDepth == globalDepth) {
      // double bucket address directory
      size_t length = bucketDirectory.size();
      for(size_t i=0; i<length; i++) {
        bucketDirectory.push_back(bucketDirectory[i]);
      }
      globalDepth++;
    }

    int mask = 1 << targetBucket->localDepth;
    targetBucket->localDepth++;
    
    // create new bucket
    auto newBucket = std::make_shared<Bucket>(targetBucket->localDepth);
    // insert new bucket to the bucket address table
    for(size_t i=0; i<bucketDirectory.size(); i++) {
      if((bucketDirectory[i] == targetBucket) && (mask & i)) {
        bucketDirectory[i] = newBucket;
      }
    }

    // rehash
    for(auto item : targetBucket->items) {
      std::shared_ptr<Bucket> target = getBucket(item.first);
      if(target != targetBucket) {
        newBucket->items[item.first] = item.second;
        targetBucket->items.erase(item.first);
      }
    }

    targetBucket = getBucket(key);
  }
  // add key
  targetBucket->items[key] = value;
}

/*
 * get bucket for key
 */
template <typename K, typename V>
std::shared_ptr<typename ExtendibleHash<K, V>::Bucket> ExtendibleHash<K, V>::getBucket(const K &key) {
  // bits from right
  int index = HashKey(key) & ((1 << globalDepth) - 1);
  return bucketDirectory[index];
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
