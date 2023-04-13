//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  //UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  //1. 找到 key 所在的桶
  size_t index = this->IndexOf(key);
  auto bucket_ptr = dir_[index];
  //2. 遍历桶中所有元素，找到 key 对应的 value
  bool ret = bucket_ptr->Find(key, value);
  return ret;

}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  //UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  //1. 找到 key 所在的桶
  size_t index = this->IndexOf(key);
  auto bucket_ptr = dir_[index];
  //2. 遍历桶中所有元素，删除 kv
  bool ret = bucket_ptr->Remove(key);
  return ret;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  //UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  while (true) {
    //1. 找到 key 所在的桶，尝试插入
    size_t index = this->IndexOf(key);
    bool insert_flag = dir_[index]->Insert(key, value);
    if(insert_flag) {
      break;
    }
    if(this->GetLocalDepth(index) < this->GetGlobalDepth()) {
      //桶分裂.local_depth++
      RedistributeBucket(dir_[index]);
    } else {
      //目录项扩容一倍,global_depth++
      this->global_depth_++;
      //扩容的新指针依此重复指向前 n 个 bucket
      for(size_t i = 0; i < dir_.size(); i++) {
        dir_.emplace_back(dir_[i]);
      }
    }
  }

}


template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  bucket->IncrementDepth(); // 局部深度增加 1
  int depth = bucket->GetDepth();
  num_buckets_++; // bucket 的数量增加 1 个
  std::shared_ptr<Bucket> p(new Bucket(bucket_size_, depth)); // 创建新的 bucket
  // 原来的哈希值，比如说是 001
  size_t preidx = std::hash<K>()((*(bucket->GetItems().begin())).first) & ((1 << (depth - 1)) - 1);
  for (auto it = bucket->GetItems().begin(); it != bucket->GetItems().end();) {
    // 现在的哈希值，因为深度加 1 了，多一比特，那么可以是 1001 或者 0001
    size_t idx = std::hash<K>()((*it).first) & ((1 << depth) - 1);
    // 如果是 1001 则从原桶中删除，并添加到新的 bucket p 中
    if (idx != preidx) {
      p->Insert((*it).first, (*it).second);
      bucket->GetItems().erase(it++);
    } else {
      it++;
    }
  }
  for (size_t i = 0; i < dir_.size(); i++) {
    // 1001，低 3 位是 001，低 4 位不是 0001（1001），这些 dir 需要指向新的 bucket
    if ((i & ((1 << (depth - 1)) - 1)) == preidx && (i & ((1 << depth) - 1)) != preidx) {
      dir_[i] = p;
    }
  }
}


//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  //UNREACHABLE("not implemented");
  for(auto it = list_.begin(); it != list_.end(); it++) {
    if((*it).first == key) {
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  //UNREACHABLE("not implemented");
  for(auto it = list_.begin(); it != list_.end();) {
    if((*it).first == key) {
      list_.erase(it);
      return true;
    }
    //删除之常鸥会自动++，所以只在这里写++
    it++;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  //UNREACHABLE("not implemented");
  for(auto it = list_.begin(); it != list_.end(); it++) {
    if((*it).first == key) {
      (*it).second = value;
      return true;
    }
  }
  if(this->IsFull()) {
    return false;
  }
  //构造对象
  list_.emplace_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
