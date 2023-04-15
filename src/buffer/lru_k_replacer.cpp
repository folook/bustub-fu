//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"


namespace bustub {


LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

// 优先从 hist_list_ 中踢，如果没有则从 cache_list_ 中踢。因为插入时将元素放在了队头，所以这里要从队尾踢。
//要按顺序检查，如果有护身符 evictable_ 为 true，则不准踢，继续检查下一个
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  bool evict_found = false;
  if(!his_list_.empty()) {
    for(auto rit = his_list_.rbegin(); rit != his_list_.rend(); ++rit) {
      if(entries_[*rit].evictable_) {
        *frame_id = *rit;
        his_list_.erase(std::next(rit).base());
        evict_found = true;
        break;
      }
    }
  }

  if(!cache_list_.empty() && !evict_found) {
    for(auto rit = cache_list_.rbegin(); rit != cache_list_.rend(); ++rit) {
      if(entries_[*rit].evictable_) {
        *frame_id = *rit;
        cache_list_.erase(std::next(rit).base());
        evict_found = true;
        break;
      }
    }
  }
//page 在缓冲池的 his 或者 cache 队列，如果踢出去，应该在 entries 数组中也踢出去
  if(evict_found) {
    entries_.erase(*frame_id);
    curr_size_--;
    return true;
  }
  return false;
}

//访问一个页面，根据访问次数是否到达 k，增删 his_list和 cache_list
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if(frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument(std::string("RecordAccess: invalid frame id") + std::to_string(frame_id));
  }

  size_t new_count = ++entries_[frame_id].hit_count_;
  if(new_count == 1) {
    //new frame,add to his_list
    ++curr_size_;
    his_list_.emplace_front(frame_id);
    entries_[frame_id].pos_ = his_list_.begin();
  } else {
    if(new_count == k_) {// move from hist list to the front of cache list
      his_list_.erase(entries_[frame_id].pos_);
      cache_list_.emplace_front(frame_id);
      entries_[frame_id].pos_ = cache_list_.begin();
    }
    if(new_count > k_) {//move to the front of cache_list
      cache_list_.erase(entries_[frame_id].pos_);
      cache_list_.emplace_front(frame_id);
      entries_[frame_id].pos_ = cache_list_.begin();

    }
  }


}

//直接设置一个 page 的可驱逐 flag，影响 curr_size_
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);

  if(frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument(std::string("SetEvictable: invalid frame id") + std::to_string(frame_id));
  }
  //not found the frame_id
  if(entries_.find(frame_id)== entries_.end()) {
    return;
  }

  if(set_evictable && !entries_[frame_id].evictable_) {
    curr_size_++;
    entries_[frame_id].evictable_ = set_evictable;
  }
  if(!set_evictable && entries_[frame_id].evictable_) {
    curr_size_--;
    entries_[frame_id].evictable_ = set_evictable;
  }

}

//直接从两个队列中移除
void LRUKReplacer::Remove(frame_id_t frame_id) {
  if(entries_.find(frame_id) == entries_.end()) {
    return ;
  }
  if(!entries_[frame_id].evictable_) {
    throw std::logic_error(std::string("can't remove an inevitable frame") = std::to_string((frame_id)));
  }

  if(entries_[frame_id].hit_count_ < k_) {
    his_list_.erase(entries_[frame_id].pos_);
  }

  if(entries_[frame_id].hit_count_ == k_) {
    cache_list_.erase(entries_[frame_id].pos_);
  }
  curr_size_--;
  entries_.erase(frame_id);
}

//返回缓冲池中的可驱逐页面的数量
auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
