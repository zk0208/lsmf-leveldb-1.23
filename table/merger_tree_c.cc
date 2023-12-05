// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger_tree.h"
#include "db/dbformat.h"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <sys/types.h>
#include <thread>
#include <vector>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/slice.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
class MergingTreeIterator : public Iterator {
 public:
  MergingTreeIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        // child_sign_(new std::string[n]),
        // levelnum_(new uint64_t[config::kNumLevels + 1]()),
        // numlog_(0),
        T_env(leveldb::Env::Default()),
        itertime_(iterTime()),
        n_(n),
        current_(nullptr),
        direction_(kForward),
        th_n_(config::kNumSingleTrees),
        done_(true),
        work_(0),
        child_idx_(n),
        optype_(opnone),
        tar_("") {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
   // THWork();
  }

  ~MergingTreeIterator() override 
  {
    // printstats();
    // Log(iterlog_, "num: %ld %ld %ld %ld %ld %ld %ld %ld", 
    //     levelnum_[0],levelnum_[1],levelnum_[2],levelnum_[3],
    //     levelnum_[4],levelnum_[5],levelnum_[6],levelnum_[7]);
    // delete [] child_sign_;
    // delete [] levelnum_; 
    // done_ = false;
    // if (!threadall_.empty()) {
    //   for (auto & t : threadall_) {
    //     if(t!=nullptr && t->joinable())
    //       t->join();
    //     delete t;
    //   }
    // } 
    delete[] children_; 
  }

  bool Valid() const override { return (current_ != nullptr); }

  void SeekToFirst() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    FindSmallest();
    direction_ = kForward;
  }

  void SeekToLast() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    FindLargest();
    direction_ = kReverse;
  }

  void Seek(const Slice& target) override {
    double t1,t2,t3;
    t1 = T_env->NowMicros();
    // uint work_res = (1 << th_n_) - 1;
    // optype_ = seek;
    tar_ = target;

    for (int i = 0; i < n_; i++) {
      children_[i].Seek(tar_);
      // threadall_.emplace_back(new std::thread( [&,i](){
      //   children_[i].Seek(target);
      // }));
    }
    // for (auto &t: threadall_) {
    //   t->join();
    // }
    // children_[0].Seek(tar_);
    // child_idx_ = 0;
    // {
    //   std::unique_lock<std::mutex> lk(th_mu_);
    //   work_.store(0);
    //   cv.notify_all();
    //   cv.wait(lk, [&]{return work_.load() == work_res;});
    // }

    t2 = T_env ->NowMicros();
    FindSmallest();
    t3 = T_env->NowMicros();
    ++itertime_.seek_num_;
    itertime_.seek_ += t2 - t1;
    ++itertime_.merge_num_;
    itertime_.merge_tree_ += t3 - t2;
    direction_ = kForward;
  }

  void Next() override {
    assert(Valid());
    double t1,t2,t3;
    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    t1 = T_env->NowMicros();

    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }
    
    current_->Next();
    t2 = T_env->NowMicros();
    FindSmallest();
    t3 = T_env->NowMicros();
    ++itertime_.next_num_;
    itertime_.next_ += t2 - t1;
    ++itertime_.merge_num_;
    itertime_.merge_tree_ += t3 - t2;
  }

  void Prev() override {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    FindLargest();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  Status status() const override {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

  //统计iter时间占比
  leveldb::Env* T_env = nullptr;
  struct iterTime
  {
    iterTime(): merge_tree_(0), next_(0),seek_(0),seek_num_(0),next_num_(0),merge_num_(0){};

    double merge_tree_; //树数目的merge对比
    double next_;
    double seek_;
    uint64_t seek_num_;
    uint64_t next_num_;
    uint64_t merge_num_;
  };

  void THWork();

 private:
  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };
  enum Op {opnone, seek, seekfirst, seeklast};

  void FindSmallest();
  void FindLargest();
  
  // 判断当前线程是否可以取child thread执行
  bool JudgeWorkNoEnd(int& idx){
    idx = child_idx_.fetch_add(1);
    // printf("%d\n",idx);
    return idx < n_;
  }
  // 表示某个线程work结束
  void SetBitOne(int id){
    uint nbit = 1 << id;
    work_ |= nbit;
  }
  void printstats();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  iterTime itertime_;
  int n_;
  IteratorWrapper* current_;
  Direction direction_;

  //线程相关
  int th_n_;  //线程数目
  bool done_; //线程结束的标志
  std::atomic_uint work_; //work结束的标志
  std::atomic_uint child_idx_;  //当前空闲线程可以选的child索引
  Op optype_;
  Slice tar_; //seek目标
  std::vector<std::thread*> threadall_;
  std::mutex th_mu_;
  // std::condition_variable child_block_;
  std::condition_variable cv;
};

void MergingTreeIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
      }
    }
  }
  current_ = smallest;
}

void MergingTreeIterator::FindLargest() {
  IteratorWrapper* largest = nullptr;
  for (int i = n_ - 1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}

void MergingTreeIterator::printstats() {
  std::string value;
  char buf[200];
  std::snprintf(buf, sizeof(buf),
                "                               iters_time\n"
                "seek_  next_ merge_(iter_tree) seek_num_ next_num_ merge_num_\n"
                "-------------------------------------------------------\n");
  value.append(buf);
  std::snprintf(buf, sizeof(buf), "%8.0f %8.0f %8.0f %10ld %10ld %10ld\n",
                itertime_.seek_,itertime_.next_,itertime_.merge_tree_,
                itertime_.seek_num_,itertime_.next_num_,itertime_.merge_num_);
  value.append(buf);
  std::fprintf(stdout, "\n%s\n", value.c_str());
}

void MergingTreeIterator::THWork(){
  if(threadall_.empty()){
    for (int i = 0; i < th_n_; i++) {
      threadall_.emplace_back(
        new std::thread(
        [&,i]() {
          while(done_) { //done_就是线程结束标志
            auto idx = child_idx_.fetch_add(1);
            if (idx < n_) {
              switch (optype_) {
                case seek:
                {
                  children_[idx].Seek(tar_);
                  break;
                }
                case seekfirst:
                {
                  children_[idx].SeekToFirst();
                  break;
                }
                case seeklast:
                {
                  children_[idx].SeekToLast();
                  break;
                }
                case opnone:
                default:
                  break;                
              }
            } else {
              SetBitOne(i);
              std::unique_lock<std::mutex> lk(th_mu_);
              cv.notify_one();
              cv.wait(lk, [&]{return  child_idx_.load() < n_;} );
            }

          }
        }
      ));
    }
  }
}
}  // namespace

Iterator* NewMergingTreeIterator(const Comparator* comparator, Iterator** children,
                             int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new MergingTreeIterator(comparator, children, n);
  }
}
}  // namespace leveldb
