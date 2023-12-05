// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"
#include <cstdint>
#include <map>
#include <string>

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
  enum level{mem,l0,l1,l2,l3,l4,l5,l6,l7};
  std::map<std::string, level> levelstrtonum{
    {"mem",mem},{"L0",l0},{"L1",l1},{"L2",l2},
    {"L3",l3},{"L4",l4},{"L5",l5},{"L6",l6}};

class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        child_sign_(new std::string[n]),
        numlog_(0),
        T_env(leveldb::Env::Default()),
        itertime_(iterTime()),
        n_(n),
        current_(nullptr),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }
  MergingIterator(const Comparator* comparator, Iterator** children, std::string* iterSign, 
                   int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        child_sign_(new std::string[n]),
        numlog_(0),
        T_env(leveldb::Env::Default()),
        itertime_(iterTime()),
        n_(n),
        current_(nullptr),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
      child_sign_[i] = iterSign[i];
    }
  }
  /*MergingIterator(const Comparator* comparator, Iterator** children, std::string* iterSign, 
                    const std::string& db_name, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        child_sign_(new std::string[n]),
        levelnum_(new uint64_t[config::kNumLevels + 1]()),
        numlog_(0),
        T_env(leveldb::Env::Default()),
        itertime_(iterTime()),
        n_(n),
        current_(nullptr),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
      child_sign_[i] = iterSign[i];
    }
    T_env->NewLogger(db_name+"/iterlog", &iterlog_);
  }*/

  ~MergingIterator() override { 
    //printstats();
    // Log(iterlog_, "num: %ld %ld %ld %ld %ld %ld %ld %ld", 
    //     levelnum_[0],levelnum_[1],levelnum_[2],levelnum_[3],
    //     levelnum_[4],levelnum_[5],levelnum_[6],levelnum_[7]);
    delete [] child_sign_;
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

  /*void Seek(const Slice& target) override {
    double t1,t2;
    // t1 = T_env->NowMicros();
    for (int i = 0; i < n_; i++) {
      t1 = T_env->NowMicros();
      children_[i].Seek(target);
      t2 = T_env->NowMicros();
      itertime_.calculate(child_sign_[i], t2-t1);
    }
    // t2 = T_env ->NowMicros();
    FindSmallest();
    // t3 = T_env->NowMicros();
    // ++itertime_.seek_num_;
    // itertime_.seek_ += t2 - t1;
    // ++itertime_.merge_num_;
    // itertime_.merge_ += t3 - t2;
    direction_ = kForward;
  }*/
  void Seek(const Slice& target) override {
    // double t1,t2,t3;
    // t1 = T_env->NowMicros();
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    // t2 = T_env ->NowMicros();
    FindSmallest();
    // t3 = T_env->NowMicros();
    // ++itertime_.seek_num_;
    // itertime_.seek_ += t2 - t1;
    // ++itertime_.merge_num_;
    // itertime_.merge_ += t3 - t2;
    direction_ = kForward;
  }

  void Next() override {
    assert(Valid());
    // double t1,t2,t3;
    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    // t1 = T_env->NowMicros();

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
    // t2 = T_env->NowMicros();
    FindSmallest();
    // t3 = T_env->NowMicros();
    // ++itertime_.next_num_;
    // itertime_.next_ += t2 - t1;
    // ++itertime_.merge_num_;
    // itertime_.merge_ += t3 - t2;
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
  // leveldb::Logger * iterlog_ = nullptr;

  struct iterTime
  {
    iterTime(): seek_(0), merge_(0),next_(0),seek_num_(0),next_num_(0),merge_num_(0){};
    double seek_; //level iters seek() time
    double merge_;  //iters findsmallest()
    double next_; 
    uint64_t seek_num_;
    uint64_t next_num_;
    uint64_t merge_num_;
    uint64_t levelnum_[8];
    double leveltime_[8];

    void calculate(const std::string & sign, double t){
      switch (levelstrtonum[sign]) 
      {
        case mem:
          ++levelnum_[0];
          leveltime_[0] += t;
          break;
        case l0:
          ++levelnum_[1];
          leveltime_[1] += t;
          break;
        case l1:
          ++levelnum_[2];
          leveltime_[2] += t;
          break;
        case l2:
          ++levelnum_[3];
          leveltime_[3] += t;
          break;
        case l3:
          ++levelnum_[4];
          leveltime_[4] += t;
          break;
        case l4:
          ++levelnum_[5];
          leveltime_[5] += t;
          break;
        case l5:
          ++levelnum_[6];
          leveltime_[6] += t;
          break;
        case l6:
          ++levelnum_[7];
          leveltime_[7] += t;
          break;
        default:
          break;
      }
    }
  };

 private:
  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };

  void FindSmallest();
  void FindLargest();
  // void calnum(int level);
  void printstats();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  std::string * child_sign_;
  //uint64_t levelnum_[8];  //每一层查找数目
  uint64_t numlog_;
  iterTime itertime_;
  int n_;
  IteratorWrapper* current_;
  Direction direction_;
};

void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  // int select = -1;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
        // select = i;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
        // select = i;
      }
    }
  }
  // calnum(select);  
  current_ = smallest;
}

void MergingIterator::FindLargest() {
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

void MergingIterator::printstats() {
  std::string value;
  char buf[200];
  // std::snprintf(buf, sizeof(buf),
  //               "                  seek iters time\n"
  //               "MEM   L0  L1  L2  L3  L4  L5  L6 \n"
  //               "---------------------------------\n");
  // value.append(buf);
  // std::snprintf(buf, sizeof(buf), "%8.0f %8.0f %8.0f %8.0f %8.0f %8.0f %8.0f %8.0f\n",
  //               itertime_.leveltime_[0],itertime_.leveltime_[1],itertime_.leveltime_[2],
  //               itertime_.leveltime_[3],itertime_.leveltime_[4],itertime_.leveltime_[5],
  //               itertime_.leveltime_[6],itertime_.leveltime_[7]);
  // value.append(buf);
  // std::snprintf(buf, sizeof(buf),
  //               "                  seek iters num \n"
  //               "MEM   L0  L1  L2  L3  L4  L5  L6 \n"
  //               "---------------------------------\n");
  // value.append(buf);
  // std::snprintf(buf, sizeof(buf), "%10ld %10ld %10ld %10ld %10ld %10ld %10ld %10ld \n",
  //               itertime_.levelnum_[0],itertime_.levelnum_[1],itertime_.levelnum_[2],
  //               itertime_.levelnum_[3],itertime_.levelnum_[4],itertime_.levelnum_[5],
  //               itertime_.levelnum_[6],itertime_.levelnum_[7]);
  // value.append(buf);
  std::snprintf(buf, sizeof(buf),
                "                               iters_time\n"
                "Seek  next  merge(iter_level) seek_num next_num merge_num\n"
                "----------------------------------------------------------------\n");
  value.append(buf);
  std::snprintf(buf, sizeof(buf), "%8.0f %8.0f %8.0f %10ld %10ld %10ld\n",
                itertime_.seek_,itertime_.next_,itertime_.merge_,
                itertime_.seek_num_,itertime_.next_num_,itertime_.merge_num_);
  value.append(buf);
  std::fprintf(stdout, "\n%s\n", value.c_str());
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                             int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new MergingIterator(comparator, children, n);
  }
}

Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children, std::string* iterSign,
                             int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new MergingIterator(comparator, children, iterSign, n);
  }
}

}  // namespace leveldb
