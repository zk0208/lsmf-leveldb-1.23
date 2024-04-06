// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
///** change record
// record :=
//    kTypeValue varstring varstring    =>    kTypeValue varstring fixed8(key's hash value) varstring 
//    kTypeDeletion varstring           =>    kTypeDeletion fixed8(key's hash value) varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include <cstdint>
#include "leveldb/db.h"
#include "util/coding.h"
#include "util/hash.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() { Clear(); }

WriteBatch::~WriteBatch() = default;

WriteBatch::Handler::~Handler() = default;

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

size_t WriteBatch::ApproximateSize() const { return rep_.size(); }


Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  uint8_t key_hash;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

///** change writebacth iterate to fit our hash inplementation
Status WriteBatch::Iterate(Handler* handler, bool hash) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  uint8_t key_hash;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) && GetKeyHashValue(&input, &key_hash) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, key_hash, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key) && GetKeyHashValue(&input, &key_hash)) {
          handler->Delete(key, key_hash);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

Status WriteBatch::GetKey(const int& index, Slice* key, uint8_t* key_hash, Slice* value, ValueType* type){
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }
  if (index >= WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch access violation");
  }
  input.remove_prefix(kHeader);
  int found = 0;
  while (!input.empty() && found <= index) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, key) && GetKeyHashValue(&input, key_hash) &&
            GetLengthPrefixedSlice(&input, value)) {
              *type = kTypeValue;
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, key) && GetKeyHashValue(&input, key_hash)) {
          *type = kTypeDeletion;
          value = nullptr;
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  return Status::OK();
}
Status WriteBatch::GetKey2(std::map<uint8_t, WriteBatch> &batchs){
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  uint8_t key_hash;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) && GetKeyHashValue(&input, &key_hash) &&
            GetLengthPrefixedSlice(&input, &value)) {
          uint8_t Tree_id = dynamicConfig::calculateTreeNum(key_hash);
          batchs[Tree_id].Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key) && GetKeyHashValue(&input, &key_hash)) {
          uint8_t Tree_id = dynamicConfig::calculateTreeNum(key_hash);
          batchs[Tree_id].Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  uint8_t key_hash = GetSingleTreeID(key.data(), key.size()) % 256;
  PutFixed8(&rep_, key_hash);
  PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
  uint8_t key_hash = GetSingleTreeID(key.data(), key.size()) % 256;
  PutFixed8(&rep_, key_hash);
}

void WriteBatch::Append(const WriteBatch& source) {
  WriteBatchInternal::Append(this, &source);
}

namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  MemTable* mem_;

  void Put(const Slice& key, const Slice& value) override {
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }
  void Delete(const Slice& key) override {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }

  void Put(const Slice& key, const uint8_t& key_hash, const Slice& value) {
    mem_->Add(sequence_, kTypeValue, key, key_hash, value);
    sequence_++;
  }
  void Delete(const Slice& key, const uint8_t& key_hash) {
    mem_->Add(sequence_, kTypeDeletion, key, key_hash, Slice());
    sequence_++;
  }
};
}  // namespace

Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter, true);   ///** change to fit our hash inplementation
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
