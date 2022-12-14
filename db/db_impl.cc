// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

/****以下是singleTree的实现****/
// Information kept for every waiting writer
struct SingleTree::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};
SingleTree::SingleTree(DBImpl* db, uint32_t id)
    : db_(db),
      id_(id),
      internal_comparator_(db->internal_comparator_),
      internal_filter_policy_(db->internal_filter_policy_),
      background_compaction_scheduled_(false),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      seed_(0),
      tmp_batch_(new WriteBatch),
      manual_compaction_(nullptr),
      versions_(new VersionSet(db_->dbname_, &db_->options_, db_->table_cache_,
                               &db_->internal_comparator_, id_)) {}

SingleTree::~SingleTree() {
  // Wait for background work to finish.
  mutex_.Lock();
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  mutex_.Unlock();

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
}
struct SingleTree::CompactionState {
  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;
};

//**** SingleTree read/write interface
Status SingleTree::Get(ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      options.read_dir = db_->dbname_ + "/vol" + std::to_string(id_ + 1);
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    //MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}
Status SingleTree::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  WriteTimeStats wtstats;
  const uint64_t start_micros = db_->env_->NowMicros();
  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(updates == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);
    //printf("batch count %d\n",WriteBatchInternal::Count(write_batch));

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      uint64_t log_start_micros = db_->env_->NowMicros();
      wtstats.other_micros += (log_start_micros - start_micros);
      db_->mutex_.Lock();
      status = db_->log_->AddRecord(WriteBatchInternal::Contents(write_batch));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = db_->logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      db_->mutex_.Unlock();
      uint64_t mem_start_micros = db_->env_->NowMicros();
      wtstats.log_micros += (mem_start_micros - log_start_micros);
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      wtstats.mem_micros += (db_->env_->NowMicros() - mem_start_micros);
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  wtstats.all_micros += (db_->env_->NowMicros() - start_micros);
  WTStats_.Add(wtstats);

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* SingleTree::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status SingleTree::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;  // 当有要写入的数据时，先判断要不要延迟写入，因为可能L0文件数太多
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      db_->env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= db_->options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(db_->options_.info_log, "SingleTree %u, Current memtable full; waiting...\n", id_);
      background_work_finished_signal_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(db_->options_.info_log, "SingleTree %u, Too many L0 files; waiting...\n", id_);
      background_work_finished_signal_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);  // 没有imm意味着之前的log文件已经是删除掉了
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = nullptr;
      s = db_->env_->NewWritableFile(LogFileName(db_->log_dir, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      db_->mutex_.Lock();  // log为多个Tree共用，可能是需要上锁的
      delete db_->log_;
      delete db_->logfile_;
      db_->logfile_ = lfile;
      db_->logfile_number_ = new_log_number;
      db_->log_ = new log::Writer(lfile);
      db_->mutex_.Unlock();
      imm_ = mem_;
      has_imm_.store(true, std::memory_order_release);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;  // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

// 暂时没有处理manual compaction
void SingleTree::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}
void SingleTree::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !db_->shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status SingleTree::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = db_->env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);   // 保证文件不会被误删
  Iterator* iter = mem->NewIterator();
  Log(db_->options_.info_log, "SingleTree %u, Level-0 table #%llu: started",
      id_, (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    std::string sub_filedir = db_->dbname_ + "/vol" + std::to_string(id_ + 1);
    s = BuildTable(sub_filedir, db_->env_, db_->options_, db_->table_cache_,
                   iter, &meta);
    //s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(db_->options_.info_log, "SingleTree %u, Level-0 table #%llu: %lld bytes %s",id_,
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }

  CompactionStats stats;
  stats.micros = db_->env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats.nums = 1;
  stats_[level].Add(stats);
  return s;
}

void SingleTree::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && db_->shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    // 遍历log链表，找到最旧的数下一个
    db_->mutex_.Lock();
    edit.SetOldestLogNumber(db_->logfile_number_);
    edit.SetLogNumber(db_->logfile_number_);  // Earlier logs no longer needed
    db_->mutex_.Unlock();
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);
    RemoveObsoleteFiles();
    db_->RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

Status SingleTree::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void SingleTree::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

void SingleTree::RemoveObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  // 保存所有singletree中仍然存活的文件
  std::set<uint64_t> live = pending_outputs_;
  //live.insert(pending_outputs_.begin(), pending_outputs_.end());
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  std::string sub_filedir = db_->dbname_ + "/vol" + std::to_string(id_ + 1);
  db_->env_->GetChildren(sub_filedir, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kLogFile:
        case kDescriptorFile:       
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          db_->table_cache_->Evict(number);
        }
        Log(db_->options_.info_log, "SingleTree %u, Delete type=%d #%lld\n",id_, static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  mutex_.Unlock();
  for (const std::string& filename : files_to_delete) {
    db_->env_->RemoveFile(sub_filedir + "/" + filename);
  }
  mutex_.Lock();
}

//**** 以下是compaction触发过程
void SingleTree::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (db_->options_.disable_compaction) {
    // 手动关闭compaction,为了测试read 和 scan
  } else if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (db_->shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    background_compaction_scheduled_ = true;
    db_->env_->Schedule(&SingleTree::BGWork, this, id_);
  }
}

void SingleTree::BGWork(void* db) {
  reinterpret_cast<SingleTree*>(db)->BackgroundCall();
}

void SingleTree::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (db_->shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

void SingleTree::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(db_->options_.info_log,
        "SingleTree %u, Manual compaction at level-%d from %s .. %s; will stop at %s\n",id_,
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->RemoveFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(db_->options_.info_log, "SingleTree %u, Moved #%lld to level-%d %lld bytes %s: %s\n",id_,
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    RemoveObsoleteFiles();
    db_->RemoveObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (db_->shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(db_->options_.info_log, "SingleTree %u, Compaction error: %s",id_, status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
}

void SingleTree::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status SingleTree::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string sub_filedir = db_->dbname_ + "/vol" + std::to_string(id_ + 1);
  std::string fname = TableFileName(sub_filedir, file_number);
  Status s = db_->env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(db_->options_, compact->outfile);
  }
  return s;
}

Status SingleTree::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    ReadOptions read_option;
    read_option.read_dir = db_->dbname_ + "/vol" + std::to_string(id_ + 1);
    Iterator* iter =
        db_->table_cache_->NewIterator(read_option, output_number, current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(db_->options_.info_log, "SingleTree %u, Generated table #%llu@%d: %lld keys, %lld bytes",id_,
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status SingleTree::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(db_->options_.info_log, "SingleTree %u, Compacted %d@%d + %d@%d files => %lld bytes",id_,
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status SingleTree::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = db_->env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(db_->options_.info_log, "SingleTree %u, Compacting %d@%d + %d@%d files",id_,
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  db_->mutex_.Lock();
  if (db_->snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = db_->snapshots_.oldest()->sequence_number();
  }
  db_->mutex_.Unlock();
  // 获取一个包含所有compaction输入文件的的迭代器
  Iterator* input = versions_->MakeInputIterator(compact->compaction);

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  while (input->Valid() && !db_->shutting_down_.load(std::memory_order_acquire)) {
    // Prioritize immutable compaction work
    if (has_imm_.load(std::memory_order_relaxed)) {
      const uint64_t imm_start = db_->env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (db_->env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
    // 检查当前key是否与level+2层文件有太多冲突，如果是的话需要完成当前文件输出新文件
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      // 没有待写入的新文件，则新建一个
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && db_->shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = db_->env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }
  stats.nums = 1;

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(db_->options_.info_log, "SingleTree %u, compacted to: %s",id_, versions_->LevelSummary(&tmp));
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* SingleTree::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  ReadOptions read_option = options;
  read_option.read_dir = db_->dbname_ + "/vol" + std::to_string(id_ + 1);
  versions_->current()->AddIterators(read_option, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

int64_t SingleTree::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

void SingleTree::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

bool SingleTree::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB) Compaction_Nums T_cache_all T_cache_miss\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f %10ld %10ld %10ld\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0,
                      stats_[level].nums,
                      db_->table_cache_->Get_find_num(0),
                      db_->table_cache_->Get_find_num(1));
        value->append(buf);
      }
    }
    return true;
  } else if (in == "writetime") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               write time\n"
                  "ALL_Time(micros) Log_Time(micros) Mem_Time(micros) Other_Time(micros)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    std::snprintf(buf, sizeof(buf), "%10ld %10ld %10ld %10ld\n",
                  WTStats_.all_micros, WTStats_.log_micros,
                  WTStats_.mem_micros, WTStats_.other_micros);
    value->append(buf);
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    //size_t total_usage = db_->options_.block_cache->TotalCharge();
    size_t total_usage = 0;
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void SingleTree::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

/****以下是DBImpl实现过程，以及DB虚基类的接口****/
// 目前的实现时WAL 暂时不处理，然后info_log、block cache、table
// cache保持整个DB一个 可配置参数仍在options.h中，不可改变配置在dbformat.h中

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      log_dir("") {}

DBImpl::~DBImpl() {
  // Wait for background work to finish.
  Log(options_.info_log, "CLOSE: maybe waiting singleTree");
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release);
  mutex_.Unlock();
  
  for (auto singletree : singleTrees_) {
    delete singletree;
  }
  

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete log_;
  delete logfile_;
  delete VersionSet::descriptor_file_;
  delete VersionSet::descriptor_log_;
  VersionSet::descriptor_file_ = nullptr;
  VersionSet::descriptor_log_ = nullptr;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  Log(options_.info_log, "create a new db and the manifest!");
  // 新建数据库与相应清单文件
  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
      VersionEdit new_Tree;
      new_Tree.SetComparatorName(user_comparator()->Name());
      new_Tree.SetLogNumber(0);
      new_Tree.SetOldestLogNumber(0);
      new_Tree.SetNextFile(2);
      new_Tree.SetLastSequence(0);
      new_Tree.setSingleTreeID(i);
      std::string record;
      new_Tree.EncodeTo(&record);
      s = log.AddRecord(record);
    }
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  uint32_t Tree_id =
      GetSingleTreeID(key.data(), key.size()) % config::kNumSingleTrees;
  ReadOptions readoptions = options;
  Status s = singleTrees_[Tree_id]->Get(readoptions, key, value);
  return s;
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
  // 先hash判断写入那个Tree
  uint32_t Tree_id =
      GetSingleTreeID(key.data(), key.size()) % config::kNumSingleTrees;
  WriteBatch batch;
  batch.Put(key, value);
  return singleTrees_[Tree_id]->Write(options, &batch);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  // 先hash判断写入那个Tree
  uint32_t Tree_id =
      GetSingleTreeID(key.data(), key.size()) % config::kNumSingleTrees;
  WriteBatch batch;
  batch.Delete(key);
  return singleTrees_[Tree_id]->Write(options, &batch);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  int batch_num = WriteBatchInternal::Count(updates);
  Status s;
  WriteBatch single_update[config::kNumSingleTrees];
  int num[config::kNumSingleTrees] = {0};
  s = updates->GetKey2(single_update, num);
  for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
    if (num[i] == 0) continue;
    s = singleTrees_[i]->Write(options, &single_update[i]);
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = singleTrees_[0]->versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  uint32_t seed_single = 0;
  for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
    Iterator* it = singleTrees_[i]->NewInternalIterator(options, latest_snapshot, &seed_single);
    *seed += seed_single;
    list.push_back(it);
  }

  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());

  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  ParsedInternalKey ikey;
  if (ParseInternalKey(key, &ikey)) {
    uint32_t Tree_id =
        GetSingleTreeID(ikey.user_key.data(), ikey.user_key.size()) %
        config::kNumSingleTrees;
    singleTrees_[Tree_id]->RecordReadSample(key);
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(singleTrees_[0]->versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  //MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
    value->append("SingleTree ");
    std::string str = "";
    value->append(std::to_string(i));
    value->append(" :\n");
    if (singleTrees_[i]->GetProperty(property, &str)) {
      value->append(str);
    } else {
      return false;
    }
  }
  return true;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    for (uint32_t j = 0; j < config::kNumSingleTrees; j++) {
      uint64_t size = 0;
      singleTrees_[j]->GetApproximateSizes(&range[i], 1, &size);
      sizes[i] += size;
    }
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
    singleTrees_[i]->CompactRange(begin, end);
  }
}

// 对指定的log恢复数据到mem，没有写盘时，log可以复用
Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
    env_->CreateDir(dbname_ + "/vol" + std::to_string(i + 1));
  }
  if (options_.depart_log) {
    env_->CreateDir(dbname_ + "/" + options_.log_dir);
    log_dir = dbname_ + "/" + options_.log_dir;
  } else {
    log_dir = dbname_;
  }
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }
  // 判断，current文件是否存在
  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }
  // 下面表示数据库已存在，读取manifest文件恢复每个tree的信息
  std::vector<VersionSet*> version_sets;
  singleTrees_.resize(config::kNumSingleTrees);
  for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
    singleTrees_[i] = new SingleTree(this, i);
    version_sets.push_back(singleTrees_[i]->versions_);
  }
  VersionSet::SetMutex(&mutex_);
  s = VersionSet::Recover(save_manifest, version_sets);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.

  // const uint64_t min_log = versions_->LogNumber();
  // const uint64_t prev_log = versions_->PrevLogNumber();
  uint64_t min_log = singleTrees_[0]->versions_->LogNumber();
  std::vector<uint64_t> prev_logs;
  std::set<uint64_t> expected;  // 由version记录的应该存在的文件
  for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
    prev_logs.push_back(singleTrees_[i]->versions_->PrevLogNumber());
    if (min_log > singleTrees_[i]->versions_->LogNumber())
      min_log = singleTrees_[i]->versions_->LogNumber();
    if (min_log > singleTrees_[i]->versions_->OldestLogNumber())
      min_log = singleTrees_[i]->versions_->OldestLogNumber();
    singleTrees_[i]->versions_->AddLiveFiles(&expected);
  }

  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::vector<std::string> tmpFilenames;
  for (int i = 0; i < config::kNumSingleTrees; i++) {
    s = env_->GetChildren(dbname_ + "/vol" + std::to_string(i + 1),
                          &tmpFilenames);
    if (!s.ok()) {
      return s;
    }
    for (const std::string tmpFilename : tmpFilenames) {
      filenames.push_back(tmpFilename);
    }
  }
  if (options_.depart_log) {
    s = env_->GetChildren(dbname_ + "/" + options_.log_dir, &tmpFilenames);
    if (!s.ok()) {
    return s;
    }
    for (const std::string tmpFilename : tmpFilenames) {
      filenames.push_back(tmpFilename);
    }
  }
  
  // versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (std::find(prev_logs.begin(), prev_logs.end(),
                                             number) != prev_logs.end())))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    // 文件号共用，只需要一个version_set设置即可
    singleTrees_[0]->versions_->MarkFileNumberUsed(logs[i]);
  }

   for (uint32_t j = 0; j < config::kNumSingleTrees; j++) {
    if (singleTrees_[j]->versions_->LastSequence() < max_sequence) {
      singleTrees_[j]->versions_->SetLastSequence(max_sequence);
    }
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem[config::kNumSingleTrees] = {nullptr};
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);
    Slice key, value;
    ValueType type;
    // 取得record记录的writebatch之后先判断属于哪一个Tree，然后插入到相应的mem中
    status = batch.GetKey(0, &key, &value, &type);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    uint32_t Tree_id =
        GetSingleTreeID(key.data(), key.size()) % config::kNumSingleTrees;
    if (mem[Tree_id] == nullptr) {
      mem[Tree_id] = new MemTable(internal_comparator_);
      mem[Tree_id]->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem[Tree_id]);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem[Tree_id]->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      singleTrees_[Tree_id]->mutex_.Lock();
      status = singleTrees_[Tree_id]->WriteLevel0Table(mem[Tree_id],
                                                       &edit[Tree_id], nullptr);
      singleTrees_[Tree_id]->mutex_.Unlock();
      mem[Tree_id]->Unref();
      mem[Tree_id] = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    for (uint32_t i = 0; i < config::kNumSingleTrees; i++)
      assert(singleTrees_[i]->mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
        if (mem[i] != nullptr) {
          singleTrees_[i]->mem_ = mem[i];
          mem[i] = nullptr;
        } else {
          // mem can be nullptr if lognum exists but was empty.
          singleTrees_[i]->mem_ = new MemTable(internal_comparator_);
          singleTrees_[i]->mem_->Ref();
        }
      }
    }
  }

  for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
    if (mem[i] != nullptr) {
      // all mem did not get reused; compact it.
      if (status.ok()) {
        *save_manifest = true;
        singleTrees_[i]->mutex_.Lock();
        status = singleTrees_[i]->WriteLevel0Table(mem[i], &edit[i], nullptr);
        singleTrees_[i]->mutex_.Unlock();
      }
      mem[i]->Unref();
    }
  }

  return status;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::RemoveObsoleteFiles() {

  // Make a set of all of the live files
  uint64_t min_log_num = singleTrees_[0]->versions_->OldestLogNumber();
  std::vector<uint64_t> prev_logs;
  for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
    if (min_log_num > singleTrees_[i]->versions_->OldestLogNumber()) {
      min_log_num = singleTrees_[i]->versions_->OldestLogNumber();
    }
    prev_logs.push_back(singleTrees_[i]->versions_->PrevLogNumber());
  }

  // printf("min lognum : %lu\n",min_log_num);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  if (options_.depart_log) {
    std::vector<std::string> tmpFilenames;
    env_->GetChildren(dbname_ + "/" + options_.log_dir, &tmpFilenames);
    for (const std::string tmpFilename : tmpFilenames) {
      filenames.push_back(tmpFilename);
    }
  }

  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= min_log_num) ||
                  (std::find(prev_logs.begin(), prev_logs.end(), number) !=
                   prev_logs.end()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= singleTrees_[0]->versions_->ManifestFileNumber());
          break;
        case kTableFile:
        case kTempFile:
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
        if (type == kLogFile && options_.depart_log) {
          env_->RemoveFile(dbname_ + "/" + options_.log_dir + "/" 
                            + filename);
        } else {
          files_to_delete.push_back(std::move(filename));
        }
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename);
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit[config::kNumSingleTrees];  // 版本初始化
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(edit, &save_manifest);
  // 恢复重成功，根据是否复用，创建新的log与mem
  if (s.ok()) {
    bool flag = false;
    for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
      if (impl->singleTrees_[i]->mem_ != nullptr) {
        flag = true;
        break;
      }
    }
    if (flag == false) {
      uint64_t new_log_number =
          impl->singleTrees_[0]->versions_->NewFileNumber();
      WritableFile* lfile;
      s = impl->options_.env->NewWritableFile(
          LogFileName(impl->log_dir, new_log_number), &lfile);
      if (s.ok()) {
        impl->logfile_ = lfile;
        impl->logfile_number_ = new_log_number;
        impl->log_ = new log::Writer(lfile);
      }
      for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
        edit[i].SetLogNumber(new_log_number);
        impl->singleTrees_[i]->mem_ = new MemTable(impl->internal_comparator_);
        impl->singleTrees_[i]->mem_->Ref();
      }
    }
  }
  // 重新建立manifest文件
  if (s.ok() && save_manifest) {
    std::vector<VersionSet*> version_sets;
    for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
      version_sets.push_back(impl->singleTrees_[i]->versions_);
    }
    s = VersionSet::InitManifest(options.env, dbname, version_sets);
    for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
      edit[i].SetPrevLogNumber(0);
      edit[i].SetLogNumber(impl->logfile_number_);
      edit[i].SetOldestLogNumber(impl->logfile_number_);
      s = version_sets[i]->LogAndApply(&edit[i], &impl->mutex_);
    }
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
      impl->singleTrees_[i]->mutex_.Lock();
      impl->singleTrees_[i]->MaybeScheduleCompaction();
      impl->singleTrees_[i]->mutex_.Unlock();
    }
  }
  //impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    for (uint32_t i = 0; i < config::kNumSingleTrees; i++) {
      std::string sub_filedir;
      std::vector<std::string> sub_filenames;
      sub_filedir = dbname + "/vol" + std::to_string(i + 1);
      result = env->GetChildren(sub_filedir, &sub_filenames);
      for (size_t i = 0; i < sub_filenames.size(); i++) {
        if (ParseFileName(sub_filenames[i], &number, &type)) {
          Status del = env->DeleteFile(sub_filedir + "/" + sub_filenames[i]);
          if (result.ok() && !del.ok()) {
            result = del;
          }
        }
      }
      env->DeleteDir(sub_filedir);
    }
    if (options.depart_log) {
      std::string sub_filedir;
      std::vector<std::string> sub_filenames;
      sub_filedir = dbname + "/" + options.log_dir;
      result = env->GetChildren(sub_filedir, &sub_filenames);
      for (size_t i = 0; i < sub_filenames.size(); i++) {
        if (ParseFileName(sub_filenames[i], &number, &type)) {
          Status del = env->DeleteFile(sub_filedir + "/" + sub_filenames[i]);
          if (result.ok() && !del.ok()) {
            result = del;
          }
        }
      }
      env->DeleteDir(sub_filedir);
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
