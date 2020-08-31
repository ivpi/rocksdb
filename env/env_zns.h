//  Copyright (c) 2019, Samsung Electronics.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  Written by Ivan L. Picoli <i.picoli@samsung.com>

#include <unistd.h>
#include <string>
#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include <iostream>
#include <atomic>
#include <libzrocks.h>

#define ZNS_DEBUG     0
#define ZNS_DEBUG_R   (ZNS_DEBUG && 0) /* Read */
#define ZNS_DEBUG_W   (ZNS_DEBUG && 1) /* Write and Sync */
#define ZNS_DEBUG_AF  (ZNS_DEBUG && 0) /* Append and Flush */

#define ZNS_OBJ_STORE 0
#define ZNS_PREFETCH  1
#define ZNS_PREFETCH_BUF_SZ (1024 * 1024 * 1) /* 1MB */

#define ZNS_MAX_MAP_ENTS 64

using namespace std;

namespace rocksdb {


/* ### ZNS Environment ### */

class ZNSFile {
  public:
    const std::string name;
    size_t size;
    int level;
    std::vector<struct zrocks_map> map;

    ZNSFile(const std::string& fname, int lvl) :
				name(fname),
				level(lvl)  {
	size = 0;
    }

    ~ZNSFile() {}
};

class ZNSEnv : public Env {

 public:
  std::map<std::string, ZNSFile*> files;

  explicit ZNSEnv(const std::string& dname) : dev_name(dname) {
    posixEnv = Env::Default();
    writable_file_leveled = true;

    cout << "Initializing ZNS Environment" << endl;

    if (zrocks_init (dev_name.data())) {
	cout << "ZRocks failed to initialize." <<  endl;
	exit(1);
    }
  }

  virtual ~ZNSEnv() {
    zrocks_exit ();
    cout << "Destroying ZNS Environment" <<  endl;
  }


  /* ### Implemented at env_zns.cc ### */

  Status NewSequentialFile(const std::string& fname,
                           std::unique_ptr<SequentialFile>* result,
                           const EnvOptions& options) override;

  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& options) override;

  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<WritableFile>* result,
                         const EnvOptions& options) override;

  Status NewWritableLeveledFile(const std::string& fname,
                         std::unique_ptr<WritableFile>* result,
                         const EnvOptions& options,
			 int level) override;

  Status DeleteFile(const std::string& fname) override;

  Status GetFileSize(const std::string& fname, uint64_t* size) override;

  Status GetFileModificationTime(const std::string& fname,
                                 uint64_t* file_mtime) override;


  /* ### Implemented here ### */

  Status LinkFile(const std::string& /*src*/,
                  const std::string& /*target*/) override {
    return Status::NotSupported(); // not supported
  }

  static uint64_t gettid() {
    assert(sizeof(pthread_t) <= sizeof(uint64_t));
    return (uint64_t)pthread_self();
  }

  uint64_t GetThreadID() const override {
    return ZNSEnv::gettid();
  }


  /* ### Posix inherited functions ### */

  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) override {
    if (ZNS_DEBUG) cout << __func__ << ":" << name << endl;
    return posixEnv->NewDirectory (name, result);
  }

  Status FileExists(const std::string& fname) override {
    if (ZNS_DEBUG) cout << __func__ << ":" << fname << endl;
    return posixEnv->FileExists(fname);
  }

  Status GetChildren(const std::string& path,
                     std::vector<std::string>* result) override {
    if (ZNS_DEBUG) cout << __func__ << ":" << path << endl;
    return posixEnv->GetChildren(path, result);
  }

  Status CreateDir(const std::string& name) override {
    if (ZNS_DEBUG) cout << __func__ << ":" << name << endl;
    return posixEnv->CreateDir(name);
  }

  Status CreateDirIfMissing(const std::string& name) override {
    if (ZNS_DEBUG) cout << __func__ << ":" << name << endl;
    return posixEnv->CreateDirIfMissing(name);
  }

  Status DeleteDir(const std::string& name) override {
    if (ZNS_DEBUG) cout << __func__ << ":" << name << endl;
    return posixEnv->DeleteDir(name);
  }

  Status RenameFile(const std::string& src, const std::string& target) override {
    if (ZNS_DEBUG) cout << __func__ << ":" << src << ":" << target << endl;
    return posixEnv->RenameFile(src, target);
  };

  Status LockFile(const std::string& fname, FileLock** lock) override {
    if (ZNS_DEBUG) cout << __func__ << ":" << fname << endl;
    return posixEnv->LockFile(fname, lock);
  }

  Status UnlockFile(FileLock* lock) override {
    if (ZNS_DEBUG) cout << __func__ << endl;
    return posixEnv->UnlockFile(lock);
  }

  Status NewLogger(const std::string& fname,
                   std::shared_ptr<Logger>* result) override {
    if (ZNS_DEBUG) cout << __func__ << ":" << fname << endl;
    return posixEnv->NewLogger (fname, result);
  }

  void Schedule(void (*function)(void* arg), void* arg, Priority pri = LOW,
                void* tag = nullptr,
                void (*unschedFunction)(void* arg) = 0) override {
    posixEnv->Schedule(function, arg, pri, tag, unschedFunction);
  }

  int UnSchedule(void* tag, Priority pri) override {
    return posixEnv->UnSchedule(tag, pri);
  }

  void StartThread(void (*function)(void* arg), void* arg) override {
    posixEnv->StartThread(function, arg);
  }

  void WaitForJoin() override {
    posixEnv->WaitForJoin();
  }

  unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override {
    return posixEnv->GetThreadPoolQueueLen(pri);
  }

  Status GetTestDirectory(std::string* path) override {
    return posixEnv->GetTestDirectory(path);
  }

  uint64_t NowMicros() override {
    return posixEnv->NowMicros();
  }

  void SleepForMicroseconds(int micros) override {
    posixEnv->SleepForMicroseconds(micros);
  }

  Status GetHostName(char* name, uint64_t len) override {
    return posixEnv->GetHostName(name, len);
  }

  Status GetCurrentTime(int64_t* unix_time) override {
    return posixEnv->GetCurrentTime(unix_time);
  }

  Status GetAbsolutePath(const std::string& db_path,
                         std::string* output_path) override {
    return posixEnv->GetAbsolutePath(db_path, output_path);
  }

  void SetBackgroundThreads(int number, Priority pri = LOW) override {
    posixEnv->SetBackgroundThreads(number, pri);
  }

  int GetBackgroundThreads(Priority pri = LOW) override {
    return posixEnv->GetBackgroundThreads(pri);
  }

  void IncBackgroundThreadsIfNeeded(int number, Priority pri) override {
    posixEnv->IncBackgroundThreadsIfNeeded(number, pri);
  }

  std::string TimeToString(uint64_t number) override {
    return posixEnv->TimeToString(number);
  }

 private:
  Env*  posixEnv;       // This object is derived from Env, but not from
                        // posixEnv. We have posixnv as an encapsulated
                        // object here so that we can use posix timers,
                        // posix threads, etc.
  const std::string dev_name;

  bool IsFilePosix (const std::string& fname) {
    return (fname.find("uuid") != std::string::npos ||
	fname.find("CURRENT") != std::string::npos ||
	fname.find("IDENTITY") != std::string::npos ||
	fname.find("MANIFEST") != std::string::npos ||
	fname.find("OPTIONS") != std::string::npos ||
	fname.find("LOG") != std::string::npos ||
	fname.find("LOCK") != std::string::npos ||
	fname.find(".dbtmp") != std::string::npos ||
	fname.find(".log") != std::string::npos);
  }
};


/* ### SequentialFile, RandAccessFile, and Writable File ### */

class ZNSSequentialFile : public SequentialFile {

 private:
  std::string filename_;
  bool use_direct_io_;
  size_t logical_sector_size_;
  uint64_t ztl_id;

 public:
  ZNSSequentialFile(const std::string& fname, const EnvOptions& options)
    : filename_(fname),
      use_direct_io_(options.use_direct_reads),
      logical_sector_size_(ZNS_ALIGMENT) {

    ztl_id = stoi(fname.substr(fname.length() - 10, 6));
  }

  virtual ~ZNSSequentialFile() {

  }


  /* ### Implemented at env_zns_io.cc ### */

  virtual Status Read(size_t n, Slice* result, char* scratch) override;

  virtual Status PositionedRead(uint64_t offset, size_t n, Slice* result,
					       char* scratch) override;

  virtual Status Skip(uint64_t n) override;

  virtual Status InvalidateCache(size_t offset, size_t length) override;


  /* ### Implemented here ### */

  virtual bool use_direct_io() const override {
    return use_direct_io_;
  }

  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
};


class ZNSRandomAccessFile : public RandomAccessFile {
 private:
  std::string filename_;
  bool use_direct_io_;
  size_t logical_sector_size_;
  uint64_t ztl_id;

  ZNSEnv *env_zns;

#if ZNS_PREFETCH
  char  *prefetch;
  size_t prefetch_sz;
  uint64_t prefetch_off;
  atomic_flag prefetch_lock = ATOMIC_FLAG_INIT;
#endif

 public:
  ZNSRandomAccessFile(const std::string& fname, ZNSEnv* zns,
					    const EnvOptions& options)
    : filename_(fname),
      use_direct_io_(options.use_direct_reads),
      logical_sector_size_(ZNS_ALIGMENT),
      env_zns(zns) {

    ztl_id = stoi(fname.substr(fname.length() - 10, 6));

#if ZNS_PREFETCH
    prefetch = (char *) zrocks_alloc (ZNS_PREFETCH_BUF_SZ);
    if (!prefetch) {
	cout << " ZRocks (alloc prefetch) error." << endl;
	prefetch = nullptr;
    }
    prefetch_sz = 0;
#endif
  }

  virtual ~ZNSRandomAccessFile() {
#if ZNS_PREFETCH
    zrocks_free (prefetch);
#endif
  }


  /* ### Implemented at env_zns_io.cc ### */

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
					char* scratch) const override;

  virtual Status Prefetch(uint64_t offset, size_t n) override;

  virtual size_t GetUniqueId(char* id, size_t max_size) const override;

  virtual Status InvalidateCache(size_t offset, size_t length) override;

  virtual Status ReadObj(uint64_t offset, size_t n, Slice* result,
						char* scratch) const;

  virtual Status ReadOffset(uint64_t offset, size_t n, Slice* result,
						char* scratch) const;


  /* ### Implemented here ### */

  virtual bool use_direct_io() const override {
    return use_direct_io_;
  }

  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
};

class ZNSWritableFile : public WritableFile {
 private:
  const std::string filename_;
  const bool use_direct_io_;
  int fd_;
  uint64_t filesize_;
  uint64_t ztl_id;
  size_t logical_sector_size_;
#ifdef ROCKSDB_FALLOCATE_PRESENT
  bool allow_fallocate_;
  bool fallocate_with_keep_size_;
#endif
  char* wcache;
  char* cache_off;
  int level;

  ZNSEnv *env_zns;
  uint64_t map_off;

 public:
  explicit ZNSWritableFile(const std::string& fname, ZNSEnv* zns,
				const EnvOptions& options, int lvl)
    : WritableFile(options),
      filename_(fname),
      use_direct_io_(options.use_direct_writes),
      logical_sector_size_(ZNS_ALIGMENT),
      level(lvl),
      env_zns(zns) {

#ifdef ROCKSDB_FALLOCATE_PRESENT
    allow_fallocate_ = options.allow_fallocate;
    fallocate_with_keep_size_ = options.fallocate_with_keep_size;
#endif

    ztl_id = stoi(fname.substr(fname.length() - 10, 6));

    wcache = (char *) zrocks_alloc (ZNS_MAX_BUF);
    if (!wcache) {
	cout << " ZRocks (alloc) error." << endl;
	cache_off = nullptr;
    }

    cache_off = wcache;
    map_off = 0;
  }

  virtual ~ZNSWritableFile() {
    zrocks_free(wcache);
  }


  /* ### Implemented at env_zns_io.cc ### */

  virtual Status Append(const Slice& data) override;

  virtual Status PositionedAppend(const Slice& data, uint64_t offset) override;

  virtual Status Truncate(uint64_t size) override;

  virtual Status Close() override;

  virtual Status Flush() override;

  virtual Status Sync() override;

  virtual Status Fsync() override;

  virtual Status InvalidateCache(size_t offset, size_t length) override;

#ifdef ROCKSDB_FALLOCATE_PRESENT
  virtual Status Allocate(uint64_t offset, uint64_t len) override;
#endif

  virtual Status RangeSync(uint64_t offset, uint64_t nbytes) override;

  virtual size_t GetUniqueId(char* id, size_t max_size) const override;


  /* ### Implemented here ### */

  virtual bool IsSyncThreadSafe() const override {
    return true;
  }

  virtual bool use_direct_io() const override {
    return use_direct_io_;
  }

  virtual void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
     if (ZNS_DEBUG) cout << __func__ << " : " << hint << endl;
  }

  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
};

} // namespace rocksdb
