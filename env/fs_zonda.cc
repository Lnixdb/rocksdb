#include <dirent.h>
#ifndef ROCKSDB_NO_DYNAMIC_EXTENSION
#include <dlfcn.h>
#endif
#include <fcntl.h>
#include <pthread.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#if defined(OS_LINUX) || defined(OS_SOLARIS) || defined(OS_ANDROID)
#include <sys/statfs.h>
#include <sys/sysmacros.h>
#endif
#include <sys/statvfs.h>
#include <sys/time.h>
#include <sys/types.h>

#include <algorithm>
#include <ctime>
// Get nano time includes
#if defined(OS_LINUX) || defined(OS_FREEBSD)
#elif defined(__MACH__)
#include <Availability.h>
#include <mach/clock.h>
#include <mach/mach.h>
#else
#include <chrono>
#endif
#include <deque>
#include <set>
#include <vector>
#include <iostream>
#include "env/io_posix.h"
#include "monitoring/thread_status_updater.h"

#include "port/port.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

#include "util/compression_context_cache.h"
#include "util/random.h"



#include <rocksdb/file_system.h>

namespace ROCKSDB_NAMESPACE {

namespace {

class ZondaFileSystem : public FileSystem {
 public:
  ZondaFileSystem(
    const std::shared_ptr<FileSystem>& base_fs){
    base_fs_ = base_fs;
  }

  static const char* kClassName() { return "ZondaFileSystem"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kDefaultName(); }

  ~ZondaFileSystem() override = default;
  bool IsInstanceOf(const std::string& name) const override {
    if (name == "zonda") {
      return true;
    } else {
      return FileSystem::IsInstanceOf(name);
    }
  }

  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override {
    return base_fs_->NewSequentialFile(fname, options, result, dbg);
  }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    return base_fs_->NewRandomAccessFile(fname, options, result, dbg);
  }


  IOStatus NewWritableFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    return base_fs_->NewWritableFile(fname, options, result, dbg);
  }

  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& options,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override {
    return base_fs_->ReopenWritableFile(fname, options, result, dbg);
  }

  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& options,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override {
    return base_fs_->ReuseWritableFile(fname, old_fname, options, result, dbg);
  }

  IOStatus NewRandomRWFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override {
    return base_fs_->NewRandomRWFile(fname, options, result, dbg);
  }

  IOStatus NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override {
    return base_fs_->NewMemoryMappedFileBuffer(fname, result);
  }

  IOStatus NewDirectory(const std::string& name, const IOOptions& opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override {
    return base_fs_->NewDirectory(name, opts, result, dbg);
  }

  IOStatus FileExists(const std::string& fname, const IOOptions& opts,
                      IODebugContext* dbg) override {
    return base_fs_->FileExists(fname,  opts, dbg);
  }

  IOStatus GetChildren(const std::string& dir, const IOOptions& opts,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override {
    return  base_fs_->GetChildren(dir, opts, result, dbg);
  }

  IOStatus DeleteFile(const std::string& fname, const IOOptions& opts,
                      IODebugContext* dbg) override {
   return base_fs_->DeleteFile(fname, opts, dbg);
  }

  IOStatus CreateDir(const std::string& name, const IOOptions& opts,
                     IODebugContext* dbg) override {
    return base_fs_->CreateDir(name, opts, dbg);
  }

  IOStatus CreateDirIfMissing(const std::string& name,
                              const IOOptions& opts,
                              IODebugContext* dbg) override {
    return base_fs_->CreateDirIfMissing(name, opts, dbg);
  }

  IOStatus DeleteDir(const std::string& name, const IOOptions& opts,
                     IODebugContext* dbg) override {
    return base_fs_->DeleteDir(name, opts, dbg);
  }

  IOStatus GetFileSize(const std::string& fname, const IOOptions& opts,
                       uint64_t* size, IODebugContext* dbg) override {
    return base_fs_->GetFileSize(fname, opts, size, dbg);
  }

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& opts,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override {
    return base_fs_->GetFileModificationTime(fname, opts, file_mtime, dbg);
  }

  IOStatus RenameFile(const std::string& src, const std::string& target,
                      const IOOptions& opts,
                      IODebugContext* dbg) override {
    return base_fs_->RenameFile(src, target, opts, dbg);
  }

  IOStatus LinkFile(const std::string& src, const std::string& target,
                    const IOOptions& opts,
                    IODebugContext* dbg) override {
    return base_fs_->LinkFile(src, target, opts, dbg);
  }

  IOStatus NumFileLinks(const std::string& fname, const IOOptions& opts,
                        uint64_t* count, IODebugContext* dbg) override {
    return base_fs_->NumFileLinks(fname, opts, count, dbg);
  }

  IOStatus AreFilesSame(const std::string& first, const std::string& second,
                        const IOOptions& opts, bool* res,
                        IODebugContext* dbg) override {
    return base_fs_->AreFilesSame(first, second, opts, res, dbg);
  }

  IOStatus LockFile(const std::string& fname, const IOOptions& opts,
                    FileLock** lock, IODebugContext* dbg) override {
    return base_fs_->LockFile(fname, opts, lock, dbg);
  }

  IOStatus UnlockFile(FileLock* lock, const IOOptions& opts,
                      IODebugContext* dbg) override {
    return base_fs_->UnlockFile(lock, opts, dbg);
  }

  IOStatus GetAbsolutePath(const std::string& db_path,
                           const IOOptions& opts, std::string* output_path,
                           IODebugContext* dbg) override {
    return base_fs_->GetAbsolutePath(db_path, opts, output_path, dbg);
  }

  IOStatus GetTestDirectory(const IOOptions& opts, std::string* result,
                            IODebugContext* dbg) override {
    return base_fs_->GetTestDirectory(opts, result, dbg);
  }

  IOStatus GetFreeSpace(const std::string& fname, const IOOptions& opts,
                        uint64_t* free_space,
                        IODebugContext* dbg) override {
    return base_fs_->GetFreeSpace(fname, opts, free_space, dbg);
  }

  IOStatus IsDirectory(const std::string& path, const IOOptions& opts,
                       bool* is_dir, IODebugContext* dbg) override {
   return base_fs_->IsDirectory(path, opts, is_dir, dbg);
  }

  FileOptions OptimizeForLogWrite(const FileOptions& file_options,
                                  const DBOptions& db_options) const override {
    return base_fs_->OptimizeForLogWrite(file_options, db_options);
  }

  FileOptions OptimizeForManifestWrite(
      const FileOptions& file_options) const override {
   return base_fs_->OptimizeForManifestWrite(file_options);
  }

  FileOptions OptimizeForCompactionTableRead(
      const FileOptions& file_options,
      const ImmutableDBOptions& db_options) const override {
   return base_fs_->OptimizeForCompactionTableRead(file_options, db_options);
  }

#ifdef OS_LINUX
  Status RegisterDbPaths(const std::vector<std::string>& paths) override {
    return base_fs_->RegisterDbPaths(paths);
  }
  Status UnregisterDbPaths(const std::vector<std::string>& paths) override {
    return base_fs_->UnregisterDbPaths(paths);
  }
#endif
 private:

  // TODO:
  // 1. Update Poll API to take into account min_completions
  // and returns if number of handles in io_handles (any order) completed is
  // equal to atleast min_completions.
  // 2. Currently in case of direct_io, Read API is called because of which call
  // to Poll API fails as it expects IOHandle to be populated.
  IOStatus Poll(std::vector<void*>& io_handles,
                size_t min_completions) override {
    return base_fs_->Poll(io_handles, min_completions);
  }

  IOStatus AbortIO(std::vector<void*>& io_handles) override {
    return base_fs_->AbortIO(io_handles);
  }

  void SupportedOps(int64_t& supported_ops) override {
    return base_fs_->SupportedOps(supported_ops);
  }

 private:
  std::shared_ptr<FileSystem> base_fs_;  // The underlying file system
};

}

//
// Zonda FS
//
std::shared_ptr<FileSystem> FileSystem::ZondaFS(const std::shared_ptr<FileSystem>& base_fs) {
  STATIC_AVOID_DESTRUCTION(std::shared_ptr<FileSystem>, instance)
  (std::make_shared<ZondaFileSystem>(base_fs));
  return instance;
}

}
