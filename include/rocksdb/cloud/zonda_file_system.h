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

#include "rocksdb/options.h"
#include "rocksdb/slice.h"

#include <rocksdb/file_system.h>

namespace ROCKSDB_NAMESPACE {

class ZondaFileSystem : public FileSystem {
 public:
  ZondaFileSystem(const std::shared_ptr<FileSystem>& base_fs);

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
                             IODebugContext* dbg) override;

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override;


  IOStatus NewWritableFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;

  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& options,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override ;

  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& options,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override ;

  IOStatus NewRandomRWFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override ;

  IOStatus NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override;

  IOStatus NewDirectory(const std::string& name, const IOOptions& opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;

  IOStatus FileExists(const std::string& fname, const IOOptions& opts,
                      IODebugContext* dbg) override ;

  IOStatus GetChildren(const std::string& dir, const IOOptions& opts,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override;

  IOStatus DeleteFile(const std::string& fname, const IOOptions& opts,
                      IODebugContext* dbg) override;

  IOStatus CreateDir(const std::string& name, const IOOptions& opts,
                     IODebugContext* dbg) override;

  IOStatus CreateDirIfMissing(const std::string& name,
                              const IOOptions& opts,
                              IODebugContext* dbg) override;

  IOStatus DeleteDir(const std::string& name, const IOOptions& opts,
                     IODebugContext* dbg) override;

  IOStatus GetFileSize(const std::string& fname, const IOOptions& opts,
                       uint64_t* size, IODebugContext* dbg) override;

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& opts,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override;

  IOStatus RenameFile(const std::string& src, const std::string& target,
                      const IOOptions& opts,
                      IODebugContext* dbg) override;

  IOStatus LinkFile(const std::string& src, const std::string& target,
                    const IOOptions& opts,
                    IODebugContext* dbg) override;

  IOStatus NumFileLinks(const std::string& fname, const IOOptions& opts,
                        uint64_t* count, IODebugContext* dbg) override;

  IOStatus AreFilesSame(const std::string& first, const std::string& second,
                        const IOOptions& opts, bool* res,
                        IODebugContext* dbg) override;

  IOStatus LockFile(const std::string& fname, const IOOptions& opts,
                    FileLock** lock, IODebugContext* dbg) override;

  IOStatus UnlockFile(FileLock* lock, const IOOptions& opts,
                      IODebugContext* dbg) override;

  IOStatus GetAbsolutePath(const std::string& db_path,
                           const IOOptions& opts, std::string* output_path,
                           IODebugContext* dbg) override;

  IOStatus GetTestDirectory(const IOOptions& opts, std::string* result,
                            IODebugContext* dbg) override;

  IOStatus GetFreeSpace(const std::string& fname, const IOOptions& opts,
                        uint64_t* free_space,
                        IODebugContext* dbg) override;

  IOStatus IsDirectory(const std::string& path, const IOOptions& opts,
                       bool* is_dir, IODebugContext* dbg) override;

  FileOptions OptimizeForLogWrite(const FileOptions& file_options,
                                  const DBOptions& db_options) const override;

  FileOptions OptimizeForManifestWrite(
      const FileOptions& file_options) const override ;

  FileOptions OptimizeForCompactionTableRead(
      const FileOptions& file_options,
      const ImmutableDBOptions& db_options) const override;
#ifdef OS_LINUX
  Status RegisterDbPaths(const std::vector<std::string>& paths) override;
  Status UnregisterDbPaths(const std::vector<std::string>& paths) override;
#endif

private:

  // TODO:
  // 1. Update Poll API to take into account min_completions
  // and returns if number of handles in io_handles (any order) completed is
  // equal to atleast min_completions.
  // 2. Currently in case of direct_io, Read API is called because of which call
  // to Poll API fails as it expects IOHandle to be populated.
  IOStatus Poll(std::vector<void*>& io_handles,
                size_t min_completions) override;

  IOStatus AbortIO(std::vector<void*>& io_handles) override;

  void SupportedOps(int64_t& supported_ops) override;

 private:
  std::shared_ptr<FileSystem> base_fs_;  // The underlying file system
};

}
