#pragma once
#include <iostream>
#include <rocksdb/file_system.h>
#include <hdfs/hdfs.h>

namespace ROCKSDB_NAMESPACE {

void errLog(const std::string& func, const std::string& fname);
void okLog(const std::string& func, const std::string& fname);

class HDFSSequentialFile : public FSSequentialFile {
public:
  HDFSSequentialFile(hdfsFS conn,  hdfsFile fd, const std::string& filename);

  ~HDFSSequentialFile() override;

  IOStatus Read(size_t n, const IOOptions& opts, Slice* result, char* scratch,
                IODebugContext* dbg) override;
  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& opts,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;
  IOStatus Skip(uint64_t n) override;
  IOStatus InvalidateCache(size_t offset, size_t length) override;
  bool use_direct_io() const override { return false; }

private:
  std::string filename_;
  hdfsFS nn_conn_;
  hdfsFile fd_;
};

class HDFSRandomAccessFile : public FSRandomAccessFile {
public:
  HDFSRandomAccessFile(hdfsFS conn,  hdfsFile file, const std::string fname) :
      filename_(fname), nn_conn_(conn), fd_(file) {}

  ~HDFSRandomAccessFile() override;

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& opts, Slice* result,
              char* scratch, IODebugContext* dbg) const override;
  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                   const IOOptions& options, IODebugContext* dbg) override;
  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& opts,
                    IODebugContext* dbg) override;
  void Hint(AccessPattern pattern) override;
  IOStatus InvalidateCache(size_t offset, size_t length);
  bool use_direct_io() const override { return false; }

private:
  std::string filename_;
  hdfsFS nn_conn_;
  hdfsFile fd_;
};

class HDFSWritableFile : public FSWritableFile {
public:
  HDFSWritableFile(hdfsFS conn,  hdfsFile file, const std::string filename) :
     nn_conn_(conn), fd_(file), filename_(filename) {}

  ~HDFSWritableFile() override;

  IOStatus Truncate(uint64_t /*size*/, const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override ;

  IOStatus Close(const IOOptions &options, IODebugContext *dbg) override;

  IOStatus Append(const Slice& data, const IOOptions& opts,
                    IODebugContext* dbg) override;

  IOStatus Append(const Slice& data, const IOOptions& opts,
                const DataVerificationInfo& /* verification_info */,
                IODebugContext* dbg) override;

  IOStatus Flush(const IOOptions& opts, IODebugContext* dbg) override;

  IOStatus Sync(const IOOptions& opts, IODebugContext* dbg) override;

  IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override;

  uint64_t GetFileSize(const IOOptions& opts, IODebugContext* dbg) override;

  IOStatus InvalidateCache(size_t offset, size_t length) override;

private:
  hdfsFS nn_conn_;
  hdfsFile fd_;
  uint64_t file_size_{0};
  std::string filename_;
};


class HDFSDirectory : public FSDirectory {
public:
  explicit HDFSDirectory() = default;
  ~HDFSDirectory();
  IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override;

  IOStatus Close(const IOOptions& opts, IODebugContext* dbg) override;

  IOStatus FsyncWithDirOptions(
      const IOOptions&, IODebugContext*,
      const DirFsyncOptions& dir_fsync_options) override;

private:
  int fd_;
  bool is_btrfs_;
  const std::string directory_name_;
};


}
