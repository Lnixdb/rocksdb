#pragma once
#include <rocksdb/file_system.h>

#include "file_client/zonda_fs.h"      // file_client::OpenFile
#include "comm/error_code.h"           // comm::IsNotOk
#include "comm/context.h"              // comm::Ctx
#include "comm/request_id.h"           // comm::RequestId::Next()
#include "file_client/comm/fs_types.h" // SeekWhence
#include "error_code.pb.h" // comm::ZONDA_OK (error code)


namespace ROCKSDB_NAMESPACE {

std::string ZondaIOErrorMsg(const std::string& context,
                       const std::string& file_name);
// file_name can be left empty if it is not unkown.
IOStatus ZondaIOError(const std::string& context, const std::string& file_name,
                 comm::ErrorCode err_number = comm::ZONDA_UNOK);

class ZondaFSSequentialFile : public FSSequentialFile {
public:
  ZondaFSSequentialFile(const std::shared_ptr<file_client::FileHandle>& handler,
                        const std::string& filename);

  ~ZondaFSSequentialFile() override;

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
  bool closed_{false};
  std::shared_ptr<file_client::FileHandle> handler_;
};

class ZondaFSRandomAccessFile : public FSRandomAccessFile {
public:
  ZondaFSRandomAccessFile(const std::shared_ptr<file_client::FileHandle>& handler,
                          const std::string& filename);

  ~ZondaFSRandomAccessFile() override;

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
  bool closed_{false};
  std::shared_ptr<file_client::FileHandle> handler_;
};

class ZondaFSWritableFile : public FSWritableFile {
public:

  ZondaFSWritableFile(const std::shared_ptr<file_client::FileHandle>& handler,
                        const std::string& filename);

  ~ZondaFSWritableFile() override;

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
  bool closed_{false};
  uint64_t file_size_{0};
  std::string filename_;
  std::shared_ptr<file_client::FileHandle> handler_;
};

class ZondaFSDirectory : public FSDirectory {
public:
  explicit ZondaFSDirectory() = default;
  ~ZondaFSDirectory();
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
