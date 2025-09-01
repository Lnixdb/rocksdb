#include "zonda_file_system_io.h"

#include "error_code.pb.h" // comm::ZONDA_OK (error code)

namespace ROCKSDB_NAMESPACE {

std::string ZondaIOErrorMsg(const std::string& context,
                       const std::string& file_name) {
  if (file_name.empty()) {
    return context;
  }
  return context + ": " + file_name;
}

// file_name can be left empty if it is not unkown.
IOStatus ZondaIOError(const std::string& context, const std::string& file_name,
                 comm::ErrorCode err_code) {

  switch (err_code) {
    case comm::ZONDA_FS_DIR_NOT_EXIST:
      return IOStatus::PathNotFound(ZondaIOErrorMsg(context, file_name), comm::ErrorCode_Name(err_code));

    case comm::ZONDA_FS_FILE_NOT_EXIST:
      return IOStatus::PathNotFound(ZondaIOErrorMsg(context, file_name), comm::ErrorCode_Name(err_code));

    default:
      return IOStatus::IOError(ZondaIOErrorMsg(context, file_name), comm::ErrorCode_Name(err_code));
  }
}

ZondaFSSequentialFile::ZondaFSSequentialFile(
    const std::shared_ptr<file_client::FileHandle>& handler,
    const std::string& fname) : filename_(fname), handler_(handler) {
}

ZondaFSSequentialFile::~ZondaFSSequentialFile() {
  file_client::CloseFile(handler_);
}

IOStatus ZondaFSSequentialFile::Read(size_t n, const IOOptions& opts,
                                  Slice* result, char* scratch,
                                  IODebugContext* dbg) {
  assert(result != nullptr && !use_direct_io());
  IOStatus s;
  comm::Ctx ctx(comm::RequestId::Next(), "read_seq_file");
  auto [code, r] = file_client::Read(&ctx, handler_, scratch, n);
  *result = Slice(scratch, r);
  if (r < n) {
    if (code == comm::ZONDA_FS_EOF) {

    } else {
      return ZondaIOError("While reading file sequentially", filename_, code);
    }
  }
  return IOStatus::OK();
}

IOStatus ZondaFSSequentialFile::PositionedRead(uint64_t offset, size_t n,
                                            const IOOptions& opts,
                                            Slice* result, char* scratch,
                                            IODebugContext* dbg) {
  return IOStatus::NotSupported("Zonda file system PositionedRead");
}

IOStatus ZondaFSSequentialFile::Skip(uint64_t n) {
  comm::Ctx ctx(comm::RequestId::Next(), "seek_file");
  auto code = file_client::Seek(&ctx, handler_, n, file_client::SeekWhence::WHENCE_SEEK_CUR);
  if (comm::IsNotOk(code)) {
    return ZondaIOError("While fseek to skip " + std::to_string(n) + " bytes", filename_, code);
  }
  return IOStatus::OK();
}

IOStatus ZondaFSSequentialFile::InvalidateCache(size_t offset, size_t length) {
  return IOStatus::NotSupported();
}

ZondaFSRandomAccessFile::ZondaFSRandomAccessFile(
    const std::shared_ptr<file_client::FileHandle>& handler,
    const std::string& fname) : filename_(fname), handler_(handler) {

}

ZondaFSRandomAccessFile::~ZondaFSRandomAccessFile() {
  file_client::CloseFile(handler_);
}

IOStatus ZondaFSRandomAccessFile::Read(uint64_t offset, size_t n,
                                    const IOOptions& opts, Slice* result,
                                    char* scratch, IODebugContext* dbg) const {
  comm::Ctx ctx(comm::RequestId::Next(), "random_read_file");
  IOStatus s;
  ssize_t r = -1;
  size_t left = n;
  char* ptr = scratch;
  comm::ErrorCode code;

  while (left > 0) {
    auto p = file_client::Read(&ctx, handler_, left, offset, ptr);
    code= p.first;

    if (comm::IsNotOk(code) && code != comm::ZONDA_FS_EOF) {
      r = -1;
      break;
    }

    r = p.second;
    if (r <= 0) {
      break;
    }
    ptr += r;
    left -= r;
  }
  if (r < 0) {
    // An error: return a non-ok status
    s = ZondaIOError("While pread offset " + std::to_string(offset) + " len " +
                    std::to_string(n), filename_, code);
  }
  *result = Slice(scratch, (r < 0) ? 0 : n - left);
  return s;
}

IOStatus ZondaFSRandomAccessFile::MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) {
  return FSRandomAccessFile::MultiRead(reqs, num_reqs, options, dbg);
}

IOStatus ZondaFSRandomAccessFile::Prefetch(uint64_t offset, size_t n,
                                        const IOOptions& opts,
                                        IODebugContext* dbg) {
  return IOStatus::NotSupported("Prefetch");
}

void ZondaFSRandomAccessFile::Hint(AccessPattern pattern) {
  return;
}

IOStatus ZondaFSRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
  return IOStatus::NotSupported();
}

ZondaFSWritableFile::~ZondaFSWritableFile()  {
  if (!closed_) {
    file_client::CloseFile(handler_);
  }
}

// 参照 PosixMmapFile 的 Truncate 实现, 直接返回
IOStatus ZondaFSWritableFile::Truncate(uint64_t /*size*/, const IOOptions& /*opts*/,
                                       IODebugContext* /*dbg*/)  {
  return IOStatus::OK();
}

IOStatus ZondaFSWritableFile::Close(const IOOptions &options, IODebugContext *dbg)  {
  IOStatus s;
  auto code = file_client::CloseFile(handler_);
  if (comm::IsNotOk(code)) {
    s = ZondaIOError("While closing file after writing", filename_, code);
  }
  closed_ = true;
  return s;
}

IOStatus ZondaFSWritableFile::Append(const Slice& data, const IOOptions& opts,
                                     IODebugContext* dbg)  {
  const char* src = data.data();
  size_t nbytes = data.size();

  comm::Ctx ctx(comm::RequestId::Next(), "append");
  auto code = file_client::Append(&ctx, handler_, nbytes, src);
  if (comm::IsNotOk(code)) {
    return ZondaIOError("While appending to file", filename_, code);
  }
  file_size_ += nbytes;
  return IOStatus::OK();
}

IOStatus ZondaFSWritableFile::Append(const Slice& data, const IOOptions& opts,
              const DataVerificationInfo& /* verification_info */,
              IODebugContext* dbg)  {
    return Append(data, opts, dbg);
}

IOStatus ZondaFSWritableFile::Flush(const IOOptions& opts, IODebugContext* dbg)  {
    return IOStatus::OK();
}

IOStatus ZondaFSWritableFile::Sync(const IOOptions& opts, IODebugContext* dbg)  {
    return IOStatus::OK();
}

IOStatus ZondaFSWritableFile::Fsync(const IOOptions& opts, IODebugContext* dbg)  {
    return IOStatus::OK();
}

uint64_t ZondaFSWritableFile::GetFileSize(const IOOptions& opts, IODebugContext* dbg)  {
  uint64_t file_size = 0;
  comm::Ctx ctx(comm::RequestId::Next(), "stat_file");
  auto code = file_client::StatFile(&ctx, handler_, &file_size);
  if (comm::IsNotOk(code)) {
    return ZondaIOError("while stat a file for GetFileSize", filename_, code);
  }
  return file_size;
}

IOStatus ZondaFSWritableFile::InvalidateCache(size_t offset, size_t length) {
  return IOStatus::NotSupported();
}

ZondaFSDirectory::~ZondaFSDirectory() {

}

IOStatus ZondaFSDirectory::Fsync(const IOOptions& opts, IODebugContext* dbg) {
  return FsyncWithDirOptions(opts, dbg, DirFsyncOptions());
}


IOStatus ZondaFSDirectory::Close(const IOOptions& /*opts*/,
                               IODebugContext* /*dbg*/) {
  IOStatus s = IOStatus::OK();
  return s;
}

IOStatus ZondaFSDirectory::FsyncWithDirOptions(
    const IOOptions& /*opts*/, IODebugContext* /*dbg*/,
    const DirFsyncOptions& dir_fsync_options) {
  IOStatus s = IOStatus::OK();
  return s;
}

}

