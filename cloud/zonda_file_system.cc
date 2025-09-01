#include "rocksdb/cloud/zonda_file_system.h"

#include <error_code.pb.h>
#include <utilities/transactions/lock/range/range_tree/lib/portability/toku_instrumentation.h>

#include <iostream>

#include "monitoring/iostats_context_imp.h"
#include "rocksdb/utilities/object_registry.h"
#include "zonda_file_system_io.h"

namespace ROCKSDB_NAMESPACE {

Status ZondaFileSystem::NewZondaFileSystem(
    const std::shared_ptr<FileSystem>& base_fs,
    const ZondaFileSystemOptions& options,
    ZondaFileSystem** zfs) {
  Status status;

  auto error_code = file_client::InitFileClientEnv(
      options.master_addr,
      options.cluster_id,
      options.client_id);
  if (comm::IsNotOk(error_code)) {
    return Status::InvalidArgument(comm::ErrorCode_Name(error_code));
  }

  *zfs = nullptr;
  auto fs = base_fs;
  if (!fs) {
    fs = FileSystem::Default();
  }
  std::unique_ptr<ZondaFileSystem> zonda_fs(new ZondaFileSystem(options, fs));
  *zfs = zonda_fs.release();
  return Status::OK();
}

ZondaFileSystem::ZondaFileSystem(
  const std::shared_ptr<FileSystem>& base_fs){
  base_fs_ = base_fs;
}

IOStatus ZondaFileSystem::NewSequentialFile(const std::string& fname,
                           const FileOptions& options,
                           std::unique_ptr<FSSequentialFile>* result,
                           IODebugContext* dbg)  {
  result->reset();
  IOSTATS_TIMER_GUARD(open_nanos);

  comm::Ctx ctx(comm::RequestId::Next(), "sequential_file");
  auto flag = file_client::OpenFlags::OPEN_FLAGS_RDONLY;
  auto [code, file_handle] = file_client::OpenFile(&ctx, fname, flag);
  if (comm::IsNotOk(code)) {
    CloseFile(fname);
    return ZondaIOError("While opening file for sequentially read", fname, code);
  }
  result->reset(new ZondaFSSequentialFile(file_handle, fname));
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::NewRandomAccessFile(const std::string& fname,
                             const FileOptions& options,
                             std::unique_ptr<FSRandomAccessFile>* result,
                             IODebugContext* dbg)  {
  result->reset();
  IOSTATS_TIMER_GUARD(open_nanos);

  comm::Ctx ctx(comm::RequestId::Next(), "random_file");
  auto flag = file_client::OpenFlags::OPEN_FLAGS_RDONLY;
  auto [code, file_handle] = file_client::OpenFile(&ctx, fname, flag);
  if (comm::IsNotOk(code)) {
    CloseFile(file_handle);
    return ZondaIOError("While open a file for random read", fname, code);
  }
  result->reset(new ZondaFSRandomAccessFile(file_handle, fname));
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::NewWritableFile(const std::string& fname,
                          const FileOptions& options,
                          std::unique_ptr<FSWritableFile>* result,
                          IODebugContext* dbg)  {
  result->reset();
  IOSTATS_TIMER_GUARD(open_nanos);

  IOStatus s;
  // POSIX (O_CREAT | O_TRUNC)
  auto status = FileExists(fname, IOOptions(), nullptr);
  if (status.ok()) {
    auto code = file_client::RemoveFile(fname);
    if (comm::IsNotOk(code)) {
      return ZondaIOError("While open a file for O_TRUNC", fname, code);
    }
  }
  if (!status.IsNotFound()) {
    return ZondaIOError("While open a file for O_TRUNC", fname);
  }

  comm::Ctx ctx(comm::RequestId::Next(), "open_writable_file");
  auto write = static_cast<uint32_t>(file_client::OpenFlags::OPEN_FLAGS_WRONLY);
  auto create = static_cast<uint32_t>(file_client::OpenFlags::OPEN_FLAGS_CREAT);
  auto flag = write | create;
  auto [code, file_handle] = file_client::OpenFile(&ctx, fname, flag);
  if (comm::IsNotOk(code)) {
    CloseFile(file_handle);
    return ZondaIOError("While open a file for appending", fname, code);
  }
  result->reset(new ZondaFSWritableFile(file_handle, fname));
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::ReopenWritableFile(const std::string& fname,
                            const FileOptions& options,
                            std::unique_ptr<FSWritableFile>* result,
                            IODebugContext* dbg)  {
  return IOStatus::NotSupported("ReopenWritableFile");
}

IOStatus ZondaFileSystem::ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg)  {
  result->reset();

  comm::Ctx ctx(comm::RequestId::Next(), "rename_file");
  auto errcode = file_client::RenameFile(&ctx, old_fname, fname);
  if (comm::IsNotOk(errcode)) {
    return ZondaIOError("while rename file to " + fname, old_fname, errcode);
  }
  auto flag = static_cast<uint32_t>(file_client::OpenFlags::OPEN_FLAGS_WRONLY);
  auto [code, file_handle] = file_client::OpenFile(&ctx, fname, flag);
  if (comm::IsNotOk(code)) {
    return ZondaIOError("While open a file for appending", fname, code);
  }
  result->reset(new ZondaFSWritableFile(file_handle, fname));
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::NewRandomRWFile(const std::string& fname,
                          const FileOptions& options,
                          std::unique_ptr<FSRandomRWFile>* result,
                          IODebugContext* dbg)  {
  return IOStatus::NotSupported("NewRandomRWFile");
}

IOStatus ZondaFileSystem::NewMemoryMappedFileBuffer(
                          const std::string& fname,
                          std::unique_ptr<MemoryMappedFileBuffer>* result)  {
  return IOStatus::NotSupported("NewMemoryMappedFileBuffer");
}

IOStatus ZondaFileSystem::NewDirectory(const std::string& name,
                      const IOOptions& opts,
                      std::unique_ptr<FSDirectory>* result,
                      IODebugContext* dbg)  {
  result->reset(new ZondaFSDirectory());
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::FileExists(const std::string& fname,
                    const IOOptions& opts,
                    IODebugContext* dbg)  {
  file_client::FileStat file_stat;
  comm::Ctx ctx(comm::RequestId::Next(), "file_exists");
  auto code = file_client::StatFile(&ctx, fname, &file_stat);
  if (comm::IsNotOk(code)) {
    return ZondaIOError("file exist ", fname, code);
  }
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::GetChildren(const std::string& dir,
                      const IOOptions& opts,
                     std::vector<std::string>* result,
                     IODebugContext* dbg)  {
  file_client::DirStat dir_stat;
  comm::Ctx ctx(comm::RequestId::Next(), "dir stat");
  auto code = StatDir(&ctx, dir, &dir_stat);
  if (comm::IsNotOk(code)) {
    return ZondaIOError("While opendir", dir, code);
  }
  for (const auto sub : dir_stat.children) {
    result->emplace_back(sub);
  }
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::DeleteFile(const std::string& fname,
                    const IOOptions& opts,
                    IODebugContext* dbg)  {
  comm::Ctx ctx(comm::RequestId::Next(), "delete_file");
  auto code = file_client::RemoveFile(&ctx, fname.c_str());
  if (comm::IsNotOk(code)) {
    return ZondaIOError("while unlink() file", fname, code);
  }
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::CreateDir(const std::string& name,
                  const IOOptions& opts,
                  IODebugContext* dbg)  {
  auto flag = static_cast<uint32_t>(file_client::CreateFlags::CREATE_FLAGS_NONE);
  comm::Ctx ctx(comm::RequestId::Next(), "create_dir");
  auto code = file_client::CreateDir(&ctx, name, flag);
  if (comm::IsNotOk(code)) {
    return ZondaIOError("While mkdir", name, code);
  }
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::CreateDirIfMissing(const std::string& name,
                            const IOOptions& opts,
                            IODebugContext* dbg)  {
  file_client::DirStat dir_stat;
  comm::Ctx ctx(comm::RequestId::Next(), "mkdir_if_missing");
  auto code = StatDir(&ctx, name, &dir_stat);
  if (code == comm::ZONDA_OK) {
    return IOStatus::OK();
  }

  auto flag = static_cast<uint32_t>(file_client::CreateFlags::CREATE_FLAGS_PARENTS);
  comm::Ctx create_ctx(comm::RequestId::Next(), "create_dir");
  auto code = file_client::CreateDir(&create_ctx, name, flag);
  if (comm::IsNotOk(code)) {
    return ZondaIOError("While mkdir if missing", name, code);
  }
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::DeleteDir(const std::string& name,
                  const IOOptions& opts,
                  IODebugContext* dbg)  {
  comm::Ctx ctx(comm::RequestId::Next(), "delete_dir");
  auto code = file_client::RemoveDir(&ctx, name);
  if (comm::IsNotOk(code)) {
    return ZondaIOError("While mkdir", name, code);
  }
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::GetFileSize(const std::string& fname,
                     const IOOptions& opts,
                     uint64_t* size, IODebugContext* dbg)  {
  file_client::FileStat file_stat;
  comm::Ctx ctx(comm::RequestId::Next(), "stat_file");
  auto code = file_client::StatFile(&ctx, fname, &file_stat);
  if (comm::IsNotOk(code)) {
    return ZondaIOError("while stat a file for size", fname, code);
  }
  *size = file_stat.size;
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::GetFileModificationTime(const std::string& fname,
                                 const IOOptions& opts,
                                 uint64_t* file_mtime,
                                 IODebugContext* dbg)  {
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::RenameFile(const std::string& src,
                    const std::string& target,
                    const IOOptions& opts,
                    IODebugContext* dbg)  {
  comm::Ctx ctx(comm::RequestId::Next(), "rename_file");
  auto code = file_client::RenameFile(&ctx, src, target);
  if (comm::IsNotOk(code)) {
    return ZondaIOError("While renaming a file to " + target, src, code);
  }
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::LinkFile(const std::string& src,
                  const std::string& target,
                  const IOOptions& opts,
                  IODebugContext* dbg)  {

  comm::Ctx ctx(comm::RequestId::Next(), "hardlink_file");
  auto code = file_client::HardLinkFile(&ctx, src, target);
  if (comm::IsNotOk(code)) {
    return ZondaIOError("while link file to " + target, src, code);
  }
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::NumFileLinks(const std::string& fname,
                      const IOOptions& opts,
                      uint64_t* count, IODebugContext* dbg)  {
  file_client::FileStat file_stat;
  comm::Ctx ctx(comm::RequestId::Next(), "stat_file");
  auto code = file_client::StatFile(&ctx, fname, &file_stat);
  if (comm::IsNotOk(code)) {
    return ZondaIOError("while stat a file for num file links", fname, code);
  }
  *count = file_stat.nlink;
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::AreFilesSame(const std::string& first,
                      const std::string& second,
                      const IOOptions& opts, bool* res,
                      IODebugContext* dbg)  {
  return IOStatus::NotSupported("AreFilesSame is not supported");
}

IOStatus ZondaFileSystem::LockFile(const std::string& fname,
                  const IOOptions& opts,
                  FileLock** lock, IODebugContext* dbg)  {
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::UnlockFile(FileLock* lock, const IOOptions& opts,
                    IODebugContext* dbg)  {
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::GetAbsolutePath(const std::string& db_path,
                         const IOOptions& opts, std::string* output_path,
                         IODebugContext* dbg)  {
  if (!db_path.empty() && db_path[0] == '/') {
    *output_path = db_path;
    return IOStatus::OK();
  }
  return IOStatus::OK();
}

IOStatus ZondaFileSystem::GetTestDirectory(const IOOptions& opts,
                          std::string* result,
                          IODebugContext* dbg)  {
  char buf[100];
  snprintf(buf, sizeof(buf), "/tmp/rocksdbtest-%d", int(geteuid()));
  *result = buf;
  IOOptions opts;
  return CreateDirIfMissing(*result, opts, nullptr);
}

IOStatus ZondaFileSystem::GetFreeSpace(const std::string& fname,
                      const IOOptions& opts,
                      uint64_t* free_space,
                      IODebugContext* dbg)  {
  return IOStatus::NotSupported("GetFreeSpace");
}

IOStatus ZondaFileSystem::IsDirectory(const std::string& path,
                     const IOOptions& opts,
                     bool* is_dir, IODebugContext* dbg)  {
  file_client::DirStat dir_stat;
  comm::Ctx ctx(comm::RequestId::Next(), "dir stat");
  auto code = StatDir(&ctx, path, &dir_stat);
  if (comm::IsNotOk(code)) {
    return ZondaIOError("While opendir", path, code);
  }
  return IOStatus::OK();
}

#ifdef OS_LINUX
Status ZondaFileSystem::RegisterDbPaths(const std::vector<std::string>& paths)  {
  return Status::OK();
}
Status ZondaFileSystem::UnregisterDbPaths(const std::vector<std::string>& paths)  {
  return Status::OK();
}
#endif

IOStatus ZondaFileSystem::Poll(std::vector<void*>& io_handles,
              size_t min_completions)  {
  return IOStatus::NotSupported("Poll");
}

IOStatus ZondaFileSystem::AbortIO(std::vector<void*>& io_handles)  {
  return IOStatus::OK();
}

void ZondaFileSystem::SupportedOps(int64_t& supported_ops)  {
  supported_ops = 0;
}

IOStatus ZondaFileSystem::NewLogger(const std::string& fname, const IOOptions& io_opts,
                             std::shared_ptr<Logger>* result,
                             IODebugContext* dbg) {
  return base_fs_->NewLogger(fname, io_opts, result, dbg);
}

static FactoryFunc<FileSystem> zonda_filesystem_reg =
    ObjectLibrary::Default()->AddFactory<FileSystem>(
        ObjectLibrary::PatternEntry("zonda").AddSeparator("://", false),
        [](const std::string& /* uri */, std::unique_ptr<FileSystem>* f,
           std::string* /* errmsg */) {
          f->reset(new ZondaFileSystem(FileSystem::Default()));
          return f->get();
        });

}
