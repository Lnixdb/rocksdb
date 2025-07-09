#include <iostream>

#include "rocksdb/cloud/zonda_file_system.h"
#include "cloud/metrics.h"
#include "rocksdb/utilities/object_registry.h"

namespace ROCKSDB_NAMESPACE {


ZondaFileSystem::ZondaFileSystem(
  const std::shared_ptr<FileSystem>& base_fs){
  base_fs_ = base_fs;
}

IOStatus ZondaFileSystem::NewSequentialFile(const std::string& fname,
                           const FileOptions& options,
                           std::unique_ptr<FSSequentialFile>* result,
                           IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("new_sequential").Increment();
  return base_fs_->NewSequentialFile(fname, options, result, dbg);
}

IOStatus ZondaFileSystem::NewRandomAccessFile(const std::string& fname,
                             const FileOptions& options,
                             std::unique_ptr<FSRandomAccessFile>* result,
                             IODebugContext* dbg)  {

  ZondaFSMetrics::Instance().GetMethodCounter("random_access").Increment();
  return base_fs_->NewRandomAccessFile(fname, options, result, dbg);
}


IOStatus ZondaFileSystem::NewWritableFile(const std::string& fname,
                          const FileOptions& options,
                          std::unique_ptr<FSWritableFile>* result,
                          IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("writable_file").Increment();
  return base_fs_->NewWritableFile(fname, options, result, dbg);
}

IOStatus ZondaFileSystem::ReopenWritableFile(const std::string& fname,
                            const FileOptions& options,
                            std::unique_ptr<FSWritableFile>* result,
                            IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("reopen_file").Increment();
  return base_fs_->ReopenWritableFile(fname, options, result, dbg);
}

IOStatus ZondaFileSystem::ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("reuse_file").Increment();
  return base_fs_->ReuseWritableFile(fname, old_fname, options, result, dbg);
}

IOStatus ZondaFileSystem::NewRandomRWFile(const std::string& fname,
                          const FileOptions& options,
                          std::unique_ptr<FSRandomRWFile>* result,
                          IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("random_rw").Increment();
  return base_fs_->NewRandomRWFile(fname, options, result, dbg);
}

IOStatus ZondaFileSystem::NewMemoryMappedFileBuffer(
                          const std::string& fname,
                          std::unique_ptr<MemoryMappedFileBuffer>* result)  {
  ZondaFSMetrics::Instance().GetMethodCounter("mapped_file").Increment();
  return base_fs_->NewMemoryMappedFileBuffer(fname, result);
}

IOStatus ZondaFileSystem::NewDirectory(const std::string& name,
                      const IOOptions& opts,
                      std::unique_ptr<FSDirectory>* result,
                      IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("new_directory").Increment();
  return base_fs_->NewDirectory(name, opts, result, dbg);
}

IOStatus ZondaFileSystem::FileExists(const std::string& fname,
                    const IOOptions& opts,
                    IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("file_exists").Increment();
  return base_fs_->FileExists(fname,  opts, dbg);
}

IOStatus ZondaFileSystem::GetChildren(const std::string& dir,
                      const IOOptions& opts,
                     std::vector<std::string>* result,
                     IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("get_children").Increment();
  return  base_fs_->GetChildren(dir, opts, result, dbg);
}

IOStatus ZondaFileSystem::DeleteFile(const std::string& fname,
                    const IOOptions& opts,
                    IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("delete_file").Increment();
 return base_fs_->DeleteFile(fname, opts, dbg);
}

IOStatus ZondaFileSystem::CreateDir(const std::string& name,
                  const IOOptions& opts,
                  IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("create_dir").Increment();
  return base_fs_->CreateDir(name, opts, dbg);
}

IOStatus ZondaFileSystem::CreateDirIfMissing(const std::string& name,
                            const IOOptions& opts,
                            IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter(
    "create_dir_if_missing").Increment();
  return base_fs_->CreateDirIfMissing(name, opts, dbg);
}

IOStatus ZondaFileSystem::DeleteDir(const std::string& name,
                  const IOOptions& opts,
                  IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("delete_dir").Increment();
  return base_fs_->DeleteDir(name, opts, dbg);
}

IOStatus ZondaFileSystem::GetFileSize(const std::string& fname,
                     const IOOptions& opts,
                     uint64_t* size, IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("get_file_size").Increment();
  return base_fs_->GetFileSize(fname, opts, size, dbg);
}

IOStatus ZondaFileSystem::GetFileModificationTime(const std::string& fname,
                                 const IOOptions& opts,
                                 uint64_t* file_mtime,
                                 IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter(
    "modification_time").Increment();
  return base_fs_->GetFileModificationTime(fname, opts, file_mtime, dbg);
}

IOStatus ZondaFileSystem::RenameFile(const std::string& src,
                    const std::string& target,
                    const IOOptions& opts,
                    IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("rename_file").Increment();
  return base_fs_->RenameFile(src, target, opts, dbg);
}

IOStatus ZondaFileSystem::LinkFile(const std::string& src,
                  const std::string& target,
                  const IOOptions& opts,
                  IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("link_file").Increment();
  return base_fs_->LinkFile(src, target, opts, dbg);
}

IOStatus ZondaFileSystem::NumFileLinks(const std::string& fname,
                      const IOOptions& opts,
                      uint64_t* count, IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("num_link_file").Increment();
  return base_fs_->NumFileLinks(fname, opts, count, dbg);
}

IOStatus ZondaFileSystem::AreFilesSame(const std::string& first,
                      const std::string& second,
                      const IOOptions& opts, bool* res,
                      IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("are_file_same").Increment();
  return base_fs_->AreFilesSame(first, second, opts, res, dbg);
}

IOStatus ZondaFileSystem::LockFile(const std::string& fname,
                  const IOOptions& opts,
                  FileLock** lock, IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("lock_file").Increment();
  return base_fs_->LockFile(fname, opts, lock, dbg);
}

IOStatus ZondaFileSystem::UnlockFile(FileLock* lock, const IOOptions& opts,
                    IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("unlock_file").Increment();
  return base_fs_->UnlockFile(lock, opts, dbg);
}

IOStatus ZondaFileSystem::GetAbsolutePath(const std::string& db_path,
                         const IOOptions& opts, std::string* output_path,
                         IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter(
    "get_absolute_path").Increment();
  return base_fs_->GetAbsolutePath(db_path, opts, output_path, dbg);
}

IOStatus ZondaFileSystem::GetTestDirectory(const IOOptions& opts,
                          std::string* result,
                          IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("get_test_directory").Increment();
  return base_fs_->GetTestDirectory(opts, result, dbg);
}

IOStatus ZondaFileSystem::GetFreeSpace(const std::string& fname,
                      const IOOptions& opts,
                      uint64_t* free_space,
                      IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("get_free_space").Increment();
  return base_fs_->GetFreeSpace(fname, opts, free_space, dbg);
}

IOStatus ZondaFileSystem::IsDirectory(const std::string& path,
                     const IOOptions& opts,
                     bool* is_dir, IODebugContext* dbg)  {
  ZondaFSMetrics::Instance().GetMethodCounter("is_directory").Increment();
 return base_fs_->IsDirectory(path, opts, is_dir, dbg);
}

FileOptions ZondaFileSystem::OptimizeForLogWrite(const FileOptions& file_options,
                                const DBOptions& db_options) const  {
  ZondaFSMetrics::Instance().
  GetMethodCounter("optimize_for_log_write").Increment();
  return base_fs_->OptimizeForLogWrite(file_options, db_options);
}

FileOptions ZondaFileSystem::OptimizeForManifestWrite(
    const FileOptions& file_options) const  {
  ZondaFSMetrics::Instance().
  GetMethodCounter("optimize_for_manifest").Increment();
 return base_fs_->OptimizeForManifestWrite(file_options);
}

FileOptions ZondaFileSystem::OptimizeForCompactionTableRead(
    const FileOptions& file_options,
    const ImmutableDBOptions& db_options) const  {
  ZondaFSMetrics::Instance().
  GetMethodCounter("optimize_for_compaction").Increment();
 return base_fs_->OptimizeForCompactionTableRead(file_options, db_options);
}

#ifdef OS_LINUX
Status ZondaFileSystem::RegisterDbPaths(const std::vector<std::string>& paths)  {
  return base_fs_->RegisterDbPaths(paths);
}
Status ZondaFileSystem::UnregisterDbPaths(const std::vector<std::string>& paths)  {
  return base_fs_->UnregisterDbPaths(paths);
}
#endif

IOStatus ZondaFileSystem::Poll(std::vector<void*>& io_handles,
              size_t min_completions)  {
  ZondaFSMetrics::Instance().GetMethodCounter("poll").Increment();
  return base_fs_->Poll(io_handles, min_completions);
}

IOStatus ZondaFileSystem::AbortIO(std::vector<void*>& io_handles)  {
  ZondaFSMetrics::Instance().GetMethodCounter("abort_io").Increment();
  return base_fs_->AbortIO(io_handles);
}

void ZondaFileSystem::SupportedOps(int64_t& supported_ops)  {
  ZondaFSMetrics::Instance().GetMethodCounter("supported_ops").Increment();
  return base_fs_->SupportedOps(supported_ops);
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
