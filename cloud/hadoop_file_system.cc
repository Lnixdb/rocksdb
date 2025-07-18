#include "rocksdb/cloud/hadoop_file_system.h"

#include <options/db_options.h>
#include <unistd.h>

#include <cstdio>
#include <iostream>
#include <string>
#include <thread>

#include "cloud/metrics.h"
#include "hadoop_io.h"
#include "rocksdb/utilities/object_registry.h"
#include "monitoring/iostats_context_imp.h"

namespace ROCKSDB_NAMESPACE {


IOStatus connectHDFS(const std::string& nn_uri, const std::string& user,
                     hdfsFS& result) {
  hdfsBuilder* builder = hdfsNewBuilder();
  hdfsBuilderSetNameNode(builder, nn_uri.c_str());
  hdfsBuilderSetUserName(builder, user.c_str());
  auto conn = hdfsBuilderConnect(builder);
  if (!conn) {
    hdfsFreeBuilder(builder);
    return IOStatus::IOError(hdfsGetLastError());
  }
  result = conn;
  return IOStatus::OK();
}

HadoopFileSystem::HadoopFileSystem(
  const std::shared_ptr<FileSystem>& base_fs){
  base_fs_ = base_fs;
}

 HadoopFileSystem::HadoopFileSystem(const std::string& nn_uri,
                                   const std::string& user) :
    hdfs_user_(user),nn_uri_(nn_uri) {
  hdfsFS conn = nullptr;
  if (auto s = connectHDFS(nn_uri_, hdfs_user_, conn); !s.ok()) {
    std::exit(-1);
  }
  conn_ = conn;
}

IOStatus HadoopFileSystem::NewSequentialFile(const std::string& fname,
                           const FileOptions& options,
                           std::unique_ptr<FSSequentialFile>* result,
                           IODebugContext* dbg)  {
  result->reset();
  IOSTATS_TIMER_GUARD(open_nanos);

  // 打开文件
  hdfsFile file = hdfsOpenFile(conn_, fname.c_str(), O_RDONLY, 0, 0, 0);
  if (!file) {
    errLog("NewSequentialFile::open", fname);
    return IOStatus::IOError(hdfsGetLastError());
  }
  result->reset(new HDFSSequentialFile(conn_, file, fname));
  return IOStatus::OK();
}

IOStatus HadoopFileSystem::NewRandomAccessFile(const std::string& fname,
                             const FileOptions& options,
                             std::unique_ptr<FSRandomAccessFile>* result,
                             IODebugContext* dbg)  {
  result->reset();
  IOSTATS_TIMER_GUARD(open_nanos);
  // 打开文件
  hdfsFile file = hdfsOpenFile(conn_, fname.c_str(), O_RDONLY, 0, 0, 0);
  if (!file) {
    errLog("NewRandomAccessFile::open", fname);
    return IOStatus::IOError(hdfsGetLastError());
  }
  result->reset(new HDFSRandomAccessFile(conn_, file, fname));
  return IOStatus::OK();
}

IOStatus HadoopFileSystem::NewWritableFile(const std::string& fname,
                          const FileOptions& options,
                          std::unique_ptr<FSWritableFile>* result,
                          IODebugContext* dbg)  {
  result->reset();
  IOSTATS_TIMER_GUARD(open_nanos);
  if (hdfsExists(conn_,fname.c_str()) == 0) {
    if (hdfsDelete(conn_, fname.c_str(), 0) != 0) {
      errLog("NewWritableFile::delete", fname);
      return IOStatus::IOError(hdfsGetLastError());
    }
  }

  // 打开文件
  auto file = hdfsOpenFile(conn_, fname.c_str(), O_CREAT | O_WRONLY , 0, 0, 0);
  if (!file) {
    errLog("NewWritableFile::open", fname);
    return IOStatus::IOError(hdfsGetLastError());
  }
  result->reset(new HDFSWritableFile(conn_, file, fname));
  return IOStatus::OK();
}

IOStatus HadoopFileSystem::ReopenWritableFile(const std::string& fname,
                            const FileOptions& options,
                            std::unique_ptr<FSWritableFile>* result,
                            IODebugContext* dbg)  {
  return IOStatus::NotSupported("ReopenWritableFile");
}

IOStatus HadoopFileSystem::ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg)  {
  result->reset();
  if (hdfsRename(conn_, old_fname.c_str(), fname.c_str()) == -1) {
    errLog("ReuseWritableFile::rename", fname + "-" + old_fname);
    return IOStatus::IOError(hdfsGetLastError());
  }
  hdfsFile file = hdfsOpenFile(conn_, fname.c_str(), O_WRONLY ,0,0,0);
  if (!file) {
    errLog("ReuseWritableFile::open", fname);
    return IOStatus::IOError(hdfsGetLastError());
  }
  result->reset(new HDFSWritableFile(conn_, file, fname));
  return IOStatus::OK();
}

IOStatus HadoopFileSystem::NewRandomRWFile(const std::string& fname,
                          const FileOptions& options,
                          std::unique_ptr<FSRandomRWFile>* result,
                          IODebugContext* dbg)  {
  return IOStatus::NotSupported("NewRandomRWFile");
}

IOStatus HadoopFileSystem::NewMemoryMappedFileBuffer(
                          const std::string& fname,
                          std::unique_ptr<MemoryMappedFileBuffer>* result)  {
  return IOStatus::NotSupported("NewMemoryMappedFileBuffer");
}

IOStatus HadoopFileSystem::NewDirectory(const std::string& name, const IOOptions& /*opts*/,
                      std::unique_ptr<FSDirectory>* result,
                      IODebugContext* /*dbg*/) {
  result->reset(new HDFSDirectory());
  return IOStatus::OK();
}

IOStatus HadoopFileSystem::FileExists(const std::string& fname,
                    const IOOptions& opts,
                    IODebugContext* dbg)  {
  if (hdfsExists(conn_,fname.c_str()) == 0) {
    return IOStatus::OK();
  }
  return IOStatus::NotFound("FileExists");
}

IOStatus HadoopFileSystem::GetChildren(const std::string& dir,
                      const IOOptions& opts,
                     std::vector<std::string>* result,
                     IODebugContext* dbg)  {
  result->clear();
  int num = 0;
  auto entries = hdfsListDirectory(conn_, dir.c_str(), &num);
  if (!entries) {
    if (strstr(hdfsGetLastError(), "NotFoundException")) {
      return IOStatus::NotFound();
    }
    return IOStatus::IOError(hdfsGetLastError());
  }

  for (int i = 0; i < num; ++i) {
    auto entry = entries[i];
    if (entry.mKind == kObjectKindDirectory &&
    (strcmp(entry.mName, ".") == 0 || strcmp(entry.mName, "..") == 0)) {
      continue;
    }
    const char* fullPath = entry.mName;
    const char* baseName = strrchr(fullPath, '/');
    if (baseName && *(baseName + 1) != '\0') {
      result->emplace_back(baseName + 1);
    }
  }
  hdfsFreeFileInfo(entries, num);
  return IOStatus::OK();
}

IOStatus HadoopFileSystem::DeleteFile(const std::string& fname,
                    const IOOptions& opts,
                    IODebugContext* dbg)  {
  if (hdfsDelete(conn_, fname.c_str(), 0) == -1) {
    if (strstr(hdfsGetLastError(), "NotFoundException")) {
      return IOStatus::OK();
    }
    errLog("HadoopFileSystem::DeleteFile", fname);
    return IOStatus::IOError(hdfsGetLastError());
  }
  return IOStatus::OK();
}

IOStatus HadoopFileSystem::CreateDir(const std::string& name,
                  const IOOptions& opts,
                  IODebugContext* dbg)  {
  if (hdfsCreateDirectory(conn_, name.c_str()) == -1) {
    errLog("HadoopFileSystem::CreateDir", name);
    return IOStatus::IOError(hdfsGetLastError());
  }
  return IOStatus::OK();
}

IOStatus HadoopFileSystem::CreateDirIfMissing(const std::string& name,
                            const IOOptions& opts,
                            IODebugContext* dbg)  {
  if (hdfsExists(conn_, name.c_str()) == 0) {
    return IOStatus::OK();
  }
  if (hdfsCreateDirectory(conn_, name.c_str()) != 0) {
    errLog("HadoopFileSystem::CreateDirIfMissing", name);
    return IOStatus::IOError(hdfsGetLastError());
  }
  return IOStatus::OK();
}

IOStatus HadoopFileSystem::DeleteDir(const std::string& name,
                  const IOOptions& opts,
                  IODebugContext* dbg)  {
  if (hdfsDelete(conn_, name.c_str(), 1) != 0) {
    errLog("HadoopFileSystem::DeleteDir", name);
    return IOStatus::IOError(hdfsGetLastError());
  }
  return IOStatus::OK();
}

IOStatus HadoopFileSystem::GetFileSize(const std::string& fname,
                     const IOOptions& opts,
                     uint64_t* size, IODebugContext* dbg)  {
  hdfsFileInfo* info = hdfsGetPathInfo(conn_, fname.c_str());
  if (!info) {
    errLog("HadoopFileSystem::GetFileSize", fname);
    return IOStatus::IOError(hdfsGetLastError());
  }
  *size = info->mSize;
  hdfsFreeFileInfo(info, 1);
  return IOStatus::OK();
}

IOStatus HadoopFileSystem::GetFileModificationTime(const std::string& fname,
                                 const IOOptions& opts,
                                 uint64_t* file_mtime,
                                 IODebugContext* dbg)  {
  hdfsFileInfo* info = hdfsGetPathInfo(conn_, fname.c_str());
  if (!info) {
    errLog("HadoopFileSystem::GetFileModificationTime", fname);
    return IOStatus::IOError(hdfsGetLastError());
  }
  *file_mtime = info->mLastMod / 1000;
  hdfsFreeFileInfo(info, 1);
  return IOStatus::OK();
}

IOStatus HadoopFileSystem::RenameFile(const std::string& src,
                    const std::string& target,
                    const IOOptions& opts,
                    IODebugContext* dbg)  {
  // HDFS  不支持目标文件覆盖, 需要先删除
  if (hdfsExists(conn_, target.c_str()) == 0) {
    hdfsDelete(conn_, target.c_str(), 0);
  }

  if(hdfsRename(conn_, src.c_str(), target.c_str()) != 0) {
    errLog("HadoopFileSystem::RenameFile", src + "-" + target);
    return IOStatus::IOError(hdfsGetLastError());
  }
  return IOStatus::OK();
}

IOStatus HadoopFileSystem::LinkFile(const std::string& src,
                  const std::string& target,
                  const IOOptions& opts,
                  IODebugContext* dbg)  {
  return IOStatus::NotSupported("LinkFile is not supported for HDFS");
}

IOStatus HadoopFileSystem::NumFileLinks(const std::string& fname,
                      const IOOptions& opts,
                      uint64_t* count, IODebugContext* dbg)  {
  return IOStatus::NotSupported("NumFileLinks is not supported for HDFS");
}

IOStatus HadoopFileSystem::AreFilesSame(const std::string& first,
                      const std::string& second,
                      const IOOptions& opts, bool* res,
                      IODebugContext* dbg)  {
  return IOStatus::NotSupported("AreFilesSame is not supported for HDFS");
}

IOStatus HadoopFileSystem::LockFile(const std::string& fname,
                  const IOOptions& opts,
                  FileLock** lock, IODebugContext* dbg)  {
 return IOStatus::OK();
}

IOStatus HadoopFileSystem::UnlockFile(FileLock* lock, const IOOptions& opts,
                    IODebugContext* dbg)  {
  return IOStatus::OK();
}

IOStatus HadoopFileSystem::GetAbsolutePath(const std::string& db_path,
                         const IOOptions& opts, std::string* output_path,
                         IODebugContext* dbg)  {
  if (!db_path.empty() && db_path[0] == '/') {
    *output_path = db_path;
    return IOStatus::OK();
  }
  hdfsFileInfo* info = hdfsGetPathInfo(conn_, db_path.c_str());
  if (!info) {
    errLog("HadoopFileSystem::GetAbsolutePath", db_path);
    return IOStatus::IOError(hdfsGetLastError());
  }
  *output_path = info->mName;
  hdfsFreeFileInfo(info, 1);
  return IOStatus::OK();
}

IOStatus HadoopFileSystem::GetTestDirectory(const IOOptions& /*opts*/,
                          std::string* result,
                          IODebugContext* dbg)  {
  char buf[100];
  snprintf(buf, sizeof(buf), "/tmp/rocksdbtest-%d", int(geteuid()));
  *result = buf;
  IOOptions opts;

  return CreateDirIfMissing(*result, opts, nullptr);
}

IOStatus HadoopFileSystem::GetFreeSpace(const std::string& fname,
                      const IOOptions& opts,
                      uint64_t* free_space,
                      IODebugContext* dbg)  {
  return IOStatus::NotSupported("GetFreeSpace");
}

IOStatus HadoopFileSystem::IsDirectory(const std::string& path,
                     const IOOptions& opts,
                     bool* is_dir, IODebugContext* dbg)  {
  hdfsFileInfo* info = hdfsGetPathInfo(conn_, path.c_str());
  if (!info) {
    errLog("HadoopFileSystem::IsDirectory", path);
    return IOStatus::IOError(hdfsGetLastError());
  }

  *is_dir = (info->mKind == kObjectKindDirectory);
  hdfsFreeFileInfo(info, 1);
  return IOStatus::OK();
}

// Default implementation returns the copy of the same object.
FileOptions HadoopFileSystem::OptimizeForLogWrite(
    const FileOptions& file_options, const DBOptions& db_options) const {
  FileOptions optimized_file_options(file_options);
  optimized_file_options.bytes_per_sync = db_options.wal_bytes_per_sync;
  optimized_file_options.writable_file_max_buffer_size =
      db_options.writable_file_max_buffer_size;
  return optimized_file_options;
}

FileOptions HadoopFileSystem::OptimizeForCompactionTableWrite(
    const FileOptions& file_options,
    const ImmutableDBOptions& db_options) const {
  FileOptions optimized_file_options(file_options);
  optimized_file_options.use_direct_writes =
      db_options.use_direct_io_for_flush_and_compaction;
  return optimized_file_options;
}

FileOptions HadoopFileSystem::OptimizeForCompactionTableRead(
    const FileOptions& file_options,
    const ImmutableDBOptions& db_options) const {
  FileOptions optimized_file_options(file_options);
  optimized_file_options.use_direct_reads = db_options.use_direct_reads;
  return optimized_file_options;
}

FileOptions HadoopFileSystem::OptimizeForManifestWrite(
    const FileOptions& file_options) const {
  return file_options;
}

#ifdef OS_LINUX
Status HadoopFileSystem::RegisterDbPaths(const std::vector<std::string>& paths)  {
  return Status::OK();
}
Status HadoopFileSystem::UnregisterDbPaths(const std::vector<std::string>& paths)  {
  return Status::OK();
}
#endif

IOStatus HadoopFileSystem::Poll(std::vector<void*>& io_handles,
              size_t min_completions)  {
  return IOStatus::NotSupported("Poll");
}

IOStatus HadoopFileSystem::AbortIO(std::vector<void*>& io_handles)  {
  return IOStatus::OK();
}

void HadoopFileSystem::SupportedOps(int64_t& supported_ops)  {
  supported_ops = 0;
  return;
}

static FactoryFunc<FileSystem> hadoop_filesystem_reg =
    ObjectLibrary::Default()->AddFactory<FileSystem>(
        ObjectLibrary::PatternEntry("hdfs").AddSeparator("://", false),
        [](const std::string& /* uri */, std::unique_ptr<FileSystem>* f,
           std::string* /* errmsg */) {
          f->reset(new HadoopFileSystem(hdfs_benchmark_nn_uri, hdfs_benchmark_user));
          return f->get();
        });

}
