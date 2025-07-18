#include "hadoop_io.h"

namespace ROCKSDB_NAMESPACE {

void errLog(const std::string& func, const std::string& fname) {
  std::cout << func << " error: " <<  hdfsGetLastError() << " fname:" << fname << std::endl;
}

void okLog(const std::string& func, const std::string& fname) {
  std::cout << func << " ok: " <<  hdfsGetLastError() << " fname:" << fname << std::endl;
};

HDFSSequentialFile::HDFSSequentialFile(hdfsFS conn,  hdfsFile fd,
  const std::string &fname) : filename_(fname), nn_conn_(conn), fd_(fd) {

}

HDFSSequentialFile::~HDFSSequentialFile() {
  if (nn_conn_ && fd_) {
    hdfsCloseFile(nn_conn_, fd_);
    fd_ = nullptr;
    nn_conn_ = nullptr;
  }
}

IOStatus HDFSSequentialFile::Read(size_t n, const IOOptions& opts,
                                  Slice* result, char* scratch,
                                  IODebugContext* dbg) {
  assert(result != nullptr && !use_direct_io());
  tSize r = 0;
  do {
    r = hdfsRead(nn_conn_, fd_, scratch, n);
  } while (r == -1 && errno == EINTR);
  if (r == -1) {
    errLog("HDFSSequentialFile::Read", filename_);
    return IOStatus::IOError(hdfsGetLastError(),filename_ +  " sequential read");
  }
  *result = Slice(scratch, r);
  return IOStatus::OK();
}

IOStatus HDFSSequentialFile::PositionedRead(uint64_t offset, size_t n,
                                            const IOOptions& opts,
                                            Slice* result, char* scratch,
                                            IODebugContext* dbg) {
  return IOStatus::NotSupported("PositionedRead");
}

IOStatus HDFSSequentialFile::Skip(uint64_t n) {
  auto seek_curr = hdfsTell(nn_conn_, fd_);
  if (seek_curr == -1) {
    errLog("HDFSSequentialFile::Skip", filename_);
    return IOStatus::IOError(hdfsGetLastError(),
         filename_ +  " sequential tell");
  }
  if (hdfsSeek(nn_conn_, fd_, seek_curr + n) == -1) {
    errLog("HDFSSequentialFile::Seek", filename_);
    return IOStatus::IOError(hdfsGetLastError(),
       filename_ +  " sequential skip");
  }
  return IOStatus::OK();
}

IOStatus HDFSSequentialFile::InvalidateCache(size_t offset, size_t length) {
  return IOStatus::NotSupported();
}

HDFSRandomAccessFile::~HDFSRandomAccessFile() {
  if (nn_conn_ && fd_) {
    hdfsCloseFile(nn_conn_, fd_);
    fd_ = nullptr;
    nn_conn_ = nullptr;
  }
}

IOStatus HDFSRandomAccessFile::Read(uint64_t offset, size_t n,
                                    const IOOptions& opts, Slice* result,
                                    char* scratch, IODebugContext* dbg) const {
  std::lock_guard<std::mutex> lock(read_mutex_);
  IOStatus s;
  if (hdfsSeek(nn_conn_, fd_, offset) == -1) {
    errLog("HDFSRandomAccessFile::Seek", filename_);
    return IOStatus::IOError(hdfsGetLastError(),
       filename_ +  " sequential seek");
  }
  ssize_t r = -1;
  size_t left = n;
  char* ptr = scratch;
  while (left > 0) {
    r = hdfsRead(nn_conn_, fd_, ptr, left);
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ptr += r;
    left -= r;
  }
  if (r < 0) {
    errLog("HDFSRandomAccessFile::read", filename_);
    s = IOStatus::IOError(hdfsGetLastError(),
       filename_ +  " sequential read");
  }
  *result = Slice(scratch, (r < 0) ? 0 : n - left);
  return s;
}

IOStatus HDFSRandomAccessFile::MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) {
  return FSRandomAccessFile::MultiRead(reqs, num_reqs, options, dbg);
}

IOStatus HDFSRandomAccessFile::Prefetch(uint64_t offset, size_t n,
                                        const IOOptions& opts,
                                        IODebugContext* dbg) {
  return IOStatus::NotSupported("Prefetch");
}

void HDFSRandomAccessFile::Hint(AccessPattern pattern) {
  return;
}

IOStatus HDFSRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
  return IOStatus::NotSupported();
}

HDFSWritableFile::~HDFSWritableFile()  {
    //IOStatus s = HDFSWritableFile::Close(IOOptions(), nullptr);
}

// 参照 PosixMmapFile 的 Truncate 实现, 直接返回
IOStatus HDFSWritableFile::Truncate(uint64_t /*size*/, const IOOptions& /*opts*/,
                    IODebugContext* /*dbg*/)  {
  return IOStatus::OK();
}

IOStatus HDFSWritableFile::Close(const IOOptions &options, IODebugContext *dbg)  {
    if (fd_ && nn_conn_) {
        if(hdfsCloseFile(nn_conn_, fd_) != 0) {
            return IOStatus::IOError(hdfsGetLastError());
        }
      fd_ = nullptr;
      nn_conn_ = nullptr;
    }
    return IOStatus::OK();
}

IOStatus HDFSWritableFile::Append(const Slice& data, const IOOptions& opts,
                  IODebugContext* dbg)  {
    const char* src = data.data();
    size_t nbytes = data.size();
    tSize bytes_written = hdfsWrite(nn_conn_, fd_, src, nbytes);
    if (bytes_written == -1) {
      errLog("HDFSWritableFile::write", filename_);
        return IOStatus::IOError(hdfsGetLastError());
    }
    file_size_ += bytes_written;
    return IOStatus::OK();
}

IOStatus HDFSWritableFile::Append(const Slice& data, const IOOptions& opts,
              const DataVerificationInfo& /* verification_info */,
              IODebugContext* dbg)  {
    return Append(data, opts, dbg);
}

IOStatus HDFSWritableFile::Flush(const IOOptions& opts, IODebugContext* dbg)  {
    if(hdfsFlush(nn_conn_, fd_) == -1) {
      errLog("HDFSWritableFile::flush", filename_);
      return IOStatus::IOError(hdfsGetLastError());
    }
    return IOStatus::OK();
}

IOStatus HDFSWritableFile::Sync(const IOOptions& opts, IODebugContext* dbg)  {
    if (hdfsSync(nn_conn_, fd_) != 0) {
      errLog("HDFSWritableFile::sync", filename_);
        return IOStatus::IOError(hdfsGetLastError());
    }
    return IOStatus::OK();
}

IOStatus HDFSWritableFile::Fsync(const IOOptions& opts, IODebugContext* dbg)  {
    if (hdfsSync(nn_conn_, fd_) != 0) {
      errLog("HDFSWritableFile::fsync", filename_);
        return IOStatus::IOError(hdfsGetLastError());
    }
    return IOStatus::OK();
}

uint64_t HDFSWritableFile::GetFileSize(const IOOptions& opts, IODebugContext* dbg)  {
    hdfsFileInfo* info = hdfsGetPathInfo(nn_conn_, filename_.c_str());
    if (info) {
        return info->mSize;
    }
    return file_size_;
}

IOStatus HDFSWritableFile::InvalidateCache(size_t offset, size_t length) {
  return IOStatus::NotSupported();
}

HDFSDirectory::~HDFSDirectory() {

}

IOStatus HDFSDirectory::Fsync(const IOOptions& opts, IODebugContext* dbg) {
  return FsyncWithDirOptions(opts, dbg, DirFsyncOptions());
}


IOStatus HDFSDirectory::Close(const IOOptions& /*opts*/,
                               IODebugContext* /*dbg*/) {
  IOStatus s = IOStatus::OK();
  return s;
}

IOStatus HDFSDirectory::FsyncWithDirOptions(
    const IOOptions& /*opts*/, IODebugContext* /*dbg*/,
    const DirFsyncOptions& dir_fsync_options) {
  IOStatus s = IOStatus::OK();
  return s;
}

}

