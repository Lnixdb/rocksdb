
#include <rocksdb/cloud/zonda_file_system.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/file_system.h>
#include <rocksdb/options.h>

#include <iostream>
#include <string>

using namespace std;

int main(int argc, char* argv[]) {
    bool read_only = false;
    int size_mb = 100;
    int write_keys = 0;
    // 遍历参数，查找 "--enable-feature" 这样的 flag
    for (int i = 1; i < argc; i++) {
      std::string arg = argv[i];
      if (arg == "--read-only") {
        read_only = true;
      }
      if (arg.rfind("--size=", 0) == 0) {   // 检查是否以 "--port=" 开头
        size_mb = std::stoi(arg.substr(7));  // 取出等号后面的部分并转为整数
      }
      if (arg.rfind("--nkey=", 0) == 0) {   // 检查是否以 "--port=" 开头
        write_keys = std::stoi(arg.substr(7));  // 取出等号后面的部分并转为整数
      }
    }

    if (read_only) {
      std::cout << "read_only enabled!" << std::endl;
    } else {
      std::cout << "read_only disabled!" << std::endl;
    }

    rocksdb::ZondaFileSystemOptions zonda_options;
    zonda_options.client_id = "test_cluster_lanyan";
    zonda_options.cluster_id = "test_cluster_lanyan";
    zonda_options.master_addr = "list://127.0.0.1:38100,127.0.0.1:38101,127.0.0.1:38102";
    zonda_options.log_path = "/home/lanyan/log";
    auto fs_posix = rocksdb::FileSystem::Default();

    rocksdb::ZondaFileSystem* zfs;
    auto status = rocksdb::ZondaFileSystem::NewZondaFileSystem(
      fs_posix, zonda_options, &zfs);
    if (!status.ok()) {
      std::cerr << status.ToString() << std::endl;
      return -1;
    }

    std::shared_ptr<rocksdb::ZondaFileSystem> fs_zonda(zfs);
    auto zonda_env = rocksdb::NewCompositeEnv(fs_zonda);

    rocksdb::DB* db;
    rocksdb::Options options;
    options.env = zonda_env.get();
    options.create_if_missing = true;

    // 1. 打开数据库
    status = rocksdb::DB::Open(options, "/zonda/fs/test_db", &db);
    if (!status.ok()) {
        cerr << "Failed to open database: " << status.ToString() << endl;
        return 1;
    }

    // 2. 写入 500MB 数据
    string key1 = "rocksdb-";
    string value1(1<<10, 'a');
    int nloop = (size_mb << 20) / (1 << 10);
    if (write_keys != 0) {
      nloop = write_keys;
    }
    if (!read_only) {
      for (int i=0; i<nloop; i++) {
        auto write_key = key1 + std::to_string(i);
        status = db->Put(rocksdb::WriteOptions(), write_key, value1);
        if (!status.ok()) {
          cerr << "Failed to write key1: " << status.ToString() << endl;
          delete db;
          return -1;
        } else {
          cout << "Write success: -> " << write_key  << " size:" << value1.size() << endl;
        }
      }
    }

    // 3. 读取数据
    string value;
    size_t read_hit = 0, read_miss = 0, read_error = 0;
    for (int i=0; i<nloop; i++) {
      value.clear();
      auto read_key = key1 + std::to_string(i);
      status = db->Get(rocksdb::ReadOptions(), read_key, &value);
      if (status.ok()) {
        read_hit += 1;
        cout << "Read success:" <<  read_key  << " value.size:" << value.size() << endl;
      } else if (status.IsNotFound()) {
        read_miss += 1;
        cerr << key1 + std::to_string(i) << " not found!" << endl;
      } else {
        read_error += 1;
        cerr << "Read failed: " << status.ToString() << endl;
      }
    }

    // 5. 遍历所有键值对

    size_t scan_hit = 0;

    cout << "\nAll keys in DB:" << endl;
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        scan_hit += 1;
        cout << "It success:" << it->key().ToString() << " -> " << it->value().size() << endl;
    }
    if (!it->status().ok()) {
        cerr << "Iterator error: " << it->status().ToString() << endl;
        delete it;
        delete db;
        return -1;
    }
    delete it;

    std::cout << "Total key:" << nloop << endl;
    std::cout << "Read hit:" << read_hit  << " miss:" << read_miss << " error:" << read_error << endl;
    std::cout << "Scan hit:" << scan_hit << std::endl;

    cout << "\nCongratulation!!!" << endl;
    // 6. 关闭数据库
    delete db;
    return 0;
}