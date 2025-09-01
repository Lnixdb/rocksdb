#include <rocksdb/cloud/zonda_file_system.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/file_system.h>
#include <rocksdb/options.h>

#include <iostream>
#include <string>

using namespace std;

int main() {
    rocksdb::ZondaFileSystemOptions zonda_options;
    zonda_options.client_id = "test_cluster_lanyan";
    zonda_options.cluster_id = "test_cluster_lanyan";
    zonda_options.master_addr = "list://127.0.0.1:38100,127.0.0.1:38101,127.0.0.1:38102";

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

    // 2. 写入数据
    string key1 = "key1";
    string value1 = "Hello, RocksDB!";
    status = db->Put(rocksdb::WriteOptions(), key1, value1);
    if (!status.ok()) {
        cerr << "Failed to write key1: " << status.ToString() << endl;
    } else {
        cout << "Write success: key1 -> " << value1 << endl;
    }


    // 3. 读取数据
    string value;
    status = db->Get(rocksdb::ReadOptions(), key1, &value);
    if (status.ok()) {
        cout << "Read success: key1 -> " << value << endl;
    } else if (status.IsNotFound()) {
        cerr << "Key1 not found!" << endl;
    } else {
        cerr << "Read failed: " << status.ToString() << endl;
    }

    // 4. 批量写入（原子操作）
    rocksdb::WriteBatch batch;
    batch.Put("key2", "value2");
    batch.Put("key3", "value3");
    batch.Delete("key1");  // 删除 key1
    status = db->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok()) {
        cerr << "Batch write failed: " << status.ToString() << endl;
    }

    // 5. 遍历所有键值对
    cout << "\nAll keys in DB:" << endl;
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        cout << it->key().ToString() << " -> " << it->value().ToString() << endl;
    }
    if (!it->status().ok()) {
        cerr << "Iterator error: " << it->status().ToString() << endl;
    }
    delete it;

    // 6. 关闭数据库
    delete db;
    return 0;
}