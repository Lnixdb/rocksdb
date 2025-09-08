#include <rocksdb/cloud/zonda_file_system.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/file_system.h>
#include <rocksdb/options.h>

#include <iostream>
#include <string>
#include <cstdio>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>

using namespace std;

// Worker thread function
void worker_thread(rocksdb::Options options, std::string db_name, bool recover,
                   int key_num, int key_size, int value_size) {

  rocksdb::DB* db;

  if (!recover) {
    DestroyDB(db_name, options);
  }
  // 1. 打开数据库
  auto status = rocksdb::DB::Open(options, db_name, &db);
  if (!status.ok()) {
    cerr << "Failed to open database: " << status.ToString() << endl;
    return std::exit(-1);
  }
  if (recover) {
    delete db;
    return;
  }


  // 2. 写入数据
  for (int i=0; i < key_num; i++) {
    std::string key(key_size, 'k' + i);
    std::string value(value_size, 'v'+i);
    status = db->Put(rocksdb::WriteOptions(), key, value);
    if (!status.ok()) {
      cerr << "Failed to write key1: " << status.ToString() << endl;
    } else {
      cout << "Write success: key1 -> " << i << endl;
    }
  }
  delete db;
  return;
}

int main(int argc, char* argv[]) {
    // Default values for parameters
    int nthread = 1;
    int key_size = 0;
    int value_size = 0;
    long long write_data = 0;
    std::string cluster_id;
    std::string log_path;
    std::string client_id;
    std::string master_addr;
    std::string db_name;
    bool recover_mode = false;

    // 解析参数
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.rfind("--cluster-id=", 0) == 0) {
            cluster_id = arg.substr(13);
        } else if (arg.rfind("--nthread=", 0) == 0) {
            try {
                nthread = std::stoi(arg.substr(10));
            } catch (const std::invalid_argument& e) {
                std::cerr << "Invalid argument for nthread: " << arg.substr(10) << std::endl;
            } catch (const std::out_of_range& e) {
                std::cerr << "Value for nthread is out of range: " << arg.substr(10) << std::endl;
            }
        } else if (arg.rfind("--key-size=", 0) == 0) {
            try {
                key_size = std::stoi(arg.substr(11));
            } catch (...) {}
        } else if (arg.rfind("--value-size=", 0) == 0) {
            try {
                value_size = std::stoi(arg.substr(13));
            } catch (...) {}
        } else if (arg.rfind("--write-data=", 0) == 0) {
            try {
                write_data = std::stoll(arg.substr(13));
            } catch (...) {}
        // New parameter parsing
        } else if (arg.rfind("--log_path=", 0) == 0) {
            log_path = arg.substr(11);
        } else if (arg.rfind("--client_id=", 0) == 0) {
            client_id = arg.substr(12);
        } else if (arg.rfind("--master_addr=", 0) == 0) {
            master_addr = arg.substr(14);
        } else if (arg.rfind("--recover=", 0) == 0) {
            recover_mode = arg.substr(10) == "true"? true : false; ;
        } else if (arg.rfind("--dbname=", 0) == 0) {
          db_name = arg.substr(9);
        }
    }

    // --- Parameter check and calculation ---
    std::cout << "--- Parsed Parameters ---" << std::endl;
    std::cout << "Cluster ID: " << cluster_id << std::endl;
    std::cout << "Number of Threads (nthread): " << nthread << std::endl;
    std::cout << "Key Size: " << key_size << " bytes" << std::endl;
    std::cout << "Value Size: " << value_size << " bytes" << std::endl;
    std::cout << "Total Write Data: " << (write_data >> 20) << " MB" << std::endl;
    std::cout << "Log Path: " << log_path << std::endl;
    std::cout << "Client ID: " << client_id << std::endl;
    std::cout << "DB name: " << master_addr << std::endl;
    std::cout << "Master Address: " << master_addr << std::endl;

    long long num_keys = 0;
    if (key_size + value_size > 0) {
        num_keys = write_data / (key_size + value_size);
    }
    std::cout << "Calculated number of keys to write: " << num_keys << std::endl;

    // Use a vector to manage our threads
    std::vector<std::thread> threads;
    threads.reserve(nthread);

    // Start timer
    auto start_time = std::chrono::high_resolution_clock::now();


    // 初始化
  rocksdb::ZondaFileSystemOptions zonda_options;
  zonda_options.client_id = client_id;
  zonda_options.cluster_id = cluster_id;
  zonda_options.master_addr = master_addr;
  zonda_options.log_path = log_path;
  auto fs_posix = rocksdb::FileSystem::Default();

  rocksdb::ZondaFileSystem* zfs;
  auto status = rocksdb::ZondaFileSystem::NewZondaFileSystem(
    fs_posix, zonda_options, &zfs);
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    std::exit(-1);
  }
  std::shared_ptr<rocksdb::ZondaFileSystem> fs_zonda(zfs);
  auto zonda_env = rocksdb::NewCompositeEnv(fs_zonda);
  rocksdb::Options options;
  options.env = zonda_env.get();
  options.create_if_missing = true;

    // Create and run threads
    for (int i = 0; i < nthread; ++i) {
      std::string name = db_name + std::to_string(i);
      threads.emplace_back(worker_thread,
        options,
        name,
        recover_mode,
        num_keys,
        key_size,
        value_size);
    }

    // Wait for all threads to finish by calling join() on each one
    for (auto& t : threads) {
        t.join();
    }

    // Stop timer
    auto end_time = std::chrono::high_resolution_clock::now();

    // --- Results ---
    std::cout << "--- All Threads Completed ---" << std::endl;

    // Calculate and display the total elapsed time
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Total elapsed time: " << duration << " ms" << std::endl;

    return 0;
}