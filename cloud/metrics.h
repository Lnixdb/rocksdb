#pragma once

#include <memory>
#include <map>
#include <string>

#include <prometheus/registry.h>
#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <rocksdb/file_system.h>

namespace ROCKSDB_NAMESPACE {

class ZondaFSMetrics {
public:
  static ZondaFSMetrics& Instance();

  // 获取某个 method 对应的 Counter
  prometheus::Counter& GetMethodCounter(const std::string& method);

  // 获取 prometheus registry（用于注册到 Exposer）
  std::shared_ptr<prometheus::Registry> GetRegistry();

private:
  ZondaFSMetrics();
  std::shared_ptr<prometheus::Registry> registry_;
  prometheus::Family<prometheus::Counter>* counter_family_;

  std::mutex mutex_;
  std::map<std::string, prometheus::Counter*> counters_;
  prometheus::Exposer exposer_{"0.0.0.0:7800"};
};

}
