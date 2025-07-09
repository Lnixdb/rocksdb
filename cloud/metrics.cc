#include "cloud/metrics.h"

namespace ROCKSDB_NAMESPACE {

ZondaFSMetrics::ZondaFSMetrics() {
  registry_ = std::make_shared<prometheus::Registry>();

  counter_family_ = &prometheus::BuildCounter()
                         .Name("zonda_fs")
                         .Help("Zonda FS method level metrics")
                         .Register(*registry_);

  exposer_.RegisterCollectable(registry_);
}

ZondaFSMetrics& ZondaFSMetrics::Instance() {
  static ZondaFSMetrics instance;
  return instance;
}

prometheus::Counter& ZondaFSMetrics::GetMethodCounter(const std::string& method) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = counters_.find(method);
  if (it != counters_.end()) {
    return *(it->second);
  }

  auto& counter = counter_family_->Add({{"method", method}});
  counters_[method] = &counter;
  return counter;
}

std::shared_ptr<prometheus::Registry> ZondaFSMetrics::GetRegistry() {
  return registry_;
}

}
