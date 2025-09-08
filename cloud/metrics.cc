#include "cloud/metrics.h"
#include "rocksdb/statistics.h"

namespace ROCKSDB_NAMESPACE {

void ZondaFSMetrics::InitImpl(int port) {
  // std::string addr = "0.0.0.0:" + std::to_string(port);
  // exposer_ = std::make_unique<prometheus::Exposer>(addr);
  // registry_ = std::make_shared<prometheus::Registry>();
  // exposer_->RegisterCollectable(registry_);
  //
  // // ticker eg: rocksdb_block_cache_miss
  // for (const auto& t : TickersNameMap) {
  //   auto name = t.second;
  //   std::replace(name.begin(), name.end(), '.', '_');
  //
  //   auto& ticker = prometheus::BuildCounter()
  //      .Name(name)
  //      .Register(*registry_);
  //
  //   ticker_[name] = &ticker.Add({{"business", "fs"} });
  // }
  //
  // auto& gauge_family = prometheus::BuildGauge()
  //       .Name("fs_histograms")
  //       .Help("rocksdb statistics monitor")
  //       .Register(*registry_);
  //
  // // latency
  // for (const auto& h : HistogramsNameMap) {
  //   auto avg = h.second + ".avg";
  //   auto p50 = h.second + ".p50";
  //   auto p95 = h.second + ".p95";
  //   auto p99 = h.second + ".p99";
  //   gauge_[avg] = &gauge_family.Add({{"name", avg} });
  //   gauge_[p50] = &gauge_family.Add({{"name", p50} });
  //   gauge_[p95] = &gauge_family.Add({{"name", p95} });
  //   gauge_[p99] = &gauge_family.Add({{"name", p99} });
  // }
  //
  // // rocksdb event listener
  // auto& counter_family = prometheus::BuildCounter()
  //       .Name("fs_event")
  //       .Help("rocksdb event listener")
  //       .Register(*registry_);
  // for (const auto& e : eventNameMap) {
  //   counter_[e.second] = &counter_family.Add({{"event", e.second}});
  // }
}

ZondaFSMetrics& ZondaFSMetrics::Instance() {
  static ZondaFSMetrics instance;
  return instance;
}

void ZondaFSMetrics::Histograms(const std::string& name,
                                const std::string& quantile, double value) {
  auto n =  name + "." + quantile;
  if (gauge_.find(n) == gauge_.end()) {
    return;
  }
  gauge_[n]->Set(value);
}

void ZondaFSMetrics::Ticker(const std::string& n, double value) {
  auto name = n;
  std::replace(name.begin(), name.end(), '.', '_');
  if (ticker_.find(name) == ticker_.end()) {
    return;
  }
  ticker_[name]->Increment(value);
}

void ZondaFSMetrics::Listener(Event event, double value) {
  if (eventNameMap.find(event) == eventNameMap.end()) {
    return;
  }
  auto name = eventNameMap[event];
  if (counter_.find(name) == counter_.end()) {
    return;
  }
  counter_[name]->Increment(value);
}

std::shared_ptr<prometheus::Registry> ZondaFSMetrics::GetRegistry() {
  return nullptr;
}

}
