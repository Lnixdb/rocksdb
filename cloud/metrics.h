#pragma once

#include <memory>
#include <string>

#include <prometheus/registry.h>
#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <rocksdb/file_system.h>

namespace ROCKSDB_NAMESPACE {

enum Event : uint32_t {
  OnFlushCompleted = 0,
  OnFlushBegin,
  OnManualFlushScheduled,
  OnTableFileDeleted,
  OnCompactionBegin,
  OnCompactionCompleted,
  OnSubcompactionBegin,
  OnSubcompactionCompleted,
  OnTableFileCreated,
  OnTableFileCreationStarted,
  OnMemTableSealed,
  OnColumnFamilyHandleDeletionStarted,
  OnExternalFileIngested,
  OnBackgroundError,
  OnStallConditionsChanged,
  OnFileReadFinish,
  OnFileWriteFinish,
  OnFileFlushFinish,
  OnFileSyncFinish,
  OnFileRangeSyncFinish,
  OnFileTruncateFinish,
  OnFileCloseFinish,
  ShouldBeNotifiedOnFileIO,
  OnErrorRecoveryBegin,
  OnErrorRecoveryEnd,
  OnBlobFileCreationStarted,
  OnBlobFileCreated,
  OnBlobFileDeleted,
  OnIOError
};

inline std::unordered_map<Event, std::string> eventNameMap = {
   {OnFlushCompleted, "on_flush_completed"},
   {OnFlushBegin, "on_flush_begin"},
   {OnManualFlushScheduled, "on_manual_flush_scheduled"},
   {OnTableFileDeleted, "on_table_file_deleted"},
   {OnCompactionBegin, "on_compaction_begin"},
   {OnCompactionCompleted, "on_compaction_completed"},
   {OnSubcompactionBegin, "on_subcompaction_begin"},
   {OnSubcompactionCompleted, "on_subcompaction_completed"},
   {OnTableFileCreated, "on_table_file_created"},
   {OnTableFileCreationStarted, "on_table_file_creation_started"},
   {OnMemTableSealed, "on_mem_table_sealed"},
   {OnColumnFamilyHandleDeletionStarted, "on_column_family_handle_deletion_started"},
   {OnExternalFileIngested, "on_external_file_ingested"},
   {OnBackgroundError, "on_background_error"},
   {OnStallConditionsChanged, "on_stall_conditions_changed"},
   {OnFileReadFinish, "on_file_read_finish"},
   {OnFileWriteFinish, "on_file_write_finish"},
   {OnFileFlushFinish, "on_file_flush_finish"},
   {OnFileSyncFinish, "on_file_sync_finish"},
   {OnFileRangeSyncFinish, "on_file_range_sync_finish"},
   {OnFileTruncateFinish, "on_file_truncate_finish"},
   {OnFileCloseFinish, "on_file_close_finish"},
   {ShouldBeNotifiedOnFileIO, "should_be_notified_on_file_io"},
   {OnErrorRecoveryBegin, "on_error_recovery_begin"},
   {OnErrorRecoveryEnd, "on_error_recovery_end"},
   {OnBlobFileCreationStarted, "on_blob_file_creation_started"},
   {OnBlobFileCreated, "on_blob_file_created"},
   {OnBlobFileDeleted, "on_blob_file_deleted"},
   {OnIOError, "on_io_error"}
};

class ZondaFSMetrics {
public:
   static ZondaFSMetrics& Instance();
   ZondaFSMetrics() = default;
   static void Init(int port) {
     Instance().InitImpl(port);
   }
   std::shared_ptr<prometheus::Registry> GetRegistry();

   void Histograms(const std::string& name, const std::string& quantile, double value);
   void Ticker(const std::string& name, double value);
   void Listener(Event event, double value);

private:
   void InitImpl(int port);
   //std::shared_ptr<prometheus::Registry> registry_;
   //std::unique_ptr<prometheus::Exposer> exposer_;
   std::unordered_map<std::string, prometheus::Gauge*> gauge_;
   std::unordered_map<std::string, prometheus::Counter*> ticker_;
   std::unordered_map<std::string, prometheus::Counter*> counter_;
};

class ZondaFSEventListener : public EventListener {
public:
  ~ZondaFSEventListener() override = default;
  static const char* kClassName() { return "ZondaFSEventListener"; }

  void OnFlushBegin(DB* /*db*/, const FlushJobInfo& /*flush_job_info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnFlushBegin, 1);
  }

  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& /*flush_job_info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnFlushCompleted, 1);
  }

  void OnManualFlushScheduled(DB* /*db*/,
    const std::vector<ManualFlushInfo>& /*manual_flush_info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnManualFlushScheduled, 1);
  }

  void OnTableFileDeleted(const TableFileDeletionInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnTableFileDeleted, 1);
  }

  void OnCompactionBegin(DB* /*db*/, const CompactionJobInfo& /*ci*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnCompactionBegin, 1);
  }

  void OnCompactionCompleted(DB* /*db*/, const CompactionJobInfo& /*ci*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnCompactionCompleted, 1);
  }

  void OnSubcompactionBegin(const SubcompactionJobInfo& /*si*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnSubcompactionBegin, 1);
  }

  void OnSubcompactionCompleted(const SubcompactionJobInfo& /*si*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnSubcompactionCompleted, 1);
  }

  void OnTableFileCreated(const TableFileCreationInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnTableFileCreated, 1);
  }

  void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnTableFileCreationStarted, 1);
  }

  void OnMemTableSealed(const MemTableInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnMemTableSealed, 1);
  }

  void OnColumnFamilyHandleDeletionStarted(
      ColumnFamilyHandle* /*handle*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnColumnFamilyHandleDeletionStarted, 1);
  }

  void OnExternalFileIngested(DB* /*db*/,
    const ExternalFileIngestionInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnExternalFileIngested, 1);
  }

  void OnStallConditionsChanged(const WriteStallInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnStallConditionsChanged, 1);
  }

  void OnFileReadFinish(const FileOperationInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnFileReadFinish, 1);
  }

  void OnFileWriteFinish(const FileOperationInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnFileWriteFinish, 1);
  }

  void OnFileFlushFinish(const FileOperationInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnFileFlushFinish, 1);
  }

  void OnFileSyncFinish(const FileOperationInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnFileSyncFinish, 1);
  }

  void OnFileRangeSyncFinish(const FileOperationInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnFileRangeSyncFinish, 1);
  }

  void OnFileTruncateFinish(const FileOperationInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnFileTruncateFinish, 1);
  }

  void OnFileCloseFinish(const FileOperationInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnFileCloseFinish, 1);
  }

  void OnErrorRecoveryBegin(BackgroundErrorReason /*reason*/, Status /*bg_error*/,
    bool* /*auto_recovery*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnErrorRecoveryBegin, 1);
  }

  void OnErrorRecoveryEnd(const BackgroundErrorRecoveryInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnErrorRecoveryEnd, 1);
  }

  void OnBlobFileCreationStarted(
    const BlobFileCreationBriefInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnBlobFileCreationStarted, 1);
  }

  void OnBlobFileCreated(const BlobFileCreationInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnBlobFileCreated, 1);
  }

  void OnBlobFileDeleted(const BlobFileDeletionInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnBlobFileDeleted, 1);
  }

  void OnIOError(const IOErrorInfo& /*info*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnIOError, 1);
  }

  void OnBackgroundError(BackgroundErrorReason /*reason*/,
    Status* /*bg_error*/) override {
    ZondaFSMetrics::Instance().Listener(Event::OnBackgroundError, 1);
  }
};


}
