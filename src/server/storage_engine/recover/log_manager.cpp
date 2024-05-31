#include "include/storage_engine/recover/log_manager.h"
#include "include/storage_engine/transaction/trx.h"

RC LogEntryIterator::init(LogFile &log_file)
{
  log_file_ = &log_file;
  return RC::SUCCESS;
}

RC LogEntryIterator::next()
{
  LogEntryHeader header;
  RC rc = log_file_->read(reinterpret_cast<char *>(&header), sizeof(header));
  if (rc != RC::SUCCESS) {
    if (log_file_->eof()) {
      return RC::RECORD_EOF;
    }
    LOG_WARN("failed to read log header. rc=%s", strrc(rc));
    return rc;
  }

  char *data = nullptr;
  int32_t entry_len = header.log_entry_len_;
  if (entry_len > 0) {
    data = new char[entry_len];
    rc = log_file_->read(data, entry_len);
    if (RC_FAIL(rc)) {
      LOG_WARN("failed to read log data. data size=%d, rc=%s", entry_len, strrc(rc));
      delete[] data;
      data = nullptr;
      return rc;
    }
  }

  if (log_entry_ != nullptr) {
    delete log_entry_;
    log_entry_ = nullptr;
  }
  log_entry_ = LogEntry::build(header, data);
  delete[] data;
  return rc;
}

bool LogEntryIterator::valid() const
{
  return log_entry_ != nullptr;
}

const LogEntry &LogEntryIterator::log_entry()
{
  return *log_entry_;
}

////////////////////////////////////////////////////////////////////////////////

LogManager::~LogManager()
{
  if (log_buffer_ != nullptr) {
    delete log_buffer_;
    log_buffer_ = nullptr;
  }

  if (log_file_ != nullptr) {
    delete log_file_;
    log_file_ = nullptr;
  }
}

RC LogManager::init(const char *path)
{
  log_buffer_ = new LogBuffer();
  log_file_   = new LogFile();
  return log_file_->init(path);
}

RC LogManager::append_begin_trx_log(int32_t trx_id)
{
  return append_log(LogEntry::build_mtr_entry(LogEntryType::MTR_BEGIN, trx_id));
}

RC LogManager::append_rollback_trx_log(int32_t trx_id)
{
  return append_log(LogEntry::build_mtr_entry(LogEntryType::MTR_ROLLBACK, trx_id));
}

RC LogManager::append_commit_trx_log(int32_t trx_id, int32_t commit_xid)
{
  RC rc = append_log(LogEntry::build_commit_entry(trx_id, commit_xid));
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to append trx commit log. trx id=%d, rc=%s", trx_id, strrc(rc));
    return rc;
  }
  rc = sync(); // 事务提交时需要把当前事务关联的日志项都写入到磁盘中，这样做是保证不丢数据
  return rc;
}

RC LogManager::append_record_log(LogEntryType type, int32_t trx_id, int32_t table_id, const RID &rid, int32_t data_len, int32_t data_offset, const char *data)
{
  LogEntry *log_entry = LogEntry::build_record_entry(type, trx_id, table_id, rid, data_len, data_offset, data);
  if (nullptr == log_entry) {
    LOG_WARN("failed to create log entry");
    return RC::NOMEM;
  }
  return append_log(log_entry);
}

RC LogManager::append_log(LogEntry *log_entry)
{
  if (nullptr == log_entry) {
    return RC::INVALID_ARGUMENT;
  }
  return log_buffer_->append_log_entry(log_entry);
}

RC LogManager::sync()
{
  return log_buffer_->flush_buffer(*log_file_);
}

// TODO [Lab5] 需要同学们补充代码，相关提示见文档
RC LogManager::recover(Db *db)
{
  TrxManager *trx_manager = GCTX.trx_manager_;
  ASSERT(trx_manager != nullptr, "cannot do recover that trx_manager is null");
  ASSERT(log_file_ != nullptr, "cannot do recover that log_file_ is null");

  LogEntryIterator log_entry_iter;
  RC rc = log_entry_iter.init(*log_file_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to init log entry iterator. rc=%s", strrc(rc));
    return rc;
  }

  while ((rc = log_entry_iter.next()) != RC::RECORD_EOF) {
    if (rc != RC::SUCCESS) {
      LOG_ERROR("failed to read next log entry. rc=%s", strrc(rc));
      return rc;
    }

    const LogEntry &log_entry = log_entry_iter.log_entry();
    int trx_id = log_entry.trx_id();
    LogEntryType log_type = log_entry.log_type();

    LOG_TRACE("recover log_entry=%s", log_entry.to_string().c_str());

    if (log_type == LogEntryType::MTR_BEGIN) {
      Trx *trx = trx_manager->create_trx(trx_id);
      if (trx == nullptr) {
        LOG_ERROR("failed to create trx. log_entry=%s", log_entry.to_string().c_str());
        return RC::INTERNAL;
      }
    } else {
      Trx *trx = trx_manager->find_trx(trx_id);
      if (trx == nullptr) {
        LOG_ERROR("failed to find trx. log_entry=%s", log_entry.to_string().c_str());
        return RC::INTERNAL;
      }

      rc = trx->redo(db, log_entry);
      if (rc != RC::SUCCESS) {
        LOG_ERROR("failed to redo log entry. rc=%s, log_entry=%s", strrc(rc), log_entry.to_string().c_str());
        return rc;
      }

      if (log_type == LogEntryType::MTR_COMMIT || log_type == LogEntryType::MTR_ROLLBACK) {
        trx_manager->destroy_trx(trx);
      }
    }
  }

  // 检查是否有未结束的事务，如果有则回滚
  std::vector<Trx *> trxes;
  trx_manager->all_trxes(trxes);
  for (Trx *trx : trxes) {
    RC rc = trx->rollback();
    if (rc != RC::SUCCESS) {
      LOG_ERROR("failed to rollback unfinished trx. trx_id=%d, rc=%s", trx->id(), strrc(rc));
      return rc;
    }
    trx_manager->destroy_trx(trx);
  }

  return RC::SUCCESS;
}

