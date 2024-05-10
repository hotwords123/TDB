#include "include/query_engine/planner/operator/index_scan_physical_operator.h"

#include "include/storage_engine/index/index.h"

// TODO [Lab2]
// IndexScanOperator的实现逻辑,通过索引直接获取对应的Page来减少磁盘的扫描

RC IndexScanPhysicalOperator::open(Trx *trx)
{
  if(table_ == nullptr || index_ == nullptr)
  {
    return RC::INTERNAL;
  }

  const char *left_key = left_null_ ? nullptr : left_key_buf_.data();
  const char *right_key = right_null_ ? nullptr : right_key_buf_.data();
  IndexScanner *index_scanner = index_->create_scanner(left_key,
                                                       left_key_buf_.length(),
                                                       left_inclusive_,
                                                       right_key,
                                                       right_key_buf_.length(),
                                                       right_inclusive_);
  if(index_scanner == nullptr)
  {
    return RC::INTERNAL;
  }

  record_handler_ = table_->record_handler();
  if(record_handler_ == nullptr)
  {
    index_scanner->destroy();
    return RC::INTERNAL;
  }
  index_scanner_ = index_scanner;

  tuple_.set_schema(table_,"",table_->table_meta().field_metas());

  return RC::SUCCESS;
}

RC IndexScanPhysicalOperator::next()
{
  RID rid;
  RecordFileHandler *record_handler = table_->record_handler();

  // TODO [Lab2] 通过IndexScanner循环获取下一个RID，然后通过RecordHandler获取对应的Record
  // 在现有的查询实现中，会在调用next()方法后通过current_tuple()获取当前的Tuple,
  // 从current_tuple()的实现中不难看出, 数据会通过current_record_传递到Tuple中并返回,
  // 因此该next()方法的主要目的就是将recordHandler获取到的数据填充到current_record_中
  RC rc = RC::SUCCESS;
  bool filter_result = false;

  while (!filter_result) {
    // 使用 IndexScanner 获取下一个 RID
    rc = index_scanner_->next_entry(&rid, isdelete_);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    // 使用 RecordFileHandler 获取对应的 Record
    record_page_handler_.cleanup();
    rc = record_handler->get_record(record_page_handler_, &rid, readonly_, &current_record_);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    // 判断过滤条件
    tuple_._set_record(&current_record_);
    rc = filter(tuple_, filter_result);
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }

  return rc;
}

RC IndexScanPhysicalOperator::close()
{
  if (index_scanner_ != nullptr) {
    index_scanner_->destroy();
    index_scanner_ = nullptr;
  }
  return RC::SUCCESS;
}

Tuple* IndexScanPhysicalOperator::current_tuple(){
  tuple_._set_record(&current_record_);
  return &tuple_;
}

std::string IndexScanPhysicalOperator::param() const
{
  return std::string(index_->index_meta().name()) + " ON " + table_->name();
}

RC IndexScanPhysicalOperator::filter(RowTuple &tuple, bool &result)
{
  RC rc = RC::SUCCESS;
  Value value;
  for (std::unique_ptr<Expression> &expr : predicates_) {
    rc = expr->get_value(tuple, value);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    bool tmp_result = value.get_boolean();
    if (!tmp_result) {
      result = false;
      return rc;
    }
  }

  result = true;
  return rc;
}
