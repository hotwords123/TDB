#include "include/query_engine/planner/operator/index_scan_physical_operator.h"
#include "include/query_engine/structor/tuple/join_tuple.h"

#include "include/storage_engine/index/index.h"

// TODO [Lab2]
// IndexScanOperator的实现逻辑,通过索引直接获取对应的Page来减少磁盘的扫描

static RC get_expr_value(const Expression *expr, const Tuple *tuple, Value &value) {
  if (tuple != nullptr) {
    return expr->get_value(*tuple, value);
  } else {
    return expr->try_get_value(value);
  }
}

RC IndexScanPhysicalOperator::open(Trx *trx)
{
  if(table_ == nullptr || index_ == nullptr)
  {
    return RC::INTERNAL;
  }

  // 获取左右边界的值
  Value left_value, right_value;
  if (left_expr_) {
    RC rc = get_expr_value(left_expr_.get(), father_tuple_, left_value);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("IndexScanPhysicalOperator: failed to get value of scan key: rc=%s", strrc(rc));
      return rc;
    }
    left_key_buf_.assign(left_value.data(), left_value.length());
  }
  if (right_expr_) {
    RC rc = get_expr_value(right_expr_.get(), father_tuple_, right_value);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("IndexScanPhysicalOperator: failed to get value of scan key: rc=%s", strrc(rc));
      return rc;
    }
    right_key_buf_.assign(right_value.data(), right_value.length());
  }

  // 检查左右边界是否有效
  if (left_expr_ && right_expr_) {
    int ret = left_value.compare(right_value);
    if (ret > 0 || (ret == 0 && !(left_inclusive_ && right_inclusive_))) {
      LOG_INFO("IndexScanPhysicalOperator: scan range is empty, skipping");
      return RC::SUCCESS;
    }
  }

  IndexScanner *index_scanner = index_->create_scanner(left_expr_ ? left_key_buf_.data() : nullptr,
                                                       left_key_buf_.length(),
                                                       left_inclusive_,
                                                       right_expr_ ? right_key_buf_.data() : nullptr,
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

  if (table_alias_.empty()) {
    table_alias_ = table_->name();
    LOG_WARN("table alias is empty, use table name as alias.\n"
      "Hint: Consider calling set_table_alias() on IndexScanOperator to set an alias for the table.");
  }

  tuple_.set_schema(table_,table_alias_,table_->table_meta().field_metas());

  return RC::SUCCESS;
}

RC IndexScanPhysicalOperator::next()
{
  if (index_scanner_ == nullptr) {
    return RC::RECORD_EOF;
  }

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
  record_page_handler_.cleanup();
  if (index_scanner_ != nullptr) {
    index_scanner_->destroy();
    index_scanner_ = nullptr;
  }
  left_key_buf_.clear();
  right_key_buf_.clear();
  return RC::SUCCESS;
}

Tuple* IndexScanPhysicalOperator::current_tuple(){
  tuple_._set_record(&current_record_);
  return &tuple_;
}

std::string IndexScanPhysicalOperator::param() const
{
  std::string res = std::string(index_->index_meta().name()) + " ON " + table_->name();
  if (table_alias_ != table_->name()) {
    res += " AS " + table_alias_;
  }
  if (left_expr_ != nullptr) {
    res += " LEFT " + left_expr_->to_string();
    res += left_inclusive_ ? " INCLUSIVE" : " EXCLUSIVE";
  }
  if (right_expr_ != nullptr) {
    res += " RIGHT " + right_expr_->to_string();
    res += right_inclusive_ ? " INCLUSIVE" : " EXCLUSIVE";
  }
  for (size_t i = 0; i < predicates_.size(); i++) {
    res += i == 0 ? " WHERE " : " AND ";
    res += predicates_[i]->to_string();
  }
  return res;
}

RC IndexScanPhysicalOperator::filter(RowTuple &tuple, bool &result)
{
  JoinedTuple joined_tuple;
  joined_tuple.set_left(const_cast<Tuple *>(father_tuple_));
  joined_tuple.set_right(&tuple);

  RC rc = RC::SUCCESS;
  Value value;
  for (std::unique_ptr<Expression> &expr : predicates_) {
    rc = expr->get_value(joined_tuple, value);
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
