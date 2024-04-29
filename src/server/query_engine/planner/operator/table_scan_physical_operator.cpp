#include "include/query_engine/planner/operator/table_scan_physical_operator.h"
#include "include/storage_engine/recorder/table.h"
#include "include/query_engine/structor/tuple/join_tuple.h"

using namespace std;

RC TableScanPhysicalOperator::open(Trx *trx)
{
  RC rc = table_->get_record_scanner(record_scanner_, trx, readonly_);
  if (rc == RC::SUCCESS) {
    tuple_.set_schema(table_, table_alias_, table_->table_meta().field_metas());
  }
  trx_ = trx;
  return rc;
}

RC TableScanPhysicalOperator::next()
{
  if (!record_scanner_.has_next()) {
    return RC::RECORD_EOF;
  }

  RC rc = RC::SUCCESS;
  bool filter_result = false;
  while (record_scanner_.has_next()) {
    rc = record_scanner_.next(current_record_);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    tuple_._set_record(&current_record_);
    rc = filter(tuple_, filter_result);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    if (filter_result) {
      break;
    } else {
      rc = RC::RECORD_EOF;
    }
  }
  return rc;
}

RC TableScanPhysicalOperator::close()
{
  return record_scanner_.close_scan();
}

Tuple *TableScanPhysicalOperator::current_tuple()
{
  if (tuple_.order_set()) {
    tuple_.remove_order_set();
    return &tuple_;
  }
  tuple_._set_record(&current_record_);
  return &tuple_;
}

string TableScanPhysicalOperator::param() const
{
  std::string res = table_->name();
  if (table_alias_ != table_->name()) {
    res += " AS " + table_alias_;
  }
  for (size_t i = 0; i < predicates_.size(); i++) {
    res += i == 0 ? " WHERE " : " AND ";
    res += predicates_[i]->to_string();
  }
  return res;
}

void TableScanPhysicalOperator::set_predicates(vector<unique_ptr<Expression>> &&exprs)
{
  predicates_ = std::move(exprs);
}

RC TableScanPhysicalOperator::filter(RowTuple &tuple, bool &result)
{
  JoinedTuple joined_tuple;
  joined_tuple.set_left(const_cast<Tuple *>(father_tuple_));
  joined_tuple.set_right(&tuple);

  RC rc = RC::SUCCESS;
  Value value;
  for (unique_ptr<Expression> &expr : predicates_) {
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
