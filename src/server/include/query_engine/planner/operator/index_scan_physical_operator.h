#pragma once

#include "physical_operator.h"
#include "include/query_engine/structor/tuple/row_tuple.h"
#include "include/storage_engine/recorder/record_manager.h"

class IndexScanner;
/**
 * TODO [Lab2]
 * 通过索引来扫描文件,与TableScanOperator扮演同等的角色.
 * 需要实现index_scan_operator,存在索引时利用索引扫描数据
 * 同时补充physical_operator_generator逻辑,基于IndexScanNode生成IndexScanOperator
 */
class IndexScanPhysicalOperator : public PhysicalOperator
{
public:
  IndexScanPhysicalOperator(Table *table, Index *index, bool readonly,
                           const Value *left_value, bool left_inclusive,
                           const Value *right_value, bool right_inclusive) :
                                             table_(table), index_(index),
                                             readonly_(readonly),
                                             left_inclusive_(left_inclusive),
                                             right_inclusive_(right_inclusive)
  {
    if (left_value != nullptr) {
      left_key_buf_.assign(left_value->data(), left_value->length());
      left_null_ = false;
    }
    if (right_value != nullptr) {
      right_key_buf_.assign(right_value->data(), right_value->length());
      right_null_ = false;
    }
  }

  IndexScanPhysicalOperator(Table *table, Index *index, bool readonly,
                            const char *left_key, int left_len, bool left_inclusive,
                            const char *right_key, int right_len, bool right_inclusive) :
                              table_(table), index_(index), readonly_(readonly),
                              left_inclusive_(left_inclusive), right_inclusive_(right_inclusive)
  {
    if (left_key != nullptr) {
      left_key_buf_.assign(left_key, left_len);
      left_null_ = false;
    }
    if (right_key != nullptr) {
      right_key_buf_.assign(right_key, right_len);
      right_null_ = false;
    }
  }

  ~IndexScanPhysicalOperator() override = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::INDEX_SCAN;
  }

  void set_table_alias(const std::string &alias) {
    table_alias_ = alias;
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

  std::string param() const override;

  void set_predicates(std::vector<std::unique_ptr<Expression>> &&predicates) {
    predicates_ = std::move(predicates);
  }

 private:
  RC filter(RowTuple &tuple, bool &result);
  Table *table_ = nullptr;
  std::string table_alias_;
  Index *index_ = nullptr;
  IndexScanner *index_scanner_ = nullptr;
  RecordFileHandler *record_handler_ = nullptr;
  bool  readonly_ = false;

  RecordPageHandler record_page_handler_;
  Record current_record_;
  RowTuple tuple_;

  std::string left_key_buf_;
  std::string right_key_buf_;
  bool left_inclusive_ = false;
  bool right_inclusive_ = false;
  bool left_null_ = true;
  bool right_null_ = true;
  std::vector<std::unique_ptr<Expression>> predicates_;
};
