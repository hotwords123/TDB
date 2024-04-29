#pragma once

#include "tuple.h"

/**
 * @brief 将两个tuple合并为一个tuple
 * @ingroup Tuple
 * @details 在join算子中使用
 */
class JoinedTuple : public Tuple
{
public:
  JoinedTuple() = default;
  virtual ~JoinedTuple() = default;

  const TupleType tuple_type() const override { return JoinedTuple_Type; }

  Tuple *left() const { return left_; }
  Tuple *right() const { return right_; }

  void set_left(Tuple *left)
  {
    left_ = left;
  }
  void set_right(Tuple *right)
  {
    right_ = right;
  }

  void get_record(std::vector<Record *> &records) const override
  {
    if (left_ != nullptr) {
      left_->get_record(records);
    }
    if (right_ != nullptr) {
      right_->get_record(records);
    }
  }

  void set_record(std::vector<Record *> &records) override
  {
    if (left_ != nullptr) {
      left_->set_record(records);
    }
    if (right_ != nullptr) {
      right_->set_record(records);
    }
  }

  int cell_num() const override
  {
    int left_cell_num = left_ != nullptr ? left_->cell_num() : 0;
    int right_cell_num = right_ != nullptr ? right_->cell_num() : 0;
    return left_cell_num + right_cell_num;
  }

  RC cell_at(int index, Value &value) const override
  {
    int left_cell_num = 0;
    if (left_ != nullptr) {
      left_cell_num = left_->cell_num();
      if (index >= 0 && index < left_cell_num) {
        return left_->cell_at(index, value);
      }
    }

    if (right_ != nullptr) {
      if (index >= left_cell_num && index < left_cell_num + right_->cell_num()) {
        return right_->cell_at(index - left_cell_num, value);
      }
    }

    return RC::NOTFOUND;
  }

  RC find_cell(const TupleCellSpec &spec, Value &value) const override
  {
    RC rc = RC::NOTFOUND;
    if (left_ != nullptr) {
      rc = left_->find_cell(spec, value);
      if (rc == RC::SUCCESS || rc != RC::NOTFOUND) {
        return rc;
      }
    }
    if (right_ != nullptr) {
      rc = right_->find_cell(spec, value);
    }
    return rc;
  }

private:
  Tuple *left_ = nullptr;
  Tuple *right_ = nullptr;
};
