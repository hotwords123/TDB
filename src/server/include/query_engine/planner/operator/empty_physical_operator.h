#pragma once

#include "physical_operator.h"

/**
 * @brief 空物理算子
 * @ingroup PhysicalOperator
 * @details 用于占位的算子，不执行任何操作
 */
class EmptyPhysicalOperator : public PhysicalOperator
{
public:
  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::EMPTY;
  }

  RC open(Trx *) override
  {
    return RC::SUCCESS;
  }

  RC next() override
  {
    return RC::RECORD_EOF;
  }

  RC close() override
  {
    return RC::SUCCESS;
  }

  Tuple *current_tuple() override
  {
    return nullptr;
  }
};
