#pragma once

#include "physical_operator.h"
#include "include/query_engine/structor/tuple/join_tuple.h"

// TODO [Lab3] join算子的头文件定义，根据需要添加对应的变量和方法
class JoinPhysicalOperator : public PhysicalOperator
{
public:
  JoinPhysicalOperator();
  ~JoinPhysicalOperator() override = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::JOIN;
  }

  std::string param() const override;

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  Tuple *current_tuple() override;

  void set_conditions(std::vector<std::unique_ptr<Expression>> &&conditions)
  {
    conditions_ = std::move(conditions);
  }

private:
  enum State {
    OUTER_LOOP_START,
    OUTER_LOOP,
    INNER_LOOP_START,
    INNER_LOOP,
    FINISHED,
  };

  std::vector<std::unique_ptr<Expression>> conditions_;
  Trx *trx_ = nullptr;
  JoinedTuple joined_tuple_;  //! 当前关联的左右两个tuple
  JoinedTuple joined_father_tuple_; //! 右节点的 father_tuple
  State state_ = FINISHED;

  RC filter(JoinedTuple &tuple, bool &result);
};
