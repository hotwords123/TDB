#pragma once

 #include <memory>
#include "logical_node.h"

//TODO [Lab3] 请根据需要实现自己的JoinLogicalNode，当前实现仅为建议实现
class JoinLogicalNode : public LogicalNode
{
public:
  JoinLogicalNode() = default;
  ~JoinLogicalNode() override = default;

  LogicalNodeType type() const override
  {
    return LogicalNodeType::JOIN;
  }

  void set_conditions(std::vector<std::unique_ptr<Expression>> &&conditions)
  {
    conditions_ = std::move(conditions);
  }

  std::vector<std::unique_ptr<Expression>> &conditions()
  {
    return conditions_;
  }
private:
  // Join的条件，关系为 AND
  std::vector<std::unique_ptr<Expression>> conditions_;
};
