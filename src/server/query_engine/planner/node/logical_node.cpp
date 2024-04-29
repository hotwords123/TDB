#include "include/query_engine/planner/node/logical_node.h"

LogicalNode::~LogicalNode()
{}

void LogicalNode::add_child(std::unique_ptr<LogicalNode> oper)
{
  children_.emplace_back(std::move(oper));
}

bool LogicalNode::visit(const Visitor &visitor) const
{
  if (visitor(this)) {
    return true;
  }
  for (const auto &child : children_) {
    if (child->visit(visitor)) {
      return true;
    }
  }
  return false;
}
