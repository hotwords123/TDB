#include "include/query_engine/planner/operator/join_physical_operator.h"

/* TODO [Lab3] join的算子实现，需要根据join_condition实现Join的具体逻辑，
  最后将结果传递给JoinTuple, 并由current_tuple向上返回
 JoinOperator通常会遵循下面的被调用逻辑：
 operator.open()
 while(operator.next()){
    Tuple *tuple = operator.current_tuple();
 }
 operator.close()
*/

JoinPhysicalOperator::JoinPhysicalOperator() = default;

// 执行next()前的准备工作, trx是之后事务中会使用到的，这里不用考虑
RC JoinPhysicalOperator::open(Trx *trx)
{
  trx_ = trx;
  children_[0]->open(trx);
  children_[1]->open(trx);

  joined_tuple_.set_left(nullptr);
  joined_tuple_.set_right(nullptr);
  finished_ = false;

  return RC::SUCCESS;
}

// 计算出接下来需要输出的数据，并将结果set到join_tuple中
// 如果没有更多数据，返回RC::RECORD_EOF
RC JoinPhysicalOperator::next()
{
  if (finished_) {
    return RC::RECORD_EOF;
  }

  PhysicalOperator *left_oper = children_[0].get();
  PhysicalOperator *right_oper = children_[1].get();

  RC rc = RC::SUCCESS;

  // 实现 Nested Loop Join
  while (true) {
    // 内层循环步进，获取右孩子的下一个 tuple
    rc = right_oper->next();
    if (rc == RC::RECORD_EOF) {
      if (joined_tuple_.left() == nullptr) {
        // 左 tuple 为空说明这是第一次循环，但是右孩子已经没有数据了，直接返回
        break;
      }
      // 重置左 tuple 和右孩子的状态，进行下一轮循环
      joined_tuple_.set_left(nullptr);
      joined_tuple_.set_right(nullptr);
      right_oper->close();
      right_oper->open(trx_);
      continue;
    } else if (rc != RC::SUCCESS) {
      LOG_ERROR("failed to get next tuple from right operator: rc=%s", strrc(rc));
      return rc;
    }
    
    joined_tuple_.set_right(right_oper->current_tuple());

    if (joined_tuple_.left() == nullptr) {
      // 外层循环步进，获取左孩子的下一个 tuple
      rc = left_oper->next();
      if (rc == RC::RECORD_EOF) {
        // 左孩子没有更多数据，直接返回
        break;
      } else if (rc != RC::SUCCESS) {
        LOG_ERROR("failed to get next tuple from left operator: rc=%s", strrc(rc));
        return rc;
      }
      joined_tuple_.set_left(left_oper->current_tuple());
    }

    // 检查是否满足 join 条件
    if (condition_) {
      Value value;
      rc = condition_->get_value(joined_tuple_, value);
      if (rc != RC::SUCCESS) {
        LOG_ERROR("failed to get value from join condition: rc=%s", strrc(rc));
        return rc;
      }

      if (!value.get_boolean()) {
        continue;
      }
    }

    return RC::SUCCESS;
  }

  finished_ = true;
  return RC::RECORD_EOF;
}

// 节点执行完成，清理左右子算子
RC JoinPhysicalOperator::close()
{
  joined_tuple_.set_left(nullptr);
  joined_tuple_.set_right(nullptr);
  finished_ = true;

  children_[0]->close();
  children_[1]->close();
  trx_ = nullptr;

  return RC::SUCCESS;
}

Tuple *JoinPhysicalOperator::current_tuple()
{
  return &joined_tuple_;
}
