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

std::string JoinPhysicalOperator::param() const
{
  std::string res;
  for (size_t i = 0; i < conditions_.size(); i++) {
    res += i == 0 ? "ON " : " AND ";
    res += conditions_[i]->to_string();
  }
  return res;
}

// 执行next()前的准备工作, trx是之后事务中会使用到的，这里不用考虑
RC JoinPhysicalOperator::open(Trx *trx)
{
  trx_ = trx;

  joined_tuple_.set_left(nullptr);
  joined_tuple_.set_right(nullptr);
  joined_father_tuple_.set_left(const_cast<Tuple *>(father_tuple_));

  state_ = OUTER_LOOP_START;
  return RC::SUCCESS;
}

// 计算出接下来需要输出的数据，并将结果set到join_tuple中
// 如果没有更多数据，返回RC::RECORD_EOF
RC JoinPhysicalOperator::next()
{
  PhysicalOperator *left_oper = children_[0].get();
  PhysicalOperator *right_oper = children_[1].get();

  RC rc = RC::SUCCESS;

  // 实现 Nested Loop Join
  while (true) {
    switch (state_) {
      case OUTER_LOOP_START: {
        // 进入外层循环，初始化左算子
        rc = left_oper->open(trx_);
        if (rc != RC::SUCCESS) {
          LOG_ERROR("failed to open left operator: rc=%s", strrc(rc));
          return rc;
        }
        state_ = OUTER_LOOP;
        [[fallthrough]];
      }

      case OUTER_LOOP: {
        // 外层循环步进，获取左算子的下一个 tuple
        rc = left_oper->next();
        if (rc == RC::RECORD_EOF) {
          // 外层循环结束，算子执行完成
          joined_tuple_.set_left(nullptr);
          left_oper->close();
          state_ = FINISHED;
          return RC::RECORD_EOF;
        } else if (rc != RC::SUCCESS) {
          LOG_ERROR("failed to get next tuple from left operator: rc=%s", strrc(rc));
          return rc;
        }

        // 获取左算子的 tuple
        Tuple *left_tuple = left_oper->current_tuple();
        joined_tuple_.set_left(left_tuple);
        joined_father_tuple_.set_right(left_tuple);

        // 进入内层循环
        state_ = INNER_LOOP_START;
        [[fallthrough]];
      }

      case INNER_LOOP_START: {
        // 进入内层循环，初始化右算子
        right_oper->set_father_tuple(&joined_father_tuple_);
        rc = right_oper->open(trx_);
        if (rc != RC::SUCCESS) {
          LOG_ERROR("failed to open right operator: rc=%s", strrc(rc));
          return rc;
        }
        state_ = INNER_LOOP;
        [[fallthrough]];
      }

      case INNER_LOOP: {
        // 内层循环步进，获取右孩子的下一个 tuple
        rc = right_oper->next();
        if (rc == RC::RECORD_EOF) {
          // 内层循环结束，重置右算子的状态，进行下一轮循环
          joined_tuple_.set_right(nullptr);
          right_oper->close();
          state_ = OUTER_LOOP;
          continue;
        } else if (rc != RC::SUCCESS) {
          LOG_ERROR("failed to get next tuple from right operator: rc=%s", strrc(rc));
          return rc;
        }

        joined_tuple_.set_right(right_oper->current_tuple());

        // 检查是否满足 join 条件
        bool result;
        rc = filter(joined_tuple_, result);
        if (rc != RC::SUCCESS) {
          LOG_ERROR("failed to filter joined tuple: rc=%s", strrc(rc));
          return rc;
        }
        if (result) {
          return RC::SUCCESS;
        }

        break;
      }

      case FINISHED:
        return RC::RECORD_EOF;
    }
  }
}

RC JoinPhysicalOperator::filter(JoinedTuple &tuple, bool &result)
{
  JoinedTuple full_tuple;
  full_tuple.set_left(const_cast<Tuple *>(father_tuple_));
  full_tuple.set_right(&tuple);

  RC rc = RC::SUCCESS;
  Value value;
  for (auto &expr : conditions_) {
    rc = expr->get_value(full_tuple, value);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("failed to get value from join condition: rc=%s", strrc(rc));
      return rc;
    }

    if (!value.get_boolean()) {
      result = false;
      return rc;
    }
  }

  result = true;
  return rc;
}

// 节点执行完成，清理左右子算子
RC JoinPhysicalOperator::close()
{
  switch (state_) {
    case INNER_LOOP:
      joined_tuple_.set_right(nullptr);
      children_[1]->close();
      [[fallthrough]];

    case INNER_LOOP_START:
      [[fallthrough]];

    case OUTER_LOOP:
      joined_tuple_.set_left(nullptr);
      children_[0]->close();
      [[fallthrough]];

    case OUTER_LOOP_START:
      [[fallthrough]];

    case FINISHED:
      joined_father_tuple_.set_left(nullptr);
      break;
  }
  
  state_ = FINISHED;
  trx_ = nullptr;

  return RC::SUCCESS;
}

Tuple *JoinPhysicalOperator::current_tuple()
{
  return &joined_tuple_;
}
