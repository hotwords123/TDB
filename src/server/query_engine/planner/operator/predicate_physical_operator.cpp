#include "common/log/log.h"
#include "include/query_engine/planner/operator/predicate_physical_operator.h"
#include "include/storage_engine/recorder/record.h"
#include "include/query_engine/structor/expression/conjunction_expression.h"
#include "include/query_engine/structor/expression/comparison_expression.h"
#include "include/query_engine/structor/tuple/join_tuple.h"

PredicatePhysicalOperator::PredicatePhysicalOperator(std::unique_ptr<Expression> expr) : expression_(std::move(expr))
{
  ASSERT(expression_->value_type() == BOOLEANS, "predicate's expression should be BOOLEAN type");
}

std::string PredicatePhysicalOperator::param() const
{
  return expression_->to_string();
}

RC PredicatePhysicalOperator::open(Trx *trx)
{
  RC rc = RC::SUCCESS;
  if (expression_->type() == ExprType::CONJUNCTION) {
    rc = dynamic_cast<ConjunctionExpr *>(expression_.get())->set_trx(trx);
  } else if (expression_->type() == ExprType::COMPARISON) {
    rc = dynamic_cast<ComparisonExpr *>(expression_.get())->set_trx(trx);
  }

  if (rc != RC::SUCCESS) {
    LOG_WARN("predicate operator failed to set trx to sub expression");
    return RC::INTERNAL;
  }

  if (children_.size() != 1) {
    LOG_WARN("predicate operator must has one child");
    return RC::INTERNAL;
  }

  return children_[0]->open(trx);
}

RC PredicatePhysicalOperator::next()
{
  RC rc;
  PhysicalOperator *oper = children_.front().get();

  while (RC::SUCCESS == (rc = oper->next())) {
    Tuple *tuple = oper->current_tuple();
    if (nullptr == tuple) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to get tuple from operator");
      break;
    }

    JoinedTuple joined_tuple;
    joined_tuple.set_left(tuple);
    joined_tuple.set_right(const_cast<Tuple *>(father_tuple_));

    Value value;
    rc = expression_->get_value(joined_tuple, value);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    if (value.get_boolean()) {
      return rc;
    }
  }
  return rc;
}

RC PredicatePhysicalOperator::close()
{
  children_[0]->close();
  return RC::SUCCESS;
}

Tuple *PredicatePhysicalOperator::current_tuple()
{
  return children_[0]->current_tuple();
}
