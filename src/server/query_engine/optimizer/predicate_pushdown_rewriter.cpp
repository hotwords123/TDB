#include "include/query_engine/optimizer/predicate_pushdown_rewriter.h"
#include "include/query_engine/planner/node/logical_node.h"
#include "include/query_engine/planner/node/table_get_logical_node.h"
#include "include/query_engine/planner/node/join_logical_node.h"
#include "include/query_engine/structor/expression/value_expression.h"
#include "include/query_engine/structor/expression/conjunction_expression.h"
#include "include/query_engine/structor/expression/comparison_expression.h"
#include "include/query_engine/structor/expression/field_expression.h"

RC PredicatePushdownRewriter::rewrite(std::unique_ptr<LogicalNode> &oper, bool &change_made)
{
  RC rc = RC::SUCCESS;
  if (oper->type() != LogicalNodeType::PREDICATE) {
    return rc;
  }

  if (oper->children().size() != 1) {
    return rc;
  }

  std::unique_ptr<LogicalNode> &child_oper = oper->children().front();

  std::vector<std::unique_ptr<Expression>> &predicate_oper_exprs = oper->expressions();
  if (predicate_oper_exprs.size() != 1) {
    return rc;
  }

  std::unique_ptr<Expression> &predicate_expr = predicate_oper_exprs.front();
  pushdown_expr(predicate_expr, child_oper.get(), change_made);

  if (!predicate_expr) {
    // 所有的表达式都下推到了下层算子，直接删除这个predicate operator
    oper = std::move(child_oper);
    change_made = true;
    // 递归调用，继续下推
    rewrite(oper, change_made);
  }
  return rc;
}

RC PredicatePushdownRewriter::pushdown_expr(
    unique_ptr<Expression> &expr, LogicalNode *oper, bool &change_made)
{
  RC rc = RC::SUCCESS;

  if (expr->type() == ExprType::CONJUNCTION) {
    // 对于复合表达式，分别尝试下推每个子表达式
    ConjunctionExpr *conj_expr = static_cast<ConjunctionExpr *>(expr.get());
    // 或 操作的比较，太复杂，现在不考虑
    if (conj_expr->conjunction_type() != ConjunctionType::AND) {
      return rc;
    }

    auto &child_exprs = conj_expr->children();
    for (auto &child_expr : child_exprs) {
      // 对于每个表达式，分别尝试下推
      rc = pushdown_expr(child_expr, oper, change_made);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to pushdown expression. rc=%s", strrc(rc));
        return rc;
      }
    }

    // 删除所有的空表达式
    auto it = remove_if(child_exprs.begin(), child_exprs.end(), [](const unique_ptr<Expression> &expr) {
      return !expr;
    });
    child_exprs.erase(it, child_exprs.end());

    if (child_exprs.empty()) {
      // 如果所有的表达式都下推成功，就删除当前表达式
      expr.reset();
    } else if (child_exprs.size() == 1) {
      // 如果只有一个表达式，就直接替换当前表达式
      expr = std::move(child_exprs.front());
    }

    return rc;
  }

  // 判断当前节点类型
  if (oper->type() == LogicalNodeType::PREDICATE) {
    // 对于 predicate 节点，继续下推
    if (oper->children().size() != 1) {
      return rc;
    }

    LogicalNode *child_oper = oper->children().front().get();
    rc = pushdown_expr(expr, child_oper, change_made);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to pushdown expression. rc=%s", strrc(rc));
      return rc;
    }
  } else if (oper->type() == LogicalNodeType::TABLE_GET) {
    // 对于 table get 节点，直接下推
    auto table_get_oper = static_cast<TableGetLogicalNode *>(oper);
    table_get_oper->predicates().emplace_back(std::move(expr));
    change_made = true;
  } else if (oper->type() == LogicalNodeType::JOIN) {
    // 对于 join 节点，判断下推到左节点还是右节点
    auto join_oper = static_cast<JoinLogicalNode *>(oper);
    if (join_oper->children().size() != 2) {
      return rc;
    }

    LogicalNode *left_oper = join_oper->children()[0].get();
    LogicalNode *right_oper = join_oper->children()[1].get();

    // 查找右节点中包含的表
    unordered_set<string> right_tables;
    right_oper->visit([&](const LogicalNode *sub_oper) {
      if (sub_oper->type() == LogicalNodeType::TABLE_GET) {
        auto table_get_oper = static_cast<const TableGetLogicalNode *>(sub_oper);
        right_tables.insert(table_get_oper->table_alias());
      }
      return false;
    });

    // 若当前表达式不涉及右节点中的表，则可以下推到左节点，否则只能下推到右节点
    auto is_field_in_right_tables = [&](const Expression *expr) {
      if (expr->type() == ExprType::FIELD) {
        auto field_expr = static_cast<const FieldExpr *>(expr);
        return right_tables.count(field_expr->field().table_alias()) > 0;
      }
      return false;
    };
    if (expr->visit(is_field_in_right_tables)) {
      rc = pushdown_expr(expr, right_oper, change_made);
    } else {
      rc = pushdown_expr(expr, left_oper, change_made);
    }
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to pushdown expression. rc=%s", strrc(rc));
      return rc;
    }

    // 如果没有下推成功，就把表达式下推到 join 节点
    if (expr) {
      join_oper->conditions().emplace_back(std::move(expr));
      change_made = true;
    }
  }

  return rc;
}
