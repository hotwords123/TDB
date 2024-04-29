#include "include/query_engine/planner/operator/physical_operator_generator.h"

#include <cmath>
#include <utility>
#include "include/query_engine/planner/node/logical_node.h"
#include "include/query_engine/planner/operator/physical_operator.h"
#include "include/query_engine/planner/node/table_get_logical_node.h"
#include "include/query_engine/planner/operator/table_scan_physical_operator.h"
#include "include/query_engine/planner/operator/index_scan_physical_operator.h"
#include "include/query_engine/planner/node/predicate_logical_node.h"
#include "include/query_engine/planner/operator/predicate_physical_operator.h"
#include "include/query_engine/planner/node/order_by_logical_node.h"
#include "include/query_engine/planner/operator/order_physical_operator.h"
#include "include/query_engine/planner/node/project_logical_node.h"
#include "include/query_engine/planner/operator/project_physical_operator.h"
#include "include/query_engine/planner/node/aggr_logical_node.h"
#include "include/query_engine/planner/operator/aggr_physical_operator.h"
#include "include/query_engine/planner/node/insert_logical_node.h"
#include "include/query_engine/planner/operator/insert_physical_operator.h"
#include "include/query_engine/planner/node/delete_logical_node.h"
#include "include/query_engine/planner/operator/delete_physical_operator.h"
#include "include/query_engine/planner/node/update_logical_node.h"
#include "include/query_engine/planner/operator/update_physical_operator.h"
#include "include/query_engine/planner/node/explain_logical_node.h"
#include "include/query_engine/planner/operator/explain_physical_operator.h"
#include "include/query_engine/planner/node/join_logical_node.h"
#include "include/query_engine/planner/operator/join_physical_operator.h"
#include "include/query_engine/planner/operator/group_by_physical_operator.h"
#include "common/log/log.h"
#include "include/storage_engine/recorder/table.h"
#include "include/storage_engine/index/index.h"
#include "include/query_engine/structor/expression/field_expression.h"
#include "include/query_engine/structor/expression/value_expression.h"
#include "include/query_engine/structor/expression/comparison_expression.h"

using namespace std;

RC PhysicalOperatorGenerator::create(LogicalNode &logical_operator, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  switch (logical_operator.type()) {
    case LogicalNodeType::TABLE_GET: {
      return create_plan(static_cast<TableGetLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::PREDICATE: {
      return create_plan(static_cast<PredicateLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::ORDER: {
      return create_plan(static_cast<OrderByLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::PROJECTION: {
      return create_plan(static_cast<ProjectLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::AGGR: {
      return create_plan(static_cast<AggrLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::INSERT: {
      return create_plan(static_cast<InsertLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::DELETE: {
      return create_plan(static_cast<DeleteLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::UPDATE: {
      return create_plan(static_cast<UpdateLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::EXPLAIN: {
      return create_plan(static_cast<ExplainLogicalNode &>(logical_operator), oper, is_delete);
    }
    // TODO [Lab3] 实现JoinNode到JoinOperator的转换
    case LogicalNodeType::JOIN: {
      return create_plan(static_cast<JoinLogicalNode &>(logical_operator), oper);
    }
    case LogicalNodeType::GROUP_BY: {
      return RC::UNIMPLENMENT;
    }

    default: {
      return RC::INVALID_ARGUMENT;
    }
  }
}

// TODO [Lab2]
// 在原有的实现中，会直接生成TableScanOperator对所需的数据进行全表扫描，但其实在生成执行计划时，我们可以进行简单的优化：
// 首先检查扫描的table是否存在索引，如果存在可以使用的索引，那么我们可以直接生成IndexScanOperator来减少磁盘的扫描
RC PhysicalOperatorGenerator::create_plan(
    TableGetLogicalNode &table_get_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  Table *table = table_get_oper.table();
  string table_alias = table_get_oper.table_alias();
  vector<unique_ptr<Expression>> &predicates = table_get_oper.predicates();

  // TODO [Lab2] 生成IndexScanOperator的准备工作,主要包含:
  // 1. 通过predicates获取具体的值表达式， 目前应该只支持等值表达式的索引查找
    // example:
    //  if(predicate.type == ExprType::COMPARE){
    //   auto compare_expr = dynamic_cast<ComparisonExpr*>(predicate.get());
    //   if(compare_expr->comp != EQUAL_TO) continue;
    //   [process]
    //  }
  // 2. 对应上面example里的process阶段， 找到等值表达式中对应的FieldExpression和ValueExpression(左值和右值)
  // 通过FieldExpression找到对应的Index, 通过ValueExpression找到对应的Value
  Index *scan_index = nullptr;

  unique_ptr<Expression> left_expr;
  unique_ptr<Expression> right_expr;
  bool left_inclusive = false;
  bool right_inclusive = false;

  for (auto index : table->indexes()) {
    // 第一个字段的名字，多重索引由于按字典序排列也能使用
    std::string first_field_name(index->index_meta().field(0));

    for (auto &predicate : predicates) {
      if (predicate->type() != ExprType::COMPARISON) {
        continue;
      }

      auto compare_expr = static_cast<ComparisonExpr *>(predicate.get());
      int comp_op;
      switch (compare_expr->comp()) {
        case CompOp::EQUAL_TO: comp_op = 0; break;
        case CompOp::GREAT_EQUAL: comp_op = 1; break;
        case CompOp::GREAT_THAN: comp_op = 2; break;
        case CompOp::LESS_EQUAL: comp_op = -1; break;
        case CompOp::LESS_THAN: comp_op = -2; break;
        default: continue;
      }

      FieldExpr *field_expr = nullptr;
      Expression *value_expr = nullptr;

      auto check_condition = [&](Expression *lhs, Expression *rhs, bool swapped) {
        if (lhs->type() != ExprType::FIELD) {
          return false;
        }

        field_expr = static_cast<FieldExpr *>(lhs);
        value_expr = rhs;
        // 检查 field 是否相同
        if (field_expr->field().table_alias() != table_alias ||
            field_expr->field_name() != first_field_name) {
          return false;
        }
        // 检查 value_expr 是否可以直接求值（不依赖于当前 table）
        auto is_field_from_table = [&](const Expression *expr) {
          if (expr->type() == ExprType::FIELD) {
            auto field_expr = static_cast<const FieldExpr *>(expr);
            return field_expr->field().table_alias() == table_alias;
          }
          return false;
        };
        if (value_expr->visit(is_field_from_table)) {
          return false;
        }

        if (swapped) {
          comp_op = -comp_op;
        }
        return true;
      };

      // 找到 field = const 或 const = field 的模式
      auto left = compare_expr->left().get();
      auto right = compare_expr->right().get();
      if (!check_condition(left, right, false) &&
          !check_condition(right, left, true)) {
        continue;
      }
      LOG_INFO("found predicate match for index %s: comp_op=%d, expr=%s",
        index->index_meta().name(), comp_op, value_expr->to_string().c_str());

      // 更新边界
      if (comp_op >= 0) {
        left_expr.reset(value_expr->copy());
        left_inclusive = comp_op != 2;
      }
      if (comp_op <= 0) {
        right_expr.reset(value_expr->copy());
        right_inclusive = comp_op != -2;
      }

      // 从 predicates 中移除这个条件
      predicate.reset();

      if (left_expr && right_expr) {
        break;
      }
    }

    if (left_expr || right_expr) {
      scan_index = index;
      auto it = remove_if(predicates.begin(), predicates.end(), [](const unique_ptr<Expression> &expr) {
        return !expr;
      });
      predicates.erase(it, predicates.end());
      break;
    }
  }

  if (scan_index == nullptr) {
    auto table_scan_oper = new TableScanPhysicalOperator(table, table_alias, table_get_oper.readonly());
    table_scan_oper->isdelete_ = is_delete;
    table_scan_oper->set_predicates(std::move(predicates));
    oper = unique_ptr<PhysicalOperator>(table_scan_oper);
    LOG_TRACE("use table scan");
  } else {
    // TODO [Lab2] 生成IndexScanOperator, 并放置在算子树上，下面是一个实现参考，具体实现可以根据需要进行修改
    // IndexScanner 在设计时，考虑了范围查找索引的情况，但此处我们只需要考虑单个键的情况
    // const Value &value = value_expression->get_value();
    // IndexScanPhysicalOperator *operator =
    //              new IndexScanPhysicalOperator(table, index, readonly, &value, true, &value, true);
    // oper = unique_ptr<PhysicalOperator>(operator);
    auto index_scan_oper = make_unique<IndexScanPhysicalOperator>(table, scan_index, table_get_oper.readonly());
    index_scan_oper->set_table_alias(table_alias);
    index_scan_oper->isdelete_ = is_delete;
    index_scan_oper->set_predicates(std::move(predicates));

    index_scan_oper->set_left_expr(std::move(left_expr));
    index_scan_oper->set_left_inclusive(left_inclusive);
    
    index_scan_oper->set_right_expr(std::move(right_expr));
    index_scan_oper->set_right_inclusive(right_inclusive);

    oper = std::move(index_scan_oper);
    LOG_INFO("use index scan: index=%s", scan_index->index_meta().name());
  }

  return RC::SUCCESS;
}

RC PhysicalOperatorGenerator::create_plan(
    PredicateLogicalNode &pred_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<LogicalNode>> &children_opers = pred_oper.children();
  ASSERT(children_opers.size() == 1, "predicate logical operator's sub oper number should be 1");

  LogicalNode &child_oper = *children_opers.front();

  unique_ptr<PhysicalOperator> child_phy_oper;
  RC rc = create(child_oper, child_phy_oper, is_delete);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create child operator of predicate operator. rc=%s", strrc(rc));
    return rc;
  }

  vector<unique_ptr<Expression>> &expressions = pred_oper.expressions();
  ASSERT(expressions.size() == 1, "predicate logical operator's children should be 1");

  unique_ptr<Expression> expression = std::move(expressions.front());

  oper = unique_ptr<PhysicalOperator>(new PredicatePhysicalOperator(std::move(expression)));
  oper->add_child(std::move(child_phy_oper));
  oper->isdelete_ = is_delete;
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(AggrLogicalNode &aggr_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = aggr_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_phy_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  auto *aggr_operator = new AggrPhysicalOperator(&aggr_oper);

  if (child_phy_oper) {
    aggr_operator->add_child(std::move(child_phy_oper));
  }

  oper = unique_ptr<PhysicalOperator>(aggr_operator);

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(OrderByLogicalNode &order_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = order_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_phy_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  OrderPhysicalOperator* order_operator = new OrderPhysicalOperator(std::move(order_oper.order_units()));

  if (child_phy_oper) {
    order_operator->add_child(std::move(child_phy_oper));
  }

  oper = unique_ptr<PhysicalOperator>(order_operator);

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(
    ProjectLogicalNode &project_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<LogicalNode>> &child_opers = project_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_phy_oper, is_delete);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  auto *project_operator = new ProjectPhysicalOperator(&project_oper);
  for (const auto &i : project_oper.expressions()) {
    project_operator->add_projector(i->copy());
  }

  if (child_phy_oper) {
    project_operator->add_child(std::move(child_phy_oper));
  }

  oper = unique_ptr<PhysicalOperator>(project_operator);
  oper->isdelete_ = is_delete;

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(InsertLogicalNode &insert_oper, unique_ptr<PhysicalOperator> &oper)
{
  Table *table = insert_oper.table();
  vector<vector<Value>> multi_values;
  for (int i = 0; i < insert_oper.multi_values().size(); i++) {
    vector<Value> &values = insert_oper.values(i);
    multi_values.push_back(values);
  }
  InsertPhysicalOperator *insert_phy_oper = new InsertPhysicalOperator(table, std::move(multi_values));
  oper.reset(insert_phy_oper);
  return RC::SUCCESS;
}

RC PhysicalOperatorGenerator::create_plan(DeleteLogicalNode &delete_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = delete_oper.children();

  unique_ptr<PhysicalOperator> child_physical_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_physical_oper, true);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  oper = unique_ptr<PhysicalOperator>(new DeletePhysicalOperator(delete_oper.table()));
  oper->isdelete_ = true;
  if (child_physical_oper) {
    oper->add_child(std::move(child_physical_oper));
  }
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(UpdateLogicalNode &update_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = update_oper.children();

  unique_ptr<PhysicalOperator> child_physical_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_physical_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  oper = unique_ptr<PhysicalOperator>(new UpdatePhysicalOperator(update_oper.table(), update_oper.update_units()));

  if (child_physical_oper) {
    oper->add_child(std::move(child_physical_oper));
  }
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(
    ExplainLogicalNode &explain_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<LogicalNode>> &child_opers = explain_oper.children();

  RC rc = RC::SUCCESS;
  unique_ptr<PhysicalOperator> explain_physical_oper(new ExplainPhysicalOperator);
  for (unique_ptr<LogicalNode> &child_oper : child_opers) {
    unique_ptr<PhysicalOperator> child_physical_oper;
    rc = create(*child_oper, child_physical_oper, is_delete);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create child physical operator. rc=%s", strrc(rc));
      return rc;
    }

    explain_physical_oper->add_child(std::move(child_physical_oper));
  }

  oper = std::move(explain_physical_oper);
  oper->isdelete_ = is_delete;
  return rc;
}

// TODO [Lab3] 根据LogicalNode生成对应的PhyiscalOperator
RC PhysicalOperatorGenerator::create_plan(
    JoinLogicalNode &join_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = join_oper.children();
  if (child_opers.size() != 2) {
    LOG_WARN("join operator should have 2 children. size=%d", child_opers.size());
    return RC::INVALID_ARGUMENT;
  }

  auto join_physical_oper = make_unique<JoinPhysicalOperator>();
  join_physical_oper->set_conditions(std::move(join_oper.conditions()));

  for (auto &child_oper : child_opers) {
    unique_ptr<PhysicalOperator> child_physical_oper;
    RC rc = create(*child_oper, child_physical_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create child physical operator. rc=%s", strrc(rc));
      return rc;
    }

    join_physical_oper->add_child(std::move(child_physical_oper));
  }

  oper = std::move(join_physical_oper);
  return RC::SUCCESS;
}
