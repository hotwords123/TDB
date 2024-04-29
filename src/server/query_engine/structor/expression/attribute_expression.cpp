#include "include/query_engine/structor/expression/attribute_expression.h"

std::string RelAttrExpr::_to_string() const {
  std::string res = rel_attr_sql_node_.attribute_name;
  if (!rel_attr_sql_node_.relation_name.empty()) {
    res = rel_attr_sql_node_.relation_name + "." + res;
  }
  return res;
}
