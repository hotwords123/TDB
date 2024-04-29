#include "include/query_engine/structor/expression/expression.h"

std::string Expression::to_string() const {
  std::string res = _to_string();
  if (!alias_.empty() && res != alias_) {
      res += " AS ";
      res += alias_;
  }
  return res;
}

std::string Expression::_to_string() const {
  return name_.empty() ? "?" : name_;
}
