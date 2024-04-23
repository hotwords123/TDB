#pragma once

#include <utility>
#include <string>

namespace common {

template <typename T, typename Compare>
struct RangeBound {
  T value;
  bool inclusive;
  bool null;

  enum Side {
    LEFT,
    RIGHT,
  };

  RangeBound() : value(), inclusive(), null(true) {}

  template <typename... Args>
  RangeBound(bool inclusive, Args &&...args) :
    value(std::forward<Args>(args)...), inclusive(inclusive), null(false) {}

  /**
   * 比较两个范围的边界。
   * 返回值为正数表示当前边界的范围大于 other，负数表示小于，0 表示相等。
   * comp 是比较函数，返回值为正数表示大于，负数表示小于，0 表示相等。
   * side 表示当前边界是左边界还是右边界。
   */
  int compare(const RangeBound& other, Side side) const {
    if (null || other.null) {
      return null - other.null;
    }

    int ret = Compare{}(value, other.value);
    if (ret == 0) {
      return inclusive - other.inclusive;
    } else {
      return side == LEFT ? -ret : ret;
    }
  }

  void clear() {
    null = true;
  }

  template <typename U>
  void assign(bool inclusive, U &&value) {
    this->value = std::forward<U>(value);
    this->inclusive = inclusive;
    null = false;
  }

  std::string to_string() const {
    if (null) {
      return "null";
    }
    if (inclusive) {
      return '[' + value.to_string() + ']';
    } else {
      return '(' + value.to_string() + ')';
    }
  }
};

}  // namespace common
