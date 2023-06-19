#pragma once

#include <utility>

namespace pax {

template <class F>
class Defer {
 public:
  const F function;

 public:
  constexpr explicit Defer(const F &function) : function{function} {}
  constexpr explicit Defer(F &&function) : function{std::move(function)} {}
  ~Defer() { function(); }
};

template <class F>
inline Defer<F> make_defer(F &&function) {
  return Defer<F>(std::forward<F>(function));
}

}  //  namespace pax

#define DEFER_CONCAT(n, ...) \
  const auto defer##n = pax::make_defer([&] { __VA_ARGS__; })
#define DEFER_FORWARD(n, ...) DEFER_CONCAT(n, __VA_ARGS__)
#define DEFER(...) DEFER_FORWARD(__LINE__, __VA_ARGS__)
