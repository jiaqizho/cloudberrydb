#pragma once

#include <utility>

namespace comm {

template <class F>
class Defer {
  const F function;

 public:
  constexpr explicit Defer(const F& function) : function{function} {}
  constexpr explicit Defer(F&& function) : function{std::move(function)} {}
  ~Defer() { function(); }
};

template <class F>
inline Defer<F> make_defer(F&& function) {
  return Defer<F>(std::forward<F>(function));
}

}  //  namespace comm

#define defer_concat(n, ...) \
  const auto defer##n = comm::make_defer([&] { __VA_ARGS__; })
#define defer_forward(n, ...) defer_concat(n, __VA_ARGS__)
#define defer(...) defer_forward(__LINE__, __VA_ARGS__)
