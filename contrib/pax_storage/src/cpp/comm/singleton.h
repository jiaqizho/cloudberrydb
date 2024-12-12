#pragma once

namespace pax {

template <typename T>
class Singleton final {
 public:
  Singleton(const Singleton &) = delete;
  Singleton &operator=(const Singleton &) = delete;

  template <typename... Args>
  inline static T *GetInstance(Args &&...args) {
    static T instance{std::forward<Args>(args)...};
    return &instance;
  }

 protected:
  Singleton() = default;
  ~Singleton() = default;
};

}  // namespace pax
