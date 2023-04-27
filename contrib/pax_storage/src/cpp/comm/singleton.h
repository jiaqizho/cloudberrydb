#pragma once

#include <memory>
#include <mutex>
#include <utility>
namespace pax {

template <typename T>
class Singleton final {
 public:
  template <typename... ArgTypes>
  static T* GetInstance(ArgTypes&&... args) {
    std::call_once(
        of_,
        [](ArgTypes&&... args) {
          instance_.reset(new T(std::forward<ArgTypes>(args)...));
        },
        std::forward<ArgTypes>(args)...);

    return instance_.get();
  }

  static inline void destory() {
    if (instance_) {
      instance_.reset();
    }
  }

 private:
  Singleton() = default;
  ~Singleton() = default;
  Singleton(const Singleton&) = delete;
  Singleton& operator=(const Singleton&) = delete;
  static std::once_flag of_;
  static std::unique_ptr<T> instance_;
};

template <class T>
std::once_flag Singleton<T>::of_;

template <class T>
std::unique_ptr<T> Singleton<T>::instance_ = nullptr;
}  // namespace pax
