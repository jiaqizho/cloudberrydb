#pragma once
#include "storage/columns/pax_column.h"
#include "storage/columns/pax_encoding_column.h"
#include "storage/columns/pax_encoding_non_fixed_column.h"
#include "storage/columns/pax_vec_column.h"
#include "storage/columns/pax_vec_encoding_column.h"

namespace pax::traits {

namespace Impl {

template <typename T>
using CreateFunc = std::function<T *(uint64)>;

template <typename T>
static T *CreateImpl(uint64 cap) {
  auto t = new T(cap);
  return t;
}

template <typename T>
using CreateEncodingFunc =
    std::function<T *(uint64, const PaxEncoder::EncodingOption &)>;

template <typename T>
using CreateDecodingFunc =
    std::function<T *(uint64, const PaxDecoder::DecodingOption &)>;

template <typename T>
static T *CreateEncodingImpl(uint64 cap,
                             const PaxEncoder::EncodingOption &encoding_opt) {
  auto t = new T(cap, encoding_opt);
  return t;
}

template <typename T>
static T *CreateDecodingImpl(uint64 cap,
                             const PaxDecoder::DecodingOption &decoding_opt) {
  auto t = new T(cap, decoding_opt);
  return t;
}

}  // namespace Impl

template <template <typename> class T, typename N>
struct ColumnCreateTraits {};

template <class T>
struct ColumnCreateTraits2 {};

#define TRAITS_DECL(_class, _type)                 \
  template <>                                      \
  struct ColumnCreateTraits<_class, _type> {       \
    static Impl::CreateFunc<_class<_type>> create; \
  }

#define TRAITS_DECL2(_class)                \
  template <>                               \
  struct ColumnCreateTraits2<_class> {      \
    static Impl::CreateFunc<_class> create; \
  }

TRAITS_DECL(PaxCommColumn, char);
TRAITS_DECL(PaxCommColumn, int8);
TRAITS_DECL(PaxCommColumn, int16);
TRAITS_DECL(PaxCommColumn, int32);
TRAITS_DECL(PaxCommColumn, int64);
TRAITS_DECL(PaxCommColumn, float);
TRAITS_DECL(PaxCommColumn, double);

TRAITS_DECL(PaxVecCommColumn, char);
TRAITS_DECL(PaxVecCommColumn, int8);
TRAITS_DECL(PaxVecCommColumn, int16);
TRAITS_DECL(PaxVecCommColumn, int32);
TRAITS_DECL(PaxVecCommColumn, int64);
TRAITS_DECL(PaxVecCommColumn, float);
TRAITS_DECL(PaxVecCommColumn, double);

Impl::CreateFunc<PaxCommColumn<char>>
    ColumnCreateTraits<PaxCommColumn, char>::create =
        Impl::CreateImpl<PaxCommColumn<char>>;
Impl::CreateFunc<PaxCommColumn<int8>>
    ColumnCreateTraits<PaxCommColumn, int8>::create =
        Impl::CreateImpl<PaxCommColumn<int8>>;
Impl::CreateFunc<PaxCommColumn<int16>>
    ColumnCreateTraits<PaxCommColumn, int16>::create =
        Impl::CreateImpl<PaxCommColumn<int16>>;
Impl::CreateFunc<PaxCommColumn<int32>>
    ColumnCreateTraits<PaxCommColumn, int32>::create =
        Impl::CreateImpl<PaxCommColumn<int32>>;
Impl::CreateFunc<PaxCommColumn<int64>>
    ColumnCreateTraits<PaxCommColumn, int64>::create =
        Impl::CreateImpl<PaxCommColumn<int64>>;
Impl::CreateFunc<PaxCommColumn<float>>
    ColumnCreateTraits<PaxCommColumn, float>::create =
        Impl::CreateImpl<PaxCommColumn<float>>;
Impl::CreateFunc<PaxCommColumn<double>>
    ColumnCreateTraits<PaxCommColumn, double>::create =
        Impl::CreateImpl<PaxCommColumn<double>>;

Impl::CreateFunc<PaxVecCommColumn<char>>
    ColumnCreateTraits<PaxVecCommColumn, char>::create =
        Impl::CreateImpl<PaxVecCommColumn<char>>;
Impl::CreateFunc<PaxVecCommColumn<int8>>
    ColumnCreateTraits<PaxVecCommColumn, int8>::create =
        Impl::CreateImpl<PaxVecCommColumn<int8>>;
Impl::CreateFunc<PaxVecCommColumn<int16>>
    ColumnCreateTraits<PaxVecCommColumn, int16>::create =
        Impl::CreateImpl<PaxVecCommColumn<int16>>;
Impl::CreateFunc<PaxVecCommColumn<int32>>
    ColumnCreateTraits<PaxVecCommColumn, int32>::create =
        Impl::CreateImpl<PaxVecCommColumn<int32>>;
Impl::CreateFunc<PaxVecCommColumn<int64>>
    ColumnCreateTraits<PaxVecCommColumn, int64>::create =
        Impl::CreateImpl<PaxVecCommColumn<int64>>;
Impl::CreateFunc<PaxVecCommColumn<float>>
    ColumnCreateTraits<PaxVecCommColumn, float>::create =
        Impl::CreateImpl<PaxVecCommColumn<float>>;
Impl::CreateFunc<PaxVecCommColumn<double>>
    ColumnCreateTraits<PaxVecCommColumn, double>::create =
        Impl::CreateImpl<PaxVecCommColumn<double>>;

TRAITS_DECL2(PaxNonFixedColumn);
TRAITS_DECL2(PaxVecNonFixedColumn);

Impl::CreateFunc<PaxNonFixedColumn>
    ColumnCreateTraits2<PaxNonFixedColumn>::create =
        Impl::CreateImpl<PaxNonFixedColumn>;
Impl::CreateFunc<PaxVecNonFixedColumn>
    ColumnCreateTraits2<PaxVecNonFixedColumn>::create =
        Impl::CreateImpl<PaxVecNonFixedColumn>;

template <template <typename> class T, typename N>
struct ColumnOptCreateTraits {};

template <class T>
struct ColumnOptCreateTraits2 {};

#define TRAITS_OPT_DECL(_class, _type)                              \
  template <>                                                       \
  struct ColumnOptCreateTraits<_class, _type> {                     \
    static Impl::CreateEncodingFunc<_class<_type>> create_encoding; \
    static Impl::CreateDecodingFunc<_class<_type>> create_decoding; \
  }

#define TRAITS_OPT_DECL2(_class)                             \
  template <>                                                \
  struct ColumnOptCreateTraits2<_class> {                    \
    static Impl::CreateEncodingFunc<_class> create_encoding; \
    static Impl::CreateDecodingFunc<_class> create_decoding; \
  }

TRAITS_OPT_DECL(PaxEncodingColumn, int8);
TRAITS_OPT_DECL(PaxEncodingColumn, int16);
TRAITS_OPT_DECL(PaxEncodingColumn, int32);
TRAITS_OPT_DECL(PaxEncodingColumn, int64);

TRAITS_OPT_DECL(PaxVecEncodingColumn, int8);
TRAITS_OPT_DECL(PaxVecEncodingColumn, int16);
TRAITS_OPT_DECL(PaxVecEncodingColumn, int32);
TRAITS_OPT_DECL(PaxVecEncodingColumn, int64);

Impl::CreateEncodingFunc<PaxEncodingColumn<int8>>
    ColumnOptCreateTraits<PaxEncodingColumn, int8>::create_encoding =
        Impl::CreateEncodingImpl<PaxEncodingColumn<int8>>;
Impl::CreateEncodingFunc<PaxEncodingColumn<int16>>
    ColumnOptCreateTraits<PaxEncodingColumn, int16>::create_encoding =
        Impl::CreateEncodingImpl<PaxEncodingColumn<int16>>;
Impl::CreateEncodingFunc<PaxEncodingColumn<int32>>
    ColumnOptCreateTraits<PaxEncodingColumn, int32>::create_encoding =
        Impl::CreateEncodingImpl<PaxEncodingColumn<int32>>;
Impl::CreateEncodingFunc<PaxEncodingColumn<int64>>
    ColumnOptCreateTraits<PaxEncodingColumn, int64>::create_encoding =
        Impl::CreateEncodingImpl<PaxEncodingColumn<int64>>;
Impl::CreateDecodingFunc<PaxEncodingColumn<int8>>
    ColumnOptCreateTraits<PaxEncodingColumn, int8>::create_decoding =
        Impl::CreateDecodingImpl<PaxEncodingColumn<int8>>;
Impl::CreateDecodingFunc<PaxEncodingColumn<int16>>
    ColumnOptCreateTraits<PaxEncodingColumn, int16>::create_decoding =
        Impl::CreateDecodingImpl<PaxEncodingColumn<int16>>;
Impl::CreateDecodingFunc<PaxEncodingColumn<int32>>
    ColumnOptCreateTraits<PaxEncodingColumn, int32>::create_decoding =
        Impl::CreateDecodingImpl<PaxEncodingColumn<int32>>;
Impl::CreateDecodingFunc<PaxEncodingColumn<int64>>
    ColumnOptCreateTraits<PaxEncodingColumn, int64>::create_decoding =
        Impl::CreateDecodingImpl<PaxEncodingColumn<int64>>;

Impl::CreateEncodingFunc<PaxVecEncodingColumn<int8>>
    ColumnOptCreateTraits<PaxVecEncodingColumn, int8>::create_encoding =
        Impl::CreateEncodingImpl<PaxVecEncodingColumn<int8>>;
Impl::CreateEncodingFunc<PaxVecEncodingColumn<int16>>
    ColumnOptCreateTraits<PaxVecEncodingColumn, int16>::create_encoding =
        Impl::CreateEncodingImpl<PaxVecEncodingColumn<int16>>;
Impl::CreateEncodingFunc<PaxVecEncodingColumn<int32>>
    ColumnOptCreateTraits<PaxVecEncodingColumn, int32>::create_encoding =
        Impl::CreateEncodingImpl<PaxVecEncodingColumn<int32>>;
Impl::CreateEncodingFunc<PaxVecEncodingColumn<int64>>
    ColumnOptCreateTraits<PaxVecEncodingColumn, int64>::create_encoding =
        Impl::CreateEncodingImpl<PaxVecEncodingColumn<int64>>;
Impl::CreateDecodingFunc<PaxVecEncodingColumn<int8>>
    ColumnOptCreateTraits<PaxVecEncodingColumn, int8>::create_decoding =
        Impl::CreateDecodingImpl<PaxVecEncodingColumn<int8>>;
Impl::CreateDecodingFunc<PaxVecEncodingColumn<int16>>
    ColumnOptCreateTraits<PaxVecEncodingColumn, int16>::create_decoding =
        Impl::CreateDecodingImpl<PaxVecEncodingColumn<int16>>;
Impl::CreateDecodingFunc<PaxVecEncodingColumn<int32>>
    ColumnOptCreateTraits<PaxVecEncodingColumn, int32>::create_decoding =
        Impl::CreateDecodingImpl<PaxVecEncodingColumn<int32>>;
Impl::CreateDecodingFunc<PaxVecEncodingColumn<int64>>
    ColumnOptCreateTraits<PaxVecEncodingColumn, int64>::create_decoding =
        Impl::CreateDecodingImpl<PaxVecEncodingColumn<int64>>;

TRAITS_OPT_DECL2(PaxNonFixedEncodingColumn);
TRAITS_OPT_DECL2(PaxVecNonFixedEncodingColumn);

Impl::CreateEncodingFunc<PaxNonFixedEncodingColumn>
    ColumnOptCreateTraits2<PaxNonFixedEncodingColumn>::create_encoding =
        Impl::CreateEncodingImpl<PaxNonFixedEncodingColumn>;
Impl::CreateDecodingFunc<PaxNonFixedEncodingColumn>
    ColumnOptCreateTraits2<PaxNonFixedEncodingColumn>::create_decoding =
        Impl::CreateDecodingImpl<PaxNonFixedEncodingColumn>;
Impl::CreateEncodingFunc<PaxVecNonFixedEncodingColumn>
    ColumnOptCreateTraits2<PaxVecNonFixedEncodingColumn>::create_encoding =
        Impl::CreateEncodingImpl<PaxVecNonFixedEncodingColumn>;
Impl::CreateDecodingFunc<PaxVecNonFixedEncodingColumn>
    ColumnOptCreateTraits2<PaxVecNonFixedEncodingColumn>::create_decoding =
        Impl::CreateDecodingImpl<PaxVecNonFixedEncodingColumn>;

}  // namespace pax::traits