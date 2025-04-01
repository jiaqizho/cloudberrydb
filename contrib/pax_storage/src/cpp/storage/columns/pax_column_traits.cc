#include "storage/columns/pax_column_traits.h"

namespace pax::traits {

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
Impl::CreateFunc<PaxNonFixedColumn>
    ColumnCreateTraits2<PaxNonFixedColumn>::create =
        Impl::CreateImpl<PaxNonFixedColumn>;
Impl::CreateFunc<PaxVecNonFixedColumn>
    ColumnCreateTraits2<PaxVecNonFixedColumn>::create =
        Impl::CreateImpl<PaxVecNonFixedColumn>;

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

Impl::CreateEncodingFunc<PaxBpCharColumn>
    ColumnOptCreateTraits2<PaxBpCharColumn>::create_encoding =
        Impl::CreateEncodingImpl<PaxBpCharColumn>;
Impl::CreateDecodingFunc<PaxBpCharColumn>
    ColumnOptCreateTraits2<PaxBpCharColumn>::create_decoding =
        Impl::CreateDecodingImpl<PaxBpCharColumn>;

Impl::CreateEncodingFunc<PaxPgNumericColumn>
    ColumnOptCreateTraits2<PaxPgNumericColumn>::create_encoding =
        Impl::CreateEncodingImpl<PaxPgNumericColumn>;
Impl::CreateDecodingFunc<PaxPgNumericColumn>
    ColumnOptCreateTraits2<PaxPgNumericColumn>::create_decoding =
        Impl::CreateDecodingImpl<PaxPgNumericColumn>;

Impl::CreateEncodingFunc<PaxShortNumericColumn>
    ColumnOptCreateTraits2<PaxShortNumericColumn>::create_encoding =
        Impl::CreateEncodingImpl<PaxShortNumericColumn>;
Impl::CreateDecodingFunc<PaxShortNumericColumn>
    ColumnOptCreateTraits2<PaxShortNumericColumn>::create_decoding =
        Impl::CreateDecodingImpl<PaxShortNumericColumn>;

Impl::CreateEncodingFunc<PaxBitPackedColumn>
    ColumnOptCreateTraits2<PaxBitPackedColumn>::create_encoding =
        Impl::CreateEncodingImpl<PaxBitPackedColumn>;
Impl::CreateDecodingFunc<PaxBitPackedColumn>
    ColumnOptCreateTraits2<PaxBitPackedColumn>::create_decoding =
        Impl::CreateDecodingImpl<PaxBitPackedColumn>;

Impl::CreateEncodingFunc<PaxVecBitPackedColumn>
    ColumnOptCreateTraits2<PaxVecBitPackedColumn>::create_encoding =
        Impl::CreateEncodingImpl<PaxVecBitPackedColumn>;
Impl::CreateDecodingFunc<PaxVecBitPackedColumn>
    ColumnOptCreateTraits2<PaxVecBitPackedColumn>::create_decoding =
        Impl::CreateDecodingImpl<PaxVecBitPackedColumn>;

}  // namespace pax::traits
