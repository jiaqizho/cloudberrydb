#pragma once
#include "comm/cbdb_api.h"

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "storage/pax_defined.h"

namespace pax {

class ColumnStatsProvider {
 public:
  virtual ~ColumnStatsProvider() = default;
  virtual int ColumnSize() const = 0;
  virtual bool AllNull(int column_index) const = 0;
  virtual bool HasNull(int column_index) const = 0;
  virtual uint64 NonNullRows(int column_index) const = 0;
  virtual const ::pax::stats::ColumnBasicInfo &ColumnInfo(
      int column_index) const = 0;
  virtual const ::pax::stats::ColumnDataStats &DataStats(
      int column_index) const = 0;

  virtual bool HasBloomFilter(int column_index) const = 0;
  virtual const ::pax::stats::BloomFilterBasicInfo &BloomFilterBasicInfo(
      int column_index) const = 0;
  virtual std::string GetBloomFilter(int column_index) const = 0;
};

} //  namespace pax