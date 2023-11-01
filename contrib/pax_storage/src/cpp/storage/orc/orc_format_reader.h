#pragma once

#include "storage/columns/pax_columns.h"
#include "storage/file_system.h"
#include "storage/proto/proto_wrappers.h"
#include "storage/proto/protobuf_stream.h"

namespace pax {
class StripeInformation;

class OrcFormatReader final {
 public:
  explicit OrcFormatReader(File *file);

  ~OrcFormatReader();

  void SetReusedBuffer(DataBuffer<char> *data_buffer);

  void Open();

  void Close();

  size_t GetStripeNums() const;

  size_t GetStripeNumberOfRows(size_t stripe_index);

  PaxColumns *ReadStripe(size_t group_index, bool *proj_map = nullptr,
                         size_t proj_len = 0);

 private:
  StripeInformation *GetStripeInfo(size_t index) const;

  orc::proto::StripeFooter ReadStripeWithProjection(
      DataBuffer<char> *data_buffer, StripeInformation *stripe_info,
      const bool *proj_map, size_t proj_len);

  void BuildProtoTypes();

 private:
  friend class OrcGroupStatsProvider;
  std::vector<orc::proto::Type_Kind> column_types_;
  File *file_;
  DataBuffer<char> *reused_buffer_;
  size_t num_of_stripes_;
  bool is_vec_;

  orc::proto::PostScript post_script_;
  orc::proto::Footer file_footer_;
  orc::proto::Metadata meta_data_;
};

}  // namespace pax
