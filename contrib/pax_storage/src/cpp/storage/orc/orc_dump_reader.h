#pragma once
#include <cstdlib>
#include <string>
namespace pax {
class OrcFormatReader;
namespace tools {

#define NO_SPEC_ID -1
#define NO_SPEC_LEN 0

struct DumpConfig {
  char *file_name = nullptr;
  char *toast_file_name = nullptr;
  bool print_all = false;
  bool print_all_desc = false;
  bool print_post_script = false;
  bool print_footer = false;
  bool print_schema = false;
  bool print_group_info = false;
  bool print_group_footer = false;
  bool print_all_data = false;

  int64_t group_id_start = NO_SPEC_ID;
  int64_t group_id_len = NO_SPEC_LEN;

  int64_t column_id_start = NO_SPEC_ID;
  int64_t column_id_len = NO_SPEC_LEN;

  int64_t row_id_start = NO_SPEC_ID;
  int64_t row_id_len = NO_SPEC_LEN;
};

class OrcDumpReader final {
 public:
  explicit OrcDumpReader(DumpConfig *config);

  bool Open();
  std::string Dump();
  void Close();

 private:
  std::string DumpAllInfo();
  std::string DumpAllDesc();
  std::string DumpPostScript();
  std::string DumpFooter();
  std::string DumpSchema();
  std::string DumpGroupInfo();
  std::string DumpGroupFooter();
  std::string DumpAllData();

 private:
  DumpConfig *config_;
  OrcFormatReader *format_reader_;
};

}  // namespace tools
}  // namespace pax
