#pragma once

#include <stddef.h>

#include <string>

#include "comm/cbdb_wrappers.h"
#include "comm/singleton.h"

namespace pax {

enum PaxColumnCompressType {
  kTypeNoCompress = 0,   // no compress
  kTypeDefaultCompress,  // default compress by column type
  kTypeZSTD,             // used ZSTD
  kTypeZLIB              // used Zlib
};

class PaxCompressor {
 public:
  enum PaxCompressorType {
    kTypeInvalid = 1,    //
    kTypeStreaming = 2,  //
    kTypeBlock = 3
  };

  struct CompressorOptions {
    int compress_level;
  };

  explicit PaxCompressor(const CompressorOptions &compressor_options);

  virtual ~PaxCompressor() = default;

  virtual bool ShouldAlignBuffer() const = 0;

  virtual PaxCompressorType GetCompressorType() const = 0;

  virtual size_t GetCompressBound(size_t src_len) = 0;

  virtual size_t Compress(void *dst_buff, size_t dst_cap, void *src_buff,
                          size_t src_len, int lvl) = 0;

  virtual size_t GetDecompressSize(const void *src_buff, size_t src_len) = 0;

  virtual size_t Decompress(void *dst_buff, size_t dst_len, void *src_buff,
                            size_t src_len) = 0;

  virtual bool IsError(size_t code) = 0;

  virtual const char *ErrorName(size_t code) = 0;

 protected:
  const CompressorOptions &compressor_options_;
};

class PaxZSTDCompressor final : public PaxCompressor {
 public:
  explicit PaxZSTDCompressor(const CompressorOptions &compressor_options);

  PaxCompressor::PaxCompressorType GetCompressorType() const override;

  bool ShouldAlignBuffer() const override;

  size_t GetCompressBound(size_t src_len) override;

  size_t Compress(void *dst_buff, size_t dst_cap, void *src_buff,
                  size_t src_len, int lvl) override;

  size_t GetDecompressSize(const void *src_buff, size_t src_len) override;

  size_t Decompress(void *dst_buff, size_t dst_len, void *src_buff,
                    size_t src_len) override;

  bool IsError(size_t code) override;

  const char *ErrorName(size_t code) override;
};

class PaxZlibCompressor final : public PaxCompressor {
 public:
  explicit PaxZlibCompressor(const CompressorOptions &compressor_options);

  PaxCompressor::PaxCompressorType GetCompressorType() const override;

  bool ShouldAlignBuffer() const override;

  size_t GetCompressBound(size_t src_len) override;

  size_t Compress(void *dst_buff, size_t dst_cap, void *src_buff,
                  size_t src_len, int lvl) override;

  size_t GetDecompressSize(const void *src_buff, size_t src_len) override;

  size_t Decompress(void *dst_buff, size_t dst_cap, void *src_buff,
                    size_t src_len) override;

  bool IsError(size_t code) override;

  const char *ErrorName(size_t code) override;

 private:
  std::string err_msg_;
};

}  // namespace pax
