#include "storage/orc/protobuf_stream.h"

#include "comm/pax_def.h"
#include "exceptions/CException.h"

namespace pax {

void BufferedOutputStream::Set(DataBuffer<char>* data_buffer,
                               uint64_t block_size) {
  Assert(data_buffer);
  data_buffer_ = data_buffer;
  block_size_ = block_size;
}

bool BufferedOutputStream::Next(void** buffer, int* size) {
  uint64_t old_capacity = data_buffer_->Capacity();
  uint64_t new_capacity = data_buffer_->Capacity();

  while (new_capacity < data_buffer_->Used() + block_size_) {
    if (new_capacity == 0) {
      new_capacity += block_size_;
    } else {
      new_capacity += data_buffer_->Capacity();
    }
  }

  if (new_capacity == old_capacity) {  // No resize
    *size = block_size_;
  } else {
    data_buffer_->ReSize(new_capacity);
    *size = static_cast<int>(new_capacity - old_capacity);
  }

  *buffer = data_buffer_->GetBuffer() + data_buffer_->Used();

  data_buffer_->Brush(*size);
  return true;
}

void BufferedOutputStream::BackUp(int count) {
  if (count >= 0) {
    if (static_cast<size_t>(count) > data_buffer_->Used()) {
      CBDB_RAISE(cbdb::CException::ExType::ExTypeIOError);
    }
    data_buffer_->BrushBack(count);
  }
}

google::protobuf::int64 BufferedOutputStream::ByteCount() const {
  return static_cast<google::protobuf::int64>(data_buffer_->Used());
}

bool BufferedOutputStream::WriteAliasedRaw([[maybe_unused]] const void* data,
                                           [[maybe_unused]] int size) {
  return false;
}

bool BufferedOutputStream::AllowsAliasing() const { return false; }

uint64_t BufferedOutputStream::GetSize() const { return data_buffer_->Used(); }

DataBuffer<char>* BufferedOutputStream::GetDataBuffer() const {
  return data_buffer_;
}

void BufferedOutputStream::StartBufferOutRecord() {
  last_used_ = data_buffer_->Used();
}

size_t BufferedOutputStream::EndBufferOutRecord() {
  return data_buffer_->Used() - last_used_;
}

void BufferedOutputStream::DirectWrite(char* ptr, size_t size) {
  if (data_buffer_->Available() < size) {
    data_buffer_->ReSize(data_buffer_->Capacity() + size);
  }
  data_buffer_->Write(ptr, size);
  data_buffer_->Brush(size);
}

bool SeekableInputStream::Next(const void** buffer, int* size) {
  if (data_buffer_.Available() > 0) {
    *buffer = data_buffer_.Position();
    *size = static_cast<int>(data_buffer_.Available());
    data_buffer_.BrushAll();
    return true;
  }
  *size = 0;
  return false;
}

void SeekableInputStream::BackUp(int count) {
  if (count >= 0) {
    if (static_cast<size_t>(count) > data_buffer_.Used()) {
      CBDB_RAISE(cbdb::CException::ExType::ExTypeIOError);
    }
    data_buffer_.BrushBack(count);
  }
}

bool SeekableInputStream::Skip(int count) {
  if (count >= 0) {
    uint64_t unsigned_count = static_cast<uint64_t>(count);
    if (unsigned_count + data_buffer_.Used() <= data_buffer_.Capacity()) {
      data_buffer_.Brush(unsigned_count);
      return true;
    } else {
      data_buffer_.BrushAll();
    }
  }
  return false;
}

google::protobuf::int64 SeekableInputStream::ByteCount() const {
  return static_cast<google::protobuf::int64>(data_buffer_.Used());
}

}  // namespace pax
