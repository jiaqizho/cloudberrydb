#include "storage/orc_file_stream.h"

#include <string>

#include "comm/cbdb_wrappers.h"

extern "C" {
#include "catalog/pg_attribute.h"
#include "executor/tuptable.h"
#include "postgres.h"  // NOLINT
}

namespace pax {

// TODO(gongxun): refactor the code later.
std::string BuildSchema(TupleDesc desc) {
  std::string schema_str;
  int n = desc->natts;
  bool is_first_col = true;
  schema_str.append("struct<");
  for (int i = 1; i <= n; i++) {
    FormData_pg_attribute *attr = &desc->attrs[i - 1];
    if (attr->attisdropped) continue;

    if (!is_first_col)
      schema_str.append(",");
    else
      is_first_col = false;

    schema_str.append("col");
    schema_str.append(std::to_string(i));
    schema_str.append(":");

    switch (attr->attlen) {
      case -1:
        schema_str.append("binary");
        break;
      case -2:
        break;
      default:
        if (attr->attbyval) {
          schema_str.append("int");
        } else {
          schema_str.append("binary");
        }
    }
  }
  schema_str.append(">");
  return schema_str;
}

std::unique_ptr<orc::OutputStream> MakeFileOutputStream(File *file) {
  return std::unique_ptr<orc::OutputStream>(new OrcFileOutputStream(file));
}

OrcFileWriter::OrcFileWriter(
    File *file, const OrcFileWriter::OrcWriterOptions &writer_options)
    : options_(writer_options),
      batch_row_index_(0),
      buffer_(orc::DataBuffer<char>(*orc::getDefaultPool(),
                                    writer_options.buffer_size > 0
                                        ? writer_options.buffer_size
                                        : ORC_BUFFER_SIZE)),
      buffer_offset_(0) {
  std::unique_ptr<orc::OutputStream> outStream =
      std::move(MakeFileOutputStream(file));

  std::string ddl = std::move(BuildSchema(writer_options.desc));
  std::unique_ptr<orc::Type> schema(orc::Type::buildTypeFromString(ddl));

  orc::WriterOptions options;
  options.setRowIndexStride(0).setColumnsUseBloomFilter({});

  std::unique_ptr<orc::Writer> writer =
      orc::createWriter(*schema, outStream.get(), options);
  type_ = std::move(schema);
  out_ = std::move(outStream);
  writer_ = std::move(writer);
  batch_ = writer_->createRowBatch(writer_options.batch_row_size);
}

OrcFileWriter::~OrcFileWriter() {}

void OrcFileWriter::Flush() {
  if (batch_row_index_ > 0) {
    batch_.get()->numElements = batch_row_index_;
    writer_->add(*batch_.get());
    batch_row_index_ = 0;
    buffer_offset_ = 0;
  }
}

void OrcFileWriter::Close() {
  Flush();
  writer_->close();
}

void OrcFileWriter::ParseTupleAndWrite(const TupleTableSlot *slot) {
  orc::StructVectorBatch *structBatch =
      dynamic_cast<orc::StructVectorBatch *>(batch_.get());

  TupleDesc tupDesc = slot->tts_tupleDescriptor;
  int n = tupDesc->natts;
  uint32_t row = batch_row_index_;
  Datum *values = slot->tts_values;
  bool *isNull = slot->tts_isnull;

  batch_row_index_++;
  for (int i = 0; i < n; i++) {
    Assert(tupDesc->attrs[i].atttypid == options_.desc->attrs[i].atttypid);
    if (tupDesc->attrs[i].attisdropped) continue;

    orc::ColumnVectorBatch *fieldBatch;
    fieldBatch = structBatch->fields[i];
    if (!isNull[i]) {
      fieldBatch->notNull[row] = true;
    } else {
      if (fieldBatch->hasNulls == false) {
        fieldBatch->hasNulls = true;
      }
      fieldBatch->notNull[row] = false;
    }
    int16_t typlen = tupDesc->attrs[i].attlen;
    bool typbyval = tupDesc->attrs[i].attbyval;
    switch (typlen) {
      case -1: {
        orc::StringVectorBatch *strBatch =
            dynamic_cast<orc::StringVectorBatch *>(structBatch->fields[i]);
        assert(strBatch);
        if (fieldBatch->notNull[row]) {
          struct varlena *vl =
              (struct varlena *)cbdb::PointerFromDatum(values[i]);
          struct varlena *tunpacked = cbdb::PgDeToastDatumPacked(vl);

          int len = VARSIZE_ANY_EXHDR(tunpacked);
          char *data = VARDATA_ANY(tunpacked);

          // TODO(gongxun): use buffer to avoid memory copy.
          if (buffer_.size() < buffer_offset_ + len) {
            buffer_.resize(buffer_.size() * 2);
          }
          memcpy(buffer_.data() + buffer_offset_, data, len);
          strBatch->length[row] = len;
          strBatch->data[row] = buffer_.data() + buffer_offset_;
          buffer_offset_ += len;
        }
        break;
      }
      case -2: {
        throw("unexpect type length");
        break;
      }
      default: {
        if (typlen <= 8 && typbyval) {
          orc::LongVectorBatch *longBatch =
              dynamic_cast<orc::LongVectorBatch *>(structBatch->fields[i]);
          assert(longBatch);
          if (fieldBatch->notNull[row]) {
            longBatch->data[row] = cbdb::Int64FromDatum(values[i]);
          }
        } else {
          orc::StringVectorBatch *strBatch =
              dynamic_cast<orc::StringVectorBatch *>(structBatch->fields[i]);
          assert(strBatch);
          if (fieldBatch->notNull[row]) {
            if (buffer_.size() < buffer_offset_ + typlen) {
              buffer_.resize(buffer_.size() * 2);
            }
            memcpy(buffer_.data() + buffer_offset_,
                   cbdb::PointerFromDatum(values[i]), typlen);
            strBatch->length[row] = typlen;
            strBatch->data[row] = buffer_.data() + buffer_offset_;
            buffer_offset_ += typlen;
          }
        }
      }
    }
  }

  if (batch_row_index_ >= options_.batch_row_size) {
    Flush();
  }
}

OrcFileReader::OrcFileReader(File *file) {
  std::unique_ptr<orc::InputStream> inStream =
      std::unique_ptr<orc::InputStream>(new OrcFileInputStream(file));

  orc::ReaderOptions options;
  orc::RowReaderOptions rowOptions;
  reader_ = orc::createReader(std::move(inStream), options);
  row_reader_ = reader_->createRowReader(rowOptions);
  assert(row_reader_);

  // TODO(gongxun): support multiple stripe
  batch_ = row_reader_->createRowBatch(1);
}

OrcFileReader::~OrcFileReader() {}

bool OrcFileReader::ReadNextBatch(CTupleSlot *cslot) {
  if (!row_reader_->next(*batch_)) {
    return false;
  }

  Datum *datum;
  bool *isNull;
  TupleTableSlot *slot;
  slot = cslot->GetTupleTableSlot();
  datum = slot->tts_values;
  isNull = slot->tts_isnull;

  for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++) {
    readTupleFromBatchVector(&datum[i], &isNull[i], i, 0,
                             slot->tts_tupleDescriptor);
  }
  return true;
}

void OrcFileReader::readTupleFromBatchVector(Datum *datum, bool *isnull, int i,
                                             uint64_t row, TupleDesc desc) {
  orc::StructVectorBatch *structBatch =
      dynamic_cast<orc::StructVectorBatch *>(batch_.get());
  if (structBatch->fields[i]->hasNulls) {
    assert(structBatch->fields[i]->numElements > row);
    if (structBatch->fields[i]->notNull[row]) {
      *isnull = false;
    } else {
      *isnull = true;
    }
  }
  int16 typlen = desc->attrs[i].attlen;
  bool typbyvl = desc->attrs[i].attbyval;
  switch (typlen) {
    case -1: {
      orc::StringVectorBatch *strBatch =
          dynamic_cast<orc::StringVectorBatch *>(structBatch->fields[i]);
      assert(strBatch);

      datum[0] =
          cbdb::DatumFromCString(strBatch->data[row], strBatch->length[row]);
    } break;
    case -2:
      throw "not support cstring pg_type";
    default: {
      if (typlen <= 8) {
        if (typbyvl) {
          orc::LongVectorBatch *longBatch =
              dynamic_cast<orc::LongVectorBatch *>(structBatch->fields[i]);
          assert(longBatch);
          datum[0] = Int64GetDatum(longBatch->data[row]);
          break;
        }
      } else {
        orc::StringVectorBatch *strBatch =
            dynamic_cast<orc::StringVectorBatch *>(structBatch->fields[i]);
        assert(strBatch);
        assert(strBatch->length[row] == typlen);
        // TODO(gongxun): need optimize, avoid memory alloc and memory copy.
        datum[0] = cbdb::DatumFromPointer(strBatch->data[row], typlen);
      }
    }
  }
}

}  // namespace pax
