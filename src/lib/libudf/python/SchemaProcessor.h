#ifndef SCHEMAPROCESSOR_H
#define SCHEMAPROCESSOR_H

// /opt/xcalar/include/arrow/vendored/string_view.hpp
// https://github.com/martinmoene/string-view-lite/commit/2f2cce35293b0027056e5449b2c05b5f9c3e89ff
#ifndef nssv_CPP11
#define nssv_CPP11 1
#endif

#include "primitives/Primitives.h"
#include <jansson.h>
#include <parquet/api/reader.h>

class SchemaProcessor
{
  public:
    virtual  ~SchemaProcessor() {}
    virtual MustCheck Status run(const char* data, int64_t dataSize) = 0;
    MustCheck Status jsonifySchema(char **jsonOutStr);

  protected:
    MustCheck Status setStatus(Status status);
    MustCheck Status checkNumColumns();
    MustCheck Status checkAvailPathBuff(size_t bytesNeeded);
    MustCheck Status checkAvailColNameBuff(size_t bytesNeeded);
    MustCheck Status addArrayElement(int64_t arrIdx);
    MustCheck Status addObjectElement(const char *name);
    char cleanColNameChar(char c, size_t idx)
    {
        if ('A' <= c && c <= 'Z') return c;
        if ('a' <= c && c <= 'z') return c - ('a' - 'A');
        if ('0' <= c && c <= '9' && idx != 0) return c;
        return '_';
    }

    static const uint64_t maxNumCols_ = 1000;
    static const uint64_t maxJsonPathLen_ = 512;
    static const uint64_t maxColNameLen_ = 255;
    static const uint64_t colNameBuffLen_ = 512;

    static const char typeSeparatorChar_ = '|';
    // FIXED_LEN_BYTE_ARRAY(20) + TIMESTAMP_MICROS(16) + 2
    static const uint8_t maxSrcFieldTypeStrLen_ = 38;
    static const uint8_t maxDfFieldTypeStrLen_ = 32;
    static const uint8_t maxStatusStrLen_ = 64;
    // ^ Should find a way of finding max value this could be.

    char pathMapBuff_[maxJsonPathLen_];
    char colNameBuff_[colNameBuffLen_];
    int64_t pathMapPtrPos_ = 0;
    int64_t colNamePtrPos_ = 0;
    int64_t colNamePtrStartPos_ = 0;

    struct ColumnInfo {
        char columnName[maxColNameLen_];
        char jsonPathMapping[maxJsonPathLen_];
        char strDfFieldType[maxDfFieldTypeStrLen_];
        char strSrcFieldType[maxSrcFieldTypeStrLen_];
    };

    struct Schema {
        Schema() = default;
        ~Schema() {
            for (uint64_t ii = 0; ii < numColumns; ii++) {
                delete (columns[ii]);
            }
        }
        ColumnInfo *columns[maxNumCols_];
        char status[maxStatusStrLen_];
        uint64_t numColumns = 0;
    };

    Schema schema_;
};


class ParquetSchemaProcessor: public SchemaProcessor
{
  public:
    MustCheck Status run(const char* data, int64_t dataSize);

  private:
    MustCheck Status findColumns(parquet::schema::NodePtr node);
    MustCheck Status walkObject(parquet::schema::NodePtr node);
    MustCheck Status walkArray(parquet::schema::NodePtr node);
    MustCheck Status addColumn(parquet::schema::NodePtr node);
    MustCheck Status copyColumnTypes(parquet::schema::NodePtr node,
                                     std::string &destDfFieldType,
                                     std::string &destSrcType);
};

// Possible future improvement:
// From Python, we call the JsonSchemaProcessor for each row
// within the sampled rows. It might take less resources to
// pass in the entire sampled portion of JSONL. Then we could
// allocate memory for a single schema, but reset()? the schema
// after the schema for each row is found. Not sure how much, if
// any, effect this will have.
class JsonSchemaProcessor: public SchemaProcessor
{
  public:
    MustCheck Status run(const char* data, int64_t dataSize);

  private:
    MustCheck Status findColumns(json_t *node);
    MustCheck Status walkObject(json_t *node);
    MustCheck Status walkArray(json_t *node);
    MustCheck Status addColumn(json_t *node);
    MustCheck Status copyColumnTypes(json_t *node,
                                     std::string &dfFieldTypeStrBuff,
                                     std::string &srcFieldTypeStrBuff);
};

#endif // SCHEMAPROCESSOR_H
