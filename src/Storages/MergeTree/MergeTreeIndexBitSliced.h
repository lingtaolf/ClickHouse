#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <roaring.hh>
#include <roaring64map.hh>
#include "IO/WriteBuffer.h"
#include <Storages/MergeTree/KeyCondition.h>
#include <sys/types.h>

namespace DB
{
using RoaringBitmap = roaring::Roaring64Map;
using RoaringBitmapPtr = std::shared_ptr<RoaringBitmap>;
using BitSlices = std::vector<RoaringBitmapPtr>;
using BitSlicesVector = std::vector<BitSlices>;

struct ColumnBitSlices
{
    uint bits_size;
    BitSlices bit_slices;
};

class MergeTreeIndexGranuleBSI final : public IMergeTreeIndexGranule
{
public:
    MergeTreeIndexGranuleBSI(String & index_name_, std::vector<ColumnBitSlices> & columns_bit_slices_, Block & index_sample_block_)
        : index_name(index_name_),
          columns_bit_slices(columns_bit_slices_),
          index_sample_block(index_sample_block_)
    {
    }

    MergeTreeIndexGranuleBSI()= default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override;

    ColumnBitSlices &  getBitSlicesByCol(const size_t & col_number) 
    {
        if (!columns_bit_slices.empty())
            return columns_bit_slices.at(col_number);
        else
            throw Exception();
    }

    String index_name;
    std::vector<ColumnBitSlices> columns_bit_slices;
    // this is for deserialize bit slices
    Block index_sample_block;
    // const IndexDescription & index;
};

class MergeTreeIndexAggregatorBSI final : public IMergeTreeIndexAggregator
{
public:
    MergeTreeIndexAggregatorBSI(const String & index_name_, const Block & index_smaple_block_)
    : index_name(index_name_),
      index_sample_block(index_smaple_block_)
    {
    }

    bool empty() const override;
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;
private:
    String index_name;
    Block index_sample_block;
    std::vector<ColumnBitSlices> columns_bit_slices;

    void columnToBitSlices(UInt64 value, const size_t & col, const size_t & row);
};


class MergeTreeIndexBitSlicedCondition final : public IMergeTreeIndexCondition
{
public:
     
    MergeTreeIndexBitSlicedCondition(
       const IndexDescription & index,
        const SelectQueryInfo & query,
        ContextPtr context);

    ~MergeTreeIndexBitSlicedCondition() override = default;
    
    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule) const override;

    DataTypes index_data_types;
    KeyCondition condition;
};

class MergeTreeIndexBitSliced final : public IMergeTreeIndex
{
public:
    MergeTreeIndexBitSliced(const IndexDescription & index_): IMergeTreeIndex(index_){}
    ~MergeTreeIndexBitSliced() override = default;

    bool mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const override { return true; }
    //This method is for deserialize the granuale
    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const SelectQueryInfo & query_info, ContextPtr /*context*/) const override;
    const char* getSerializedFileExtension() const override { return ".idx2"; }
    MergeTreeIndexFormat getDeserializedFormat(DiskPtr disk, const std::string & path_prefix) const override;

};


}
