#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <roaring.hh>
#include <roaring64map.hh>
#include "IO/WriteBuffer.h"
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{
using RoaringBitmap = roaring::Roaring64Map;
using RoaringBitmapPtr = std::shared_ptr<RoaringBitmap>;
using BitSlices = std::vector<RoaringBitmapPtr>;
using BitSlicesVector = std::vector<BitSlices>;

struct ColumnBitSlices
{

};

class MergeTreeIndexGranuleBSI final : public IMergeTreeIndexGranule
{
public:
    MergeTreeIndexGranuleBSI(String & index_name_, BitSlicesVector bit_slices_vector_, std::vector<size_t> bit_slices_sizes_, Block & index_sample_block_)
        : index_name(index_name_),
          bit_slices_vector(bit_slices_vector_),
          bit_slices_sizes(bit_slices_sizes_),
          index_sample_block(index_sample_block_)
    {
        for (auto & bit_slices : bit_slices_vector)
        {
            bit_slices_sizes.emplace_back(bit_slices.size());
        }
    }

    MergeTreeIndexGranuleBSI()= default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override;

    BitSlices &  getBitSlicesByCol(const size_t & col_number) 
    {
        if (!bit_slices_vector.empty())
            return bit_slices_vector.at(col_number);
        else
            throw Exception();
    }

    String index_name;
    BitSlicesVector bit_slices_vector;
    // this is for deserialize bit slices
    std::vector<size_t> bit_slices_sizes;
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
        bit_slices_vector.resize(index_sample_block.columns());
        bit_slices_sizes.resize(index_sample_block.columns());
    }

    bool empty() const override;
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;
private:
    String index_name;
    Block index_sample_block;
    BitSlicesVector bit_slices_vector;
    std::vector<size_t> bit_slices_sizes;

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
    bool mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const override { return true; }
    //This method is for deserialize the granuale
    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const SelectQueryInfo & query_info, ContextPtr /*context*/) const override;
    const char* getSerializedFileExtension() const override { return ".idx2"; }
    MergeTreeIndexFormat getDeserializedFormat(const DiskPtr disk, const std::string & path_prefix) const override;

};

}
