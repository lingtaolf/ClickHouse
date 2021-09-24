#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <roaring.hh>
#include <roaring64map.hh>
#include <Interpreters/Context_fwd.h>


namespace DB
{
using RoaringBitmap = roaring::Roaring64Map;
using RoaringBitmapPtr = std::shared_ptr<RoaringBitmap>;
using BitSlices = std::vector<RoaringBitmapPtr>;
using BitSlicesVector = std::vector<BitSlices>;
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

    MergeTreeIndexGranuleBSI(){}

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override;

private:
    String index_name;
    BitSlicesVector bit_slices_vector;
    std::vector<size_t> bit_slices_sizes;
    Block index_sample_block;
};

class MergeTreeIndexAggregatorBSI final : public IMergeTreeIndexAggregator
{
public:
    MergeTreeIndexAggregatorBSI(const String & index_name_, const Block & index_smaple_block_)
    : index_name(index_name_),
      index_sample_block(index_smaple_block_)
    {}

    bool empty() const override;
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;
private:
    String index_name;
    Block index_sample_block;
    BitSlicesVector bit_slices_vector;
    std::vector<size_t> bit_slices_sizes;

    void columnToBitSlices(UInt64 value, size_t col);
};

class MergeTreeIndexBitSlicedCondition final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexBitSlicedCondition(const SelectQueryInfo & /*info_*/){}

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule) const override;
private:
    // const SelectQueryInfo & query_info;
    //const Block header;
};

class MergeTreeIndexBitSliced final : public IMergeTreeIndex
{
public:
    MergeTreeIndexBitSliced(const IndexDescription & index_): IMergeTreeIndex(index_){}
    bool mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const override { return true; }
    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const SelectQueryInfo & query_info, ContextPtr /*context*/) const override;

};

}
