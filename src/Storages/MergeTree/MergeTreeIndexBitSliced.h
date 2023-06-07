#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <roaring.hh>
#include <roaring64map.hh>
#include "Storages/IndicesDescription.h"
#include "Storages/SelectQueryInfo.h"
#include <IO/WriteBuffer.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <sys/types.h>
#include <unordered_map>

namespace DB
{
using RoaringBitmap = roaring::Roaring64Map;
using RoaringBitmapPtr = std::shared_ptr<RoaringBitmap>;
using BitSlices = std::vector<RoaringBitmapPtr>;
using BitSlicesVector = std::vector<BitSlices>;
using NamesAndBitSlices = std::unordered_map<String, BitSlices>;


class MergeTreeIndexGranuleBSI final : public IMergeTreeIndexGranule
{
public:
    MergeTreeIndexGranuleBSI(String & index_name_, NamesAndBitSlices && name_bitslices_, Block & index_sample_block_)
        : index_name(index_name_),
          names_bitslices(name_bitslices_),
          index_sample_block(index_sample_block_)
    {
    }

    MergeTreeIndexGranuleBSI() = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override;

    String index_name;
    NamesAndBitSlices names_bitslices;
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
    NamesAndBitSlices names_bitslices;

    void columnToBitSlices(UInt64 value, const String & col_name, const size_t & row);
};


class MergeTreeIndexBitSlicedCondition final : public IMergeTreeIndexCondition
{
public:
     
    explicit MergeTreeIndexBitSlicedCondition(
        const IndexDescription & index_,
         const SelectQueryInfo & query_,
         ContextPtr context);
    
    ~MergeTreeIndexBitSlicedCondition() override = default;
    
    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule) const override;

    //DataTypes index_data_types;
    IndexDescription index;
    SelectQueryInfo query;
    ContextPtr context;
};

class MergeTreeIndexBitSliced final : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexBitSliced(const IndexDescription & index_): IMergeTreeIndex(index_){}
    ~MergeTreeIndexBitSliced() override = default;

    bool mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const override { return true; }
    //This method is for deserialize the granuale
    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const SelectQueryInfo & query_info, ContextPtr /*context*/) const override;
    const char* getSerializedFileExtension() const override { return ".idx2"; }
    MergeTreeIndexFormat getDeserializedFormat(const IDataPartStorage & data_part_storage, const std::string & path_prefix) const override; /// NOLINT

};


}
