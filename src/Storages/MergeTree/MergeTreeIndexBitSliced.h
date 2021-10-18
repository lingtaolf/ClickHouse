#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <roaring.hh>
#include <roaring64map.hh>
#include <Storages/MergeTree/RPNBuilder.h>
#include "MergeTreeIndexConditionBloomFilter.h"

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

    BitSlices &  getBitSlicesByCol(const size_t & col_number) 
    {
        if (!bit_slices_vector.empty())
            return bit_slices_vector.at(col_number);
        else
            throw Exception();
    }

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
    struct RPNElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
            FUNCTION_IN,
            FUNCTION_NOT_IN,
            FUNCTION_MULTI_SEARCH,
            FUNCTION_UNKNOWN, /// Can take any value.
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        RPNElement(
            Function function_ = FUNCTION_UNKNOWN, size_t key_column_ = 0, std::list<uint> binary_arr_ = nullptr)
            : function(function_), key_column(key_column_), binary_arr(binary_arr_) {}

        Function function = FUNCTION_UNKNOWN;
        /// For FUNCTION_EQUALS, FUNCTION_NOT_EQUALS and FUNCTION_MULTI_SEARCH
        size_t key_column;
        // List to represent a integer base on binary like '1001'
        std::list<uint> binary_arr;
    };

    MergeTreeIndexBitSlicedCondition(const SelectQueryInfo & info_, ContextPtr context_, const Block & header_);
    ~MergeTreeIndexBitSlicedCondition() override = default;

    bool alwaysUnknownOrTrue() const override { return true; }

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule) const override;

    bool atomFromAST(const ASTPtr & node, Block & block_with_constants, RPNElement & out) { return false; }

private:
    using RPN = std::vector<RPNElement>;

    const Block & header;
    const SelectQueryInfo & query_info;
    Names index_columns;
    RPN rpn;
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

};

}
