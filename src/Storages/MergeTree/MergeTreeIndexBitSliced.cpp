#include "MergeTreeIndexBitSliced.h"
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_QUERY;
extern const int BAD_ARGUMENTS;
}


void MergeTreeIndexGranuleBSI::serializeBinary(DB::WriteBuffer & ostr) const
{

    for (auto iter = bit_slices_sizes.begin(); iter != bit_slices_sizes.end(); iter++)
        writeVarUInt(*iter, ostr);

    for (size_t col = 0; col < index_sample_block.columns(); col++)
    {
        for (auto bit_slice : bit_slices_vector[col])
        {
            auto size = bit_slice->getSizeInBytes();
            std::unique_ptr<char[]> buf(new char[size]);
            bit_slice->write(buf.get());
            ostr.write(buf.get(), size);
        }
    }

}


void MergeTreeIndexGranuleBSI::deserializeBinary(DB::ReadBuffer & istr, DB::MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    for (size_t i = 0; i < index_sample_block.columns(); i++)
    {
        size_t size;
        readVarUInt(size, istr);
        bit_slices_sizes.emplace_back(size);
    }

    bit_slices_vector.resize(index_sample_block.columns());
    size_t index = 0;
    for (auto & bit_slices : bit_slices_vector)
    {
        size_t bit_slices_size = bit_slices_sizes[index];

        for (size_t i = 0; i < bit_slices_size; i++)
        {
            size_t size;
            readVarUInt(size, istr);
            std::unique_ptr<char[]> buf(new char[size]);
            istr.readStrict(buf.get(), size);
            bit_slices.emplace_back(std::make_shared<RoaringBitmap>(RoaringBitmap::read(buf.get())));
        }
    }
}


bool MergeTreeIndexGranuleBSI::empty() const
{
    return bit_slices_vector.empty();
}


MergeTreeIndexPtr bitSlicedIndexCreator(const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexBitSliced>(index);
}


MergeTreeIndexAggregatorPtr MergeTreeIndexBitSliced::createIndexAggregator() const
{

    return std::make_shared<MergeTreeIndexAggregatorBSI>(index.name, index.sample_block);
}


void bitSlicedIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    for (const auto & data_type : index.data_types)
    {
        if (data_type->getTypeId() != TypeIndex::UInt8
            && data_type->getTypeId() != TypeIndex::UInt32
            && data_type->getTypeId() != TypeIndex::UInt64
            && data_type->getTypeId() != TypeIndex::UInt128
            && data_type->getTypeId() != TypeIndex::UInt256)
            throw Exception("Bit sliced index can be used only with positive integer type.", ErrorCodes::INCORRECT_QUERY);
    }
}

bool MergeTreeIndexAggregatorBSI::empty() const
{
    return false;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorBSI::getGranuleAndReset()
{
    return std::make_shared<MergeTreeIndexGranuleBSI>(index_name, std::move(bit_slices_vector),
                                                      std::move(bit_slices_sizes), index_sample_block);
}

void MergeTreeIndexAggregatorBSI::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            "The provided position is not less than the number of block rows. Position: "
            + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

    for (size_t col = 0; col < index_sample_block.columns(); ++col)
    {
        auto index_column_name = index_sample_block.getByPosition(col).name;
        const auto & column = block.getByName(index_column_name).column->cut(*pos, rows_read);

        for (size_t i = 0; i < rows_read; ++i)
        {
            UInt64 value = static_cast<UInt64>(column->getUInt(*pos + i));
            columnToBitSlices(value, col);
        }
    }

    *pos += rows_read;
}

void MergeTreeIndexAggregatorBSI::columnToBitSlices(UInt64 value, size_t col)
{
    //Decimal to binary
    std::list<uint> l;

    while(value!=0)
    {
        l.push_front(value%2);
        value = value >> 1;
    }

    BitSlices bit_slices = bit_slices_vector[col];

    if (l.size() > bit_slices.size())
    {
        for (size_t i = 0; i < (l.size() - bit_slices.size()); i ++)
        {
            bit_slices.emplace_back(std::make_shared<RoaringBitmap>());
        }
        bit_slices_sizes[col] = l.size();
    }

    size_t bit_index = 0;

    for (auto iter = l.begin(); iter != l.end(); iter++)
    {
        auto bit_slice = bit_slices.at(bit_index);
        bit_slice->add(static_cast<UInt64>(*iter));
    }

}

MergeTreeIndexGranulePtr MergeTreeIndexBitSliced::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleBSI>();
}

MergeTreeIndexConditionPtr MergeTreeIndexBitSliced::createIndexCondition(const SelectQueryInfo & query_info, ContextPtr) const
{

    return std::make_shared<MergeTreeIndexBitSlicedCondition>(query_info);

}


bool MergeTreeIndexBitSlicedCondition::alwaysUnknownOrTrue() const
{
    return false;
}


bool MergeTreeIndexBitSlicedCondition::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::shared_ptr<MergeTreeIndexGranuleBSI> granule
        = std::dynamic_pointer_cast<MergeTreeIndexGranuleBSI>(idx_granule);

    if (!granule)
        throw Exception(
            "Bit sliced index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);
    return true;
}
}
