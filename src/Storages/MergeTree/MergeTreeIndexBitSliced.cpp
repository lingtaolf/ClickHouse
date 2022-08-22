#include <sstream>
#include <Storages/MergeTree/MergeTreeIndexBitSliced.h>
#include <IO/WriteHelpers.h>
#include "DataTypes/IDataType.h"
#include "IO/VarInt.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int BAD_ARGUMENTS;
}


void MergeTreeIndexGranuleBSI::serializeBinary(DB::WriteBuffer & ostr) const
{

    for (size_t col = 0; col < index_sample_block.columns(); col++)
    {
        for (const auto & column_bit_slices : columns_bit_slices)
            for (const auto & bm : column_bit_slices.bit_slices)
                writeVarUInt(bm->getSizeInBytes(), ostr);
    }
    for (size_t col = 0; col < index_sample_block.columns(); col++)
    {
        //FIXME: this bit_slices_vector is empty
        for (const auto & column_bit_slice : columns_bit_slices)
        {
            for (const auto & bit_slice : column_bit_slice.bit_slices)
            {
                auto size = bit_slice->getSizeInBytes();
                std::unique_ptr<char[]> buf(new char[size]);
                bit_slice->write(buf.get());
                ostr.write(buf.get(), size);
            }
        }
    }
}


void MergeTreeIndexGranuleBSI::deserializeBinary(DB::ReadBuffer & istr, DB::MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    // for (size_t i = 0; i < index_sample_block.columns(); i++)
    // {
    //     size_t size;
    //     readVarUInt(size, istr);
    //     bit_slices_sizes.emplace_back(size);
    // }
    std::vector<size_t> bm_sizes;
    columns_bit_slices.resize(index_sample_block.columns());

    for (size_t i = 0; i < columns_bit_slices.size(); ++i)
    {
            size_t size;
            std::unique_ptr<char[]> buf(new char[size]);
            istr.readStrict(buf.get(), size);
            columns_bit_slices.emplace_back(std::make_shared<RoaringBitmap>(RoaringBitmap::read(buf.get())));
        }
    }
}


bool MergeTreeIndexGranuleBSI::empty() const
{
    return bit_slices_vector.empty();
}

MergeTreeIndexAggregatorPtr MergeTreeIndexBitSliced::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorBSI>(index.name, index.sample_block);
}

bool MergeTreeIndexAggregatorBSI::empty() const
{
    return false;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorBSI::getGranuleAndReset()
{
    return std::make_shared<MergeTreeIndexGranuleBSI>(
        index_name, std::move(columns_bit_slices), index_sample_block);
}

void MergeTreeIndexAggregatorBSI::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            "The provided position is not less than the number of block rows. Position: " + toString(*pos)
                + ", Block rows: " + toString(block.rows()) + ".",
            ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);
    size_t row_number = *pos;

    for (size_t col = 0; col < index_sample_block.columns(); ++col)
    {
        auto index_column_name = index_sample_block.getByPosition(col).name;
        const auto & column = block.getByName(index_column_name).column;

        if (columns_bit_slices.size() < col)
        {
            BitSlices bit_slices;
            columns_bit_slices.emplace_back(bit_slices);
        }

        for (size_t i = 0; i < rows_read; ++i)
        {
            UInt64 value = static_cast<UInt64>(column->getUInt(*pos + i));
            columnToBitSlices(value, col, row_number);
            row_number++;
        }
    }

    *pos += rows_read;
}

void MergeTreeIndexAggregatorBSI::columnToBitSlices(UInt64 value, const size_t & col, const size_t & row_number)
{
    //Decimal to binary
    std::list<bool> binary_value;
    auto & column_bit_slices = columns_bit_slices.at(col);

    while (value != 0)
    {
        binary_value.push_front(value % 2);
        value = value >> 1;
    }
    std::stringstream ss;
    for (const auto & iv : binary_value)
        ss<<iv;

    LOG_INFO(&Poco::Logger::get("XF_TEST"), "====== binary value is {}", ss.str());

    auto & bit_slices = column_bit_slices.bit_slices;

    //input Bnn as the first bit slice;
    //if (bit_slices.empty())
    //    bit_slices.emplace_back(std::make_shared<RoaringBitmap>());

    //Add more bit slice to represent bigger value
    if (bit_slices.size() < binary_value.size())
    {
        size_t rb_to_increase = binary_value.size() - (bit_slices.size() - 1);

        for (size_t i = 0; i < rb_to_increase; i++)
        {
            bit_slices.emplace_back(std::make_shared<RoaringBitmap>());
        }

    }

    size_t bit_index = binary_value.size();

    for (bool & iter : binary_value)
    {
        auto & bit_slice = bit_slices.at(bit_index - 1);
        if (iter == 1)
            bit_slice->add(static_cast<UInt64>(row_number));

        bit_index--;
    }
    // update Bnn
    //bit_slices.at(0)->add(static_cast<UInt64>(row_number));
}

MergeTreeIndexGranulePtr MergeTreeIndexBitSliced::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleBSI>();
}

MergeTreeIndexConditionPtr MergeTreeIndexBitSliced::createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexBitSlicedCondition>(index, query, context);
}

MergeTreeIndexFormat MergeTreeIndexBitSliced::getDeserializedFormat(const DiskPtr disk, const std::string & relative_path_prefix) const
{
    if (disk->exists(relative_path_prefix + ".idx2"))
        return {2, ".idx2"};
    else if (disk->exists(relative_path_prefix + ".idx"))
        return {1, ".idx"};
    return {0 /* unknown */, ""};
}

MergeTreeIndexBitSlicedCondition::MergeTreeIndexBitSlicedCondition(
    const IndexDescription & index, const SelectQueryInfo & query, ContextPtr context)
    : index_data_types(index.data_types), condition(query, context, index.column_names, index.expression)
{
}

bool MergeTreeIndexBitSlicedCondition::alwaysUnknownOrTrue() const
{
    return condition.alwaysUnknownOrTrue();
}

bool MergeTreeIndexBitSlicedCondition::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::shared_ptr<MergeTreeIndexGranuleBSI> granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleBSI>(idx_granule);

    if (!granule)
        throw Exception("Bit sliced index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);

    return condition.checkInBitSlices(granule->columns_bit_slices, index_data_types).can_be_true;
}

MergeTreeIndexPtr bitSlicedIndexCreator(const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexBitSliced>(index);
}

void bitSlicedIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    for (const auto & data_type : index.data_types)
    {
        WhichDataType which(data_type);

        if (which.isUInt())
            throw Exception("Bit sliced index can be used only with positive integer type.", ErrorCodes::INCORRECT_QUERY);
    }
}

}
