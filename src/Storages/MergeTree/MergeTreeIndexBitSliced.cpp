#include <memory>
#include <sstream>
#include <vector>
#include <Storages/MergeTree/MergeTreeIndexBitSliced.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/IDataType.h>
#include <IO/VarInt.h>
#include <Common/Exception.h>
#include "Columns/ColumnsNumber.h"
#include "Columns/IColumn.h"
#include "Storages/IndicesDescription.h"
#include "base/types.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int BAD_ARGUMENTS;
}


void MergeTreeIndexGranuleBSI::serializeBinary(DB::WriteBuffer & ostr) const
{
    std::cout<<"== test test test"<<std::endl;
    std::cout<<"== Granule Ser"<<std::endl;
    writeVarUInt(names_bitslices.size(), ostr);
    for (auto it = names_bitslices.begin(); it != names_bitslices.end(); ++it)
    {
        writeString(it->first, ostr);
        std::cout<<"==col name:"<<it->first<<std::endl;
        for (const auto & bit_slice : it->second)
        {
            auto size = bit_slice->getSizeInBytes();
            writeVarUInt(size, ostr);
            std::unique_ptr<char[]> buf(new char[size]);
            bit_slice->write(buf.get());
            std::cout<<"=="<<bit_slice->toString()<<std::endl;
            ostr.write(buf.get(), size);
        }
    }
}


void MergeTreeIndexGranuleBSI::deserializeBinary(DB::ReadBuffer & istr, DB::MergeTreeIndexVersion version)
{
    std::cout<<"== Granule Dser"<<std::endl;
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    size_t map_size;
    readVarUInt(map_size, istr);
    for (size_t i = 0; i < map_size; ++i)
    {
        String col_name;
        std::vector<RoaringBitmapPtr> bit_slices;
        readString(col_name, istr);
        size_t bit_slices_size;
        readVarUInt(bit_slices_size, istr);
        for (size_t j = 0; j < bit_slices_size; ++j)
        {
            size_t size;
            readVarUInt(size, istr);
            std::unique_ptr<char[]> buf(new char[size]);
            istr.readStrict(buf.get(), size);
            bit_slices.emplace_back(std::make_shared<RoaringBitmap>(RoaringBitmap::read(buf.get())));
        }
        names_bitslices[col_name] = bit_slices;
    }
}


bool MergeTreeIndexGranuleBSI::empty() const
{
    return names_bitslices.empty();
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
    return std::make_shared<MergeTreeIndexGranuleBSI>(index_name, std::move(names_bitslices), index_sample_block);
}

void MergeTreeIndexAggregatorBSI::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The provided position is not less than the number of block rows.  Position: {}, Block rows: {}.", toString(*pos), toString(block.rows()));


    size_t rows_read = std::min(limit, block.rows() - *pos);

    if (rows_read <= 0)
        return ;

    for (size_t col = 0; col < index_sample_block.columns(); ++col)
    {
        auto index_column_name = index_sample_block.getByPosition(col).name;
        std::cout<<"====== index column name is :"<<index_column_name<<std::endl;
        const auto & column = block.getByName(index_column_name).column;

        if (!names_bitslices.count(index_column_name))
            names_bitslices[index_column_name] = std::vector<RoaringBitmapPtr>(); 
        
        auto & bit_slices = names_bitslices[index_column_name];

        const ColumnUInt64 * column_uint = checkAndGetColumn<ColumnUInt64>(column.get());
        for (size_t r = 0 ; r < rows_read; ++r)
        {
            auto ref = column_uint->get64(*pos + r);
            //Decimal to binary
            std::cout<<"====== row num is :"<<*pos+r<<std::endl;
            std::cout<<"====== data is :"<<ref<<std::endl;
            auto value = ref;
            std::list<bool> binary_value;
          
            while (value != 0)
            {
                binary_value.push_front(value % 2);
                value = value >> 1;
            }

            size_t to_add_slices = binary_value.size() - bit_slices.size();

            if (to_add_slices > 0)
            {
                for (size_t i = 0; i < to_add_slices; ++i)
                    bit_slices.emplace_back(std::make_shared<RoaringBitmap>());
            }

            size_t bitmap_index = 0;

            for (auto it = binary_value.begin(); it != binary_value.end(); ++it)
            {
                if (*it)
                {
                    std::cout<<"===== it value: "<<*it<<std::endl;
                    bit_slices.at(bitmap_index)->add(static_cast<UInt64>(r + 1));
                }
                bitmap_index += 1;
            }

            // This is only for debugging
            std::stringstream ss;
            for (const auto & iv : binary_value)
                ss<<iv;

            std::cout<<"XF_TEST"<<"====== binary value is: "<<ss.str()<<std::endl;

        }
    }

    *pos += rows_read;
}

void MergeTreeIndexAggregatorBSI::columnToBitSlices(UInt64 value, const String & col_name, const size_t & row_number)
{
    //Decimal to binary
    std::list<bool> binary_value;
    auto & bit_slices= names_bitslices[col_name];

    while (value != 0)
    {
        binary_value.push_front(value % 2);
        value = value >> 1;
    }
    std::stringstream ss;
    for (const auto & iv : binary_value)
        ss<<iv;

    std::cout<<"XF_TEST"<<"====== binary value is: "<<ss.str();

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
    std::cout<<3333<<std::endl;
    return std::make_shared<MergeTreeIndexGranuleBSI>();
}

MergeTreeIndexConditionPtr MergeTreeIndexBitSliced::createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const
{
    std::cout<<4444<<std::endl;
    return std::make_shared<MergeTreeIndexBitSlicedCondition>(index, query, context);
}

MergeTreeIndexFormat MergeTreeIndexBitSliced::getDeserializedFormat(const IDataPartStorage & data_part_storage, const std::string & relative_path_prefix) const
{
   if (data_part_storage.exists(relative_path_prefix + ".idx2"))
       return {2, ".idx2"};
   else if (data_part_storage.exists(relative_path_prefix + ".idx"))
       return {1, ".idx"};
   return {0 /* unknown */, ""};
}

MergeTreeIndexBitSlicedCondition::MergeTreeIndexBitSlicedCondition(
    const IndexDescription & index_, const SelectQueryInfo & query_, ContextPtr context_)
    : index(index_)
    , query(query_)
    , context(context_)
{
}

bool MergeTreeIndexBitSlicedCondition::alwaysUnknownOrTrue() const
{
    std::cout<<2222<<std::endl;
    return false;
    //return condition.alwaysUnknownOrTrue();
}

bool MergeTreeIndexBitSlicedCondition::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::cout<<1111<<std::endl;
    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleBSI>(idx_granule);
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Bit-sliced index condition got a granule with the wrong type");

    if (granule->empty())
        return true;
    
    std::cout<<query.filter_actions_dag->dumpDAG()<<std::endl;

    return true;
    //return condition.checkInBitSlices(granule->columns_bit_slices, index_data_types).can_be_true;
}

MergeTreeIndexPtr bsiIndexCreator(const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexBitSliced>(index);
}

void bsiIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    for (const auto & data_type : index.data_types)
    {
        WhichDataType which(data_type);

        if (!which.isUInt())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Bit sliced index can be used only with positive integer type." );
    }
}

}
