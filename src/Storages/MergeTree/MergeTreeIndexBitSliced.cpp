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
        //FIXME: this bit_slices_vector is empty
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
    size_t row_number = *pos;

    for (size_t col = 0; col < index_sample_block.columns(); ++col)
    {
        auto index_column_name = index_sample_block.getByPosition(col).name;
        const auto & column = block.getByName(index_column_name).column;

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
    std::list<uint> binary_value; 

    while(value!=0)
    {
        binary_value.push_front(value % 2);
        value = value >> 1;
    }

    if (bit_slices_sizes.empty() || bit_slices_vector.empty())
    {
        bit_slices_vector.resize(index_sample_block.columns());
        bit_slices_sizes.resize(index_sample_block.columns());
    }

    BitSlices & bit_slices = bit_slices_vector[col];
    size_t & bit_slices_size = bit_slices_sizes[col];

    //input Bnn as the first bit slice;
    if (bit_slices.size() == 0)
        bit_slices.emplace_back(std::make_shared<RoaringBitmap>());

    //Add more bit slice to represent bigger value
    if (bit_slices.size()-1 < binary_value.size())
    {
        size_t rb_to_increase = binary_value.size() - bit_slices.size();
        for (size_t i = 0; i < rb_to_increase; i ++)
        {
            bit_slices.emplace_back(std::make_shared<RoaringBitmap>());
        }
        bit_slices_size = binary_value.size();
    }

    size_t bit_index = binary_value.size();

    for (auto iter = binary_value.begin(); iter != binary_value.end(); iter++)
    {
        auto bit_slice = bit_slices.at(bit_index);
        if (*iter == 1)
            bit_slice->add(static_cast<UInt64>(row_number));
        
        bit_index--;
    }
    // update Bnn
    bit_slices.at(0)->add(static_cast<UInt64>(row_number));

}

MergeTreeIndexGranulePtr MergeTreeIndexBitSliced::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleBSI>();
}

MergeTreeIndexConditionPtr MergeTreeIndexBitSliced::createIndexCondition(const SelectQueryInfo & query_info, ContextPtr context) const
{

    return std::make_shared<MergeTreeIndexBitSlicedCondition>(query_info, context, index.sample_block);

}

MergeTreeIndexBitSlicedCondition::MergeTreeIndexBitSlicedCondition(const SelectQueryInfo & info_, ContextPtr context_, const Block & header_)
    : header(header_), query_info(info_)
{
    rpn = std::move(
            RPNBuilder<RPNElement>(
                    info_,
                    context_,
                    [this] (const ASTPtr & node, ContextPtr /* context */, Block & block_with_constants, RPNElement & out) -> bool
                    {
                        return this->atomFromAST(node, block_with_constants, out);
                    }).extractRPN());
}

// bool MergeTreeIndexBitSlicedCondition::alwaysUnknownOrTrue() const
// {
//     return false;
// }


bool MergeTreeIndexBitSlicedCondition::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::shared_ptr<MergeTreeIndexGranuleBSI> granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleBSI>(idx_granule);

    if (!granule)
        throw Exception("Bit sliced index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);
    std::vector<BoolMask> rpn_stack;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }

        auto bit_slices = granule->getBitSlicesByCol(element.key_column);

        size_t slice_amount = bit_slices.size()-1;
    
        RoaringBitmapPtr b_nn = bit_slices.at(0); 
        
        RoaringBitmap b_eq; 
        RoaringBitmap b_lt; 
        RoaringBitmap b_gt; 

        for (int slice_index = slice_amount; slice_index > 0; slice_index--)
        {
            RoaringBitmapPtr b_i = bit_slices.at(slice_index);
            
        }
        // else

        //     throw Exception("Unexpected function type in BloomFilterCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    if (rpn_stack.size() != 1)
        throw Exception("Unexpected stack size in BloomFilterCondition::mayBeTrueOnGranule", ErrorCodes::LOGICAL_ERROR);

    return rpn_stack[0].can_be_true;
}
}
