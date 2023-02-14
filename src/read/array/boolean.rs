use std::io::{Cursor, Seek, SeekFrom};

use crate::read::{read_basic::*, BufReader, PageInfo, PageIterator};
use arrow::array::BooleanArray;
use arrow::bitmap::MutableBitmap;
use arrow::datatypes::DataType;
use arrow::error::Result;
use arrow::io::parquet::read::{InitNested, NestedState};
use parquet2::metadata::ColumnDescriptor;

#[derive(Debug)]
pub struct BooleanIter<I>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
{
    iter: I,
    is_nullable: bool,
    data_type: DataType,
    scratch: Vec<u8>,
}

impl<I> BooleanIter<I>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
{
    pub fn new(iter: I, is_nullable: bool, data_type: DataType) -> Self {
        Self {
            iter,
            is_nullable,
            data_type,
            scratch: vec![],
        }
    }
}

impl<I> Iterator for BooleanIter<I>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
{
    type Item = Result<BooleanArray>;

    fn next(&mut self) -> Option<Self::Item> {
        let (page_infos, buffer) = match self.iter.next() {
            Some(Ok((page_infos, buffer))) => (page_infos, buffer),
            Some(Err(err)) => {
                return Some(Result::Err(err));
            }
            None => {
                return None;
            }
        };
        let num_values = page_infos
            .iter()
            .map(|p| if p.is_skip { 0 } else { p.num_values })
            .sum();

        let mut validity_builder = if self.is_nullable {
            MutableBitmap::with_capacity(num_values)
        } else {
            MutableBitmap::new()
        };
        let mut bitmap_builder = MutableBitmap::with_capacity(num_values);

        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        for page_info in page_infos {
            if page_info.is_skip {
                if let Some(err) = reader
                    .seek(SeekFrom::Current(page_info.length as i64))
                    .err()
                {
                    return Some(Result::Err(err.into()));
                }
                continue;
            }
            let length = page_info.num_values;
            if self.is_nullable {
                if let Some(err) = read_validity(&mut reader, length, &mut validity_builder).err() {
                    return Some(Result::Err(err));
                }
            }
            if let Some(err) =
                read_bitmap(&mut reader, length, &mut self.scratch, &mut bitmap_builder).err()
            {
                return Some(Result::Err(err));
            }
        }

        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        let validity = if self.is_nullable {
            Some(std::mem::take(&mut validity_builder).into())
        } else {
            None
        };
        let values = std::mem::take(&mut bitmap_builder).into();

        Some(BooleanArray::try_new(
            self.data_type.clone(),
            values,
            validity,
        ))
    }
}

#[derive(Debug)]
pub struct BooleanNestedIter<I>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
{
    iter: I,
    data_type: DataType,
    leaf: ColumnDescriptor,
    init: Vec<InitNested>,
    scratch: Vec<u8>,
}

impl<I> BooleanNestedIter<I>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
{
    pub fn new(
        iter: I,
        data_type: DataType,
        leaf: ColumnDescriptor,
        init: Vec<InitNested>,
    ) -> Self {
        Self {
            iter,
            data_type,
            leaf,
            init,
            scratch: vec![],
        }
    }
}

impl<I> Iterator for BooleanNestedIter<I>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
{
    type Item = Result<(NestedState, BooleanArray)>;

    fn next(&mut self) -> Option<Self::Item> {
        let (page_infos, buffer) = match self.iter.next() {
            Some(Ok((page_infos, buffer))) => (page_infos, buffer),
            Some(Err(err)) => {
                return Some(Result::Err(err));
            }
            None => {
                return None;
            }
        };
        // todo read multi pages
        let length = page_infos[0].num_values;
        let mut bitmap_builder = MutableBitmap::with_capacity(length);
        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        let (nested, validity) =
            match read_validity_nested(&mut reader, length, &self.leaf, self.init.clone()) {
                Ok((nested, validity)) => (nested, validity),
                Err(err) => {
                    return Some(Result::Err(err));
                }
            };

        if let Some(err) =
            read_bitmap(&mut reader, length, &mut self.scratch, &mut bitmap_builder).err()
        {
            return Some(Result::Err(err));
        }
        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        let values = std::mem::take(&mut bitmap_builder).into();
        let array = match BooleanArray::try_new(self.data_type.clone(), values, validity) {
            Ok(array) => array,
            Err(err) => {
                return Some(Result::Err(err));
            }
        };
        Some(Ok((nested, array)))
    }
}
