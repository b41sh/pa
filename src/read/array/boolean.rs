use std::io::Cursor;

use crate::read::{read_basic::*, BufReader, PageIterator, SkipIterator};
use arrow::array::{Array, BooleanArray};
use arrow::datatypes::DataType;
use arrow::error::Result;
use arrow::io::parquet::read::{InitNested, NestedState};
use parquet2::metadata::ColumnDescriptor;

#[derive(Debug)]
pub struct BooleanIter<I>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
{
    iter: I,
    is_nullable: bool,
    data_type: DataType,
    scratch: Vec<u8>,
}

impl<I> BooleanIter<I>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
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
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
{
    //type Item = Result<BooleanArray>;
    type Item = Result<Box<dyn Array>>;

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        println!("----------nth 111111111");
        let (num_values, buffer) = match self.iter.nth(n) {
            Some(Ok((num_values, buffer))) => (num_values, buffer),
            Some(Err(err)) => {
                return Some(Result::Err(err));
            }
            None => {
                return None;
            }
        };

        let length = num_values as usize;
        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        let validity = if self.is_nullable {
            match read_validity(&mut reader, length) {
                Ok(validity) => validity,
                Err(err) => {
                    return Some(Result::Err(err));
                }
            }
        } else {
            None
        };
        let values = match read_bitmap(&mut reader, length, &mut self.scratch) {
            Ok(values) => values,
            Err(err) => {
                return Some(Result::Err(err));
            }
        };
        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        let array = match BooleanArray::try_new(
            self.data_type.clone(),
            values,
            validity,
        ) {
            Ok(array) => array,
            Err(err) => {
                return Some(Result::Err(err));
            }
        };

        Some(Ok(Box::new(array) as Box<dyn Array>))
    }

    fn next(&mut self) -> Option<Self::Item> {
        println!("this is next()");
        self.nth(0)
    }
}

#[derive(Debug)]
pub struct BooleanNestedIter<I>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
{
    iter: I,
    data_type: DataType,
    leaf: ColumnDescriptor,
    init: Vec<InitNested>,
    scratch: Vec<u8>,
}

impl<I> BooleanNestedIter<I>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
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
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
{
    type Item = Result<(NestedState, Box<dyn Array>)>;

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        println!("----------nth 111111111");
        let (num_values, buffer) = match self.iter.nth(n) {
            Some(Ok((num_values, buffer))) => (num_values, buffer),
            Some(Err(err)) => {
                return Some(Result::Err(err));
            }
            None => {
                return None;
            }
        };

        let length = num_values as usize;
        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        let (nested, validity) =
            match read_validity_nested(&mut reader, length, &self.leaf, self.init.clone()) {
                Ok((nested, validity)) => (nested, validity),
                Err(err) => {
                    return Some(Result::Err(err));
                }
            };
        let values = match read_bitmap(&mut reader, length, &mut self.scratch) {
            Ok(values) => values,
            Err(err) => {
                return Some(Result::Err(err));
            }
        };
        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        let array = match BooleanArray::try_new(self.data_type.clone(), values, validity) {
            Ok(array) => array,
            Err(err) => {
                return Some(Result::Err(err));
            }
        };
        Some(Ok((nested, Box::new(array) as Box<dyn Array>)))
    }

    fn next(&mut self) -> Option<Self::Item> {
        println!("this is next()");
        self.nth(0)
    }
}
