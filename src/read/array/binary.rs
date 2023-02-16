use std::io::Cursor;
use std::marker::PhantomData;

use crate::read::{read_basic::*, BufReader, PageIterator};
use arrow::array::BinaryArray;
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::error::Result;
use arrow::io::parquet::read::{InitNested, NestedState};
use arrow::offset::OffsetsBuffer;
use arrow::types::Offset;
use parquet2::metadata::ColumnDescriptor;

#[derive(Debug)]
pub struct BinaryIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    iter: I,
    is_nullable: bool,
    data_type: DataType,
    scratch: Vec<u8>,
    _phantom: PhantomData<O>,
}

impl<I, O> BinaryIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    pub fn new(iter: I, is_nullable: bool, data_type: DataType) -> Self {
        Self {
            iter,
            is_nullable,
            data_type,
            scratch: vec![],
            _phantom: PhantomData,
        }
    }
}

impl<I, O> Iterator for BinaryIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    type Item = Result<BinaryArray<O>>;

    //fn advance_by(&mut self, n: usize) -> std::io::Result<()> {
    //fn advance_by(&mut self, n: usize) -> std::io::Result<(), usize> {
    fn advance_by(&mut self, n: usize) -> std::result::Result<(), usize> {
        println!("------this is advance_by");
        //self.iter.advance_by(n)
        todo!()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        println!("----------nth");

        todo!()
    }

    fn next(&mut self) -> Option<Self::Item> {
        let (num_values, buffer) = match self.iter.next() {
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

        let offsets: Buffer<O> = match read_buffer(&mut reader, 1 + length, &mut self.scratch) {
            Ok(offsets) => offsets,
            Err(err) => {
                return Some(Result::Err(err));
            }
        };
        let last_offset = offsets.last().unwrap().to_usize();
        let values = match read_buffer(&mut reader, last_offset, &mut self.scratch) {
            Ok(values) => values,
            Err(err) => {
                return Some(Result::Err(err));
            }
        };
        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        Some(BinaryArray::<O>::try_new(
            self.data_type.clone(),
            unsafe { OffsetsBuffer::new_unchecked(offsets) },
            values,
            validity,
        ))
    }
}

#[derive(Debug)]
pub struct BinaryNestedIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    iter: I,
    data_type: DataType,
    leaf: ColumnDescriptor,
    init: Vec<InitNested>,
    scratch: Vec<u8>,
    _phantom: PhantomData<O>,
}

impl<I, O> BinaryNestedIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
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
            _phantom: PhantomData,
        }
    }
}

impl<I, O> Iterator for BinaryNestedIter<I, O>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    type Item = Result<(NestedState, BinaryArray<O>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let (num_values, buffer) = match self.iter.next() {
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
        let offsets: Buffer<O> = match read_buffer(&mut reader, 1 + length, &mut self.scratch) {
            Ok(offsets) => offsets,
            Err(err) => {
                return Some(Result::Err(err));
            }
        };
        let last_offset = offsets.last().unwrap().to_usize();
        let values = match read_buffer(&mut reader, last_offset, &mut self.scratch) {
            Ok(values) => values,
            Err(err) => {
                return Some(Result::Err(err));
            }
        };
        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        let array = match BinaryArray::<O>::try_new(
            self.data_type.clone(),
            unsafe { OffsetsBuffer::new_unchecked(offsets) },
            values,
            validity,
        ) {
            Ok(array) => array,
            Err(err) => {
                return Some(Result::Err(err));
            }
        };

        Some(Ok((nested, array)))
    }
}
