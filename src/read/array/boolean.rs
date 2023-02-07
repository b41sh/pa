use crate::read::NativeReadBuf;
use arrow::array::BooleanArray;
use arrow::datatypes::DataType;
use arrow::error::Result;
use arrow::io::parquet::read::{InitNested, NestedState};
use parquet2::metadata::ColumnDescriptor;

use super::super::read_basic::*;

use crate::read::BufReader;
use std::io::Cursor;

pub fn read_boolean<R: NativeReadBuf>(
    reader: &mut R,
    is_nullable: bool,
    data_type: DataType,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<BooleanArray> {
    let validity = if is_nullable {
        read_validity(reader, length)?
    } else {
        None
    };
    let values = read_bitmap(reader, length, scratch)?;
    BooleanArray::try_new(data_type, values, validity)
}

pub fn read_boolean_nested<R: NativeReadBuf>(
    reader: &mut R,
    data_type: DataType,
    leaf: &ColumnDescriptor,
    init: Vec<InitNested>,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<(NestedState, BooleanArray)> {
    let (mut nested, validity) = read_validity_nested(reader, length, leaf, init)?;
    nested.nested.pop();

    let values = read_bitmap(reader, length, scratch)?;
    let array = BooleanArray::try_new(data_type, values, validity)?;

    Ok((nested, array))
}

#[derive(Debug)]
pub struct BooleanIter<I: Iterator<Item = Result<(u64, Vec<u8>)>> + Send + Sync> {
    iter: I,
    is_nullable: bool,
    data_type: DataType,
}

impl<I: Iterator<Item = Result<(u64, Vec<u8>)>> + Send + Sync> BooleanIter<I> {
    pub fn new(iter: I, is_nullable: bool, data_type: DataType) -> Self {
        Self {
            iter,
            is_nullable,
            data_type,
        }
    }
}

impl<I: Iterator<Item = Result<(u64, Vec<u8>)>> + Send + Sync> Iterator for BooleanIter<I> {
    type Item = Result<BooleanArray>;

    fn next(&mut self) -> Option<Self::Item> {
        let page_data = match self.iter.next() {
            Some(page_data) => page_data,
            None => {
                return None;
            }
        };
        let (num_values, buf) = page_data.unwrap();
        println!("buf={:?}", buf);
        let buffer_size = buf.len();
        let reader = Cursor::new(buf);
        let mut reader = BufReader::with_capacity(buffer_size, reader);

        let length = num_values as usize;
        let mut scratch = vec![];
        let validity = if self.is_nullable {
            read_validity(&mut reader, length).unwrap()
        } else {
            None
        };
        let values = read_bitmap(&mut reader, length, &mut scratch).unwrap();
        println!("validity={:?}", validity);
        println!("values={:?}", values);

        Some(BooleanArray::try_new(
            self.data_type.clone(),
            values,
            validity,
        ))
    }
}
