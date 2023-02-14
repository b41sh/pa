use std::io::{Cursor, Seek, SeekFrom};
use std::marker::PhantomData;

use crate::read::{read_basic::*, BufReader, PageInfo, PageIterator};
use arrow::bitmap::MutableBitmap;
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::error::Result;
use arrow::io::parquet::read::{InitNested, NestedState};
use arrow::{array::PrimitiveArray, types::NativeType};
use parquet2::metadata::ColumnDescriptor;
use std::convert::TryInto;

pub struct PrimitiveIter<I, T>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
    T: NativeType,
{
    iter: I,
    is_nullable: bool,
    data_type: DataType,
    scratch: Vec<u8>,
    _phantom: PhantomData<T>,
}

impl<I, T> PrimitiveIter<I, T>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
    T: NativeType,
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

impl<I, T> Iterator for PrimitiveIter<I, T>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
    T: NativeType,
    Vec<u8>: TryInto<T::Bytes>,
{
    type Item = Result<PrimitiveArray<T>>;

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

        let mut offset = 0;
        let mut validity_builder = if self.is_nullable {
            MutableBitmap::with_capacity(num_values)
        } else {
            MutableBitmap::new()
        };
        let mut out_buffer: Vec<T> = Vec::with_capacity(num_values);

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
            if let Some(err) = read_buffer(
                &mut reader,
                offset,
                length,
                &mut self.scratch,
                &mut out_buffer,
            )
            .err()
            {
                return Some(Result::Err(err));
            }
            offset += length;
        }

        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        let validity = if self.is_nullable {
            Some(std::mem::take(&mut validity_builder).into())
        } else {
            None
        };
        let values: Buffer<T> = std::mem::take(&mut out_buffer).into();

        Some(PrimitiveArray::<T>::try_new(
            self.data_type.clone(),
            values,
            validity,
        ))
    }
}

#[derive(Debug)]
pub struct PrimitiveNestedIter<I, T>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
    T: NativeType,
{
    iter: I,
    data_type: DataType,
    leaf: ColumnDescriptor,
    init: Vec<InitNested>,
    scratch: Vec<u8>,
    _phantom: PhantomData<T>,
}

impl<I, T> PrimitiveNestedIter<I, T>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
    T: NativeType,
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

impl<I, T> Iterator for PrimitiveNestedIter<I, T>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
    T: NativeType,
    Vec<u8>: TryInto<T::Bytes>,
{
    type Item = Result<(NestedState, PrimitiveArray<T>)>;

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
        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        let (nested, validity) =
            match read_validity_nested(&mut reader, length, &self.leaf, self.init.clone()) {
                Ok((nested, validity)) => (nested, validity),
                Err(err) => {
                    return Some(Result::Err(err));
                }
            };

        let mut out_buffer: Vec<T> = Vec::with_capacity(length);
        if let Some(err) =
            read_buffer(&mut reader, 0, length, &mut self.scratch, &mut out_buffer).err()
        {
            return Some(Result::Err(err));
        }

        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        let values: Buffer<T> = std::mem::take(&mut out_buffer).into();
        let array = match PrimitiveArray::<T>::try_new(self.data_type.clone(), values, validity) {
            Ok(array) => array,
            Err(err) => {
                return Some(Result::Err(err));
            }
        };

        Some(Ok((nested, array)))
    }
}
