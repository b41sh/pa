use std::io::{Cursor, Seek, SeekFrom};
use std::marker::PhantomData;

use crate::read::{read_basic::*, BufReader, PageInfo, PageIterator};
use arrow::array::Utf8Array;
use arrow::bitmap::MutableBitmap;
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::error::Result;
use arrow::io::parquet::read::{InitNested, NestedState};
use arrow::offset::OffsetsBuffer;
use arrow::types::Offset;
use parquet2::metadata::ColumnDescriptor;

#[derive(Debug)]
pub struct Utf8Iter<I, O>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    iter: I,
    is_nullable: bool,
    data_type: DataType,
    scratch: Vec<u8>,
    _phantom: PhantomData<O>,
}

impl<I, O> Utf8Iter<I, O>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
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

impl<I, O> Iterator for Utf8Iter<I, O>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    type Item = Result<Utf8Array<O>>;

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

        let mut off_offset = 0;
        let mut buf_offset = 0;
        let mut validity_builder = if self.is_nullable {
            MutableBitmap::with_capacity(num_values)
        } else {
            MutableBitmap::new()
        };
        let out_off_len = num_values + 2;
        // don't know how much space is needed for the buffer,
        // if not enough, it may need to be allocated several times
        let out_buf_len = buffer.len() * 2;
        let mut out_offsets: Vec<O> = Vec::with_capacity(out_off_len);
        let mut out_buffer: Vec<u8> = Vec::with_capacity(out_buf_len);

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
            let start_offset = out_offsets.last().copied();
            if let Some(err) = read_buffer(
                &mut reader,
                off_offset,
                length + 1,
                &mut self.scratch,
                &mut out_offsets,
            )
            .err()
            {
                return Some(Result::Err(err));
            }

            let last_offset = out_offsets.last().unwrap().to_usize();
            if let Some(start_offset) = start_offset {
                for i in out_offsets.len() - length - 1..out_offsets.len() - 1 {
                    unsafe {
                        let next_val = *out_offsets.get_unchecked(i + 1);
                        let val = out_offsets.get_unchecked_mut(i);
                        *val = start_offset + next_val;
                    }
                }
                unsafe { out_offsets.set_len(out_offsets.len() - 1) };
                off_offset += length;
            } else {
                off_offset += length + 1;
            }
            if let Some(err) = read_buffer(
                &mut reader,
                buf_offset,
                last_offset,
                &mut self.scratch,
                &mut out_buffer,
            )
            .err()
            {
                return Some(Result::Err(err));
            }
            buf_offset += last_offset;
        }
        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        let validity = if self.is_nullable {
            Some(std::mem::take(&mut validity_builder).into())
        } else {
            None
        };
        let offsets: Buffer<O> = std::mem::take(&mut out_offsets).into();
        let values: Buffer<u8> = std::mem::take(&mut out_buffer).into();

        Some(Utf8Array::<O>::try_new(
            self.data_type.clone(),
            unsafe { OffsetsBuffer::new_unchecked(offsets) },
            values,
            validity,
        ))
    }
}

#[derive(Debug)]
pub struct Utf8NestedIter<I, O>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    iter: I,
    data_type: DataType,
    leaf: ColumnDescriptor,
    init: Vec<InitNested>,
    scratch: Vec<u8>,
    _phantom: PhantomData<O>,
}

impl<I, O> Utf8NestedIter<I, O>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
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

impl<I, O> Iterator for Utf8NestedIter<I, O>
where
    I: Iterator<Item = Result<(Vec<PageInfo>, Vec<u8>)>> + PageIterator + Send + Sync,
    O: Offset,
{
    type Item = Result<(NestedState, Utf8Array<O>)>;

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

        let mut out_offsets: Vec<O> = Vec::with_capacity(length + 1);
        if let Some(err) =
            read_buffer(&mut reader, 0, length, &mut self.scratch, &mut out_offsets).err()
        {
            return Some(Result::Err(err));
        }

        let last_offset = out_offsets.last().unwrap().to_usize();
        let mut out_buffer: Vec<u8> = Vec::with_capacity(last_offset);
        if let Some(err) = read_buffer(
            &mut reader,
            0,
            last_offset,
            &mut self.scratch,
            &mut out_buffer,
        )
        .err()
        {
            return Some(Result::Err(err));
        }

        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        let offsets: Buffer<O> = std::mem::take(&mut out_offsets).into();
        let values: Buffer<u8> = std::mem::take(&mut out_buffer).into();

        let array = match Utf8Array::<O>::try_new(
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
