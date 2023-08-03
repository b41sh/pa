// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::io::{Read, Seek, SeekFrom};

use arrow::datatypes::{DataType, PhysicalType, Schema};
use arrow::error::Result;
use arrow::io::ipc::read::deserialize_schema;

use crate::{ColumnMeta, PageMeta};

use super::{
    read_basic::{read_u32, read_u32_async, read_u64},
    NativeReadBuf, PageIterator,
};

use futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

const DEFAULT_FOOTER_SIZE: u64 = 64 * 1024;

pub fn is_primitive(data_type: &DataType) -> bool {
    matches!(
        data_type.to_physical_type(),
        PhysicalType::Primitive(_)
            | PhysicalType::Null
            | PhysicalType::Boolean
            | PhysicalType::Utf8
            | PhysicalType::LargeUtf8
            | PhysicalType::Binary
            | PhysicalType::LargeBinary
            | PhysicalType::FixedSizeBinary
            | PhysicalType::Dictionary(_)
    )
}

#[derive(Debug)]
pub struct NativeReader<R: NativeReadBuf> {
    page_reader: R,
    page_metas: Vec<PageMeta>,
    current_page: usize,
    scratch: Vec<u8>,
}

impl<R: NativeReadBuf> NativeReader<R> {
    /// Creates a new [`NativeReader`]
    pub fn new(page_reader: R, page_metas: Vec<PageMeta>, scratch: Vec<u8>) -> Self {
        Self {
            page_reader,
            page_metas,
            current_page: 0,
            scratch,
        }
    }

    /// Check whether there is more data to read,
    /// returns true, if current page is not the last one, false otherwise
    pub fn has_next(&self) -> bool {
        self.current_page < self.page_metas.len()
    }

    /// Returns current page number
    pub fn current_page(&self) -> usize {
        self.current_page
    }
}

impl<R: NativeReadBuf> PageIterator for NativeReader<R> {
    fn swap_buffer(&mut self, scratch: &mut Vec<u8>) {
        std::mem::swap(&mut self.scratch, scratch)
    }
}

impl<R: NativeReadBuf + std::io::Seek> Iterator for NativeReader<R> {
    type Item = Result<(u64, Vec<u8>)>;

    /// Reads the next nth page of data, skipping the intermediate pages
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let mut i = 0;
        let mut length = 0;
        while i < n {
            if self.current_page == self.page_metas.len() {
                break;
            }
            let page_meta = &self.page_metas[self.current_page];
            length += page_meta.length;
            i += 1;
            self.current_page += 1;
        }
        if i < n {
            return None;
        }
        if length > 0 {
            if let Some(err) = self
                .page_reader
                .seek(SeekFrom::Current(length as i64))
                .err()
            {
                return Some(Result::Err(err.into()));
            }
        }
        self.next()
    }

    /// Reads the next page of data
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_page == self.page_metas.len() {
            return None;
        }
        let mut buffer = std::mem::take(&mut self.scratch);
        let page_meta = &self.page_metas[self.current_page];
        buffer.resize(page_meta.length as usize, 0);
        if let Some(err) = self.page_reader.read_exact(&mut buffer).err() {
            return Some(Result::Err(err.into()));
        }
        self.current_page += 1;
        Some(Ok((page_meta.num_values, buffer)))
    }
}

impl<R: NativeReadBuf + std::io::Seek> NativeReader<R> {
    /// Skips the next page
    pub fn skip_page(&mut self) -> Result<()> {
        if self.current_page == self.page_metas.len() {
            return Ok(());
        }
        let page_meta = &self.page_metas[self.current_page];
        self.page_reader
            .seek(SeekFrom::Current(page_meta.length as i64))?;
        self.current_page += 1;
        Ok(())
    }
}

fn deserialize_meta(buf: Vec<u8>) -> Result<Vec<ColumnMeta>> {
    let mut buf_reader = std::io::Cursor::new(buf);
    let mut buf = vec![0u8; 8];
    let meta_len = read_u64(&mut buf_reader, buf.as_mut_slice())?;
    let mut metas = Vec::with_capacity(meta_len as usize);
    for _i in 0..meta_len {
        let offset = read_u64(&mut buf_reader, buf.as_mut_slice())?;
        let page_num = read_u64(&mut buf_reader, buf.as_mut_slice())?;
        let mut pages = Vec::with_capacity(page_num as usize);
        for _p in 0..page_num {
            let length = read_u64(&mut buf_reader, buf.as_mut_slice())?;
            let num_values = read_u64(&mut buf_reader, buf.as_mut_slice())?;

            pages.push(PageMeta { length, num_values });
        }
        metas.push(ColumnMeta { offset, pages })
    }
    Ok(metas)
}

pub fn read_meta<Reader: Read + Seek>(reader: &mut Reader) -> Result<Vec<ColumnMeta>> {
    // EOS(8 bytes) + meta_size(4 bytes) = 12 bytes
    reader.seek(SeekFrom::End(-12))?;
    let mut buf = vec![0u8; 4];
    let meta_size = read_u32(reader, buf.as_mut_slice())? as usize;
    reader.seek(SeekFrom::End(-16 - meta_size as i64))?;

    let mut meta_buf = vec![0u8; meta_size];
    reader.read_exact(&mut meta_buf)?;
    deserialize_meta(meta_buf)
}

pub async fn read_meta_async<Reader: AsyncRead + AsyncSeek + Send + Unpin>(
    reader: &mut Reader,
    total_len: Option<usize>,
) -> Result<Vec<ColumnMeta>> {
    match total_len {
        Some(total_len) => {
            // Pre-read footer data to reduce IO.
            let pre_read_len = total_len.min(DEFAULT_FOOTER_SIZE as usize);

            reader.seek(SeekFrom::End(-(pre_read_len as i64))).await?;
            let mut buf = vec![0u8; pre_read_len];
            reader.read_exact(&mut buf).await?;

            let mut footer_reader = std::io::Cursor::new(buf);
            // EOS(8 bytes) + meta_size(4 bytes) = 12 bytes
            footer_reader.seek(SeekFrom::End(-12))?;
            let mut buf = vec![0u8; 4];
            let meta_size = read_u32(&mut footer_reader, buf.as_mut_slice())? as usize;

            let footer_size = meta_size + 16;
            if footer_size <= pre_read_len {
                footer_reader.seek(SeekFrom::End(-16 - meta_size as i64))?;
                let mut meta_buf = vec![0u8; meta_size];
                footer_reader.read_exact(&mut meta_buf)?;
                deserialize_meta(meta_buf)
            } else {
                // The readed data is not long enough to hold the meta data.
                // Should read again.
                reader.seek(SeekFrom::End(-(footer_size as i64))).await?;
                let mut buf = vec![0u8; footer_size];
                reader.read_exact(&mut buf).await?;
                let mut final_reader = std::io::Cursor::new(buf);
                read_meta(&mut final_reader)
            }
        }
        _ => {
            reader.seek(SeekFrom::End(-12)).await?;
            let mut buf = vec![0u8; 4];
            let meta_size = read_u32_async(reader, buf.as_mut_slice()).await? as usize;
            let mut meta_buf = vec![0u8; meta_size];
            reader.seek(SeekFrom::End(-16 - meta_size as i64)).await?;
            reader.read_exact(&mut meta_buf).await?;
            deserialize_meta(meta_buf)
        }
    }
}

pub fn infer_schema<Reader: Read + Seek>(reader: &mut Reader) -> Result<Schema> {
    // EOS(8 bytes) + meta_size(4 bytes) + schema_size(4bytes) = 16 bytes
    reader.seek(SeekFrom::End(-16))?;
    let mut buf = vec![0u8; 4];
    let schema_size = read_u32(reader, buf.as_mut_slice())? as usize;
    let column_meta_size = read_u32(reader, buf.as_mut_slice())? as usize;

    reader.seek(SeekFrom::Current(
        -(column_meta_size as i64) - (schema_size as i64) - 8,
    ))?;
    let mut schema_bytes = vec![0u8; schema_size];
    reader.read_exact(&mut schema_bytes)?;
    let (schema, _) = deserialize_schema(&schema_bytes).expect("deserialize schema error");
    Ok(schema)
}

pub async fn infer_schema_async<Reader: AsyncRead + AsyncSeek + Send + Unpin>(
    reader: &mut Reader,
) -> Result<Schema> {
    // EOS(8 bytes) + meta_size(4 bytes) + schema_size(4bytes) = 16 bytes
    reader.seek(SeekFrom::End(-16)).await?;
    let mut buf = vec![0u8; 4];
    let schema_size = read_u32_async(reader, buf.as_mut_slice()).await? as usize;
    let column_meta_size = read_u32_async(reader, buf.as_mut_slice()).await? as usize;

    reader
        .seek(SeekFrom::Current(
            -(column_meta_size as i64) - (schema_size as i64) - 8,
        ))
        .await?;
    let mut schema_bytes = vec![0u8; schema_size];
    reader.read_exact(&mut schema_bytes).await?;

    let (schema, _) = deserialize_schema(&schema_bytes).expect("deserialize schema error");
    Ok(schema)
}
