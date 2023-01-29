use crate::{ColumnMeta, PageMeta};

use super::read_basic::read_u64;
use super::NativeReadBuf;
use super::{deserialize, read_basic::read_u32};
use arrow::datatypes::{Schema, DataType, Field, PhysicalType};
use arrow::error::Result;
use arrow::io::ipc::read::deserialize_schema;
use arrow::array::Array;
use std::io::{Read, Seek, SeekFrom};

use arrow::io::parquet::read::ColumnDescriptor;


fn is_primitive(data_type: &DataType) -> bool {
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

pub struct NativeReader<R: NativeReadBuf> {
    page_readers: Vec<R>,
    field: Field,
    leaves: Vec<ColumnDescriptor>,
    column_metas: Vec<ColumnMeta>,
    current_page: usize,
    scratchs: Vec<Vec<u8>>,
}

impl<R: NativeReadBuf> NativeReader<R> {
    pub fn new(
        page_readers: Vec<R>,
        field: Field,
        leaves: Vec<ColumnDescriptor>,
        column_metas: Vec<ColumnMeta>,
        scratchs: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            page_readers,
            field,
            leaves,
            column_metas,
            current_page: 0,
            scratchs,
        }
    }


    /// must call after has_next
    //pub fn next_array(&mut self) -> Result<()> {
    pub fn next_array(&mut self) -> Result<Box<dyn Array>> {
        let result = if is_primitive(self.field.data_type()) {
            let page_meta = &self.column_metas[0].pages[self.current_page].clone();

            //let page = &self.page_metas[self.current_page];
            deserialize::read_simple(
                &mut self.page_readers[0],
                self.field.data_type().clone(),
                page_meta.num_values as usize,
                &mut self.scratchs[0],
            )?
        } else {
            let page_metas = &self.column_metas.iter()
                    .map(|meta| meta.pages[self.current_page].clone())
                    .collect::<Vec<_>>();
            println!("page_metas={:?}", page_metas);

            let (_, array) = deserialize::read_nested(
                &mut self.page_readers,
                self.field.clone(),
                &mut self.leaves,
                page_metas.clone(),
                vec![],
                &mut self.scratchs,
            )?;
            array
        };
        self.current_page += 1;

        Ok(result)
    }

    pub fn has_next(&self) -> bool {
        self.current_page < self.column_metas[0].pages.len()
    }

    pub fn current_page(&self) -> usize {
        self.current_page
    }
}

impl<R: NativeReadBuf + std::io::Seek> NativeReader<R> {
    pub fn skip_page(&mut self) -> Result<()> {
        for (i, column_meta) in self.column_metas.iter().enumerate() {
            let page_meta = &column_meta.pages[self.current_page];
            self.page_readers[i].seek(SeekFrom::Current(
                page_meta.length as i64,
            ))?;
        }
        self.current_page += 1;
        Ok(())
    }
}

pub fn read_meta<Reader: Read + Seek>(reader: &mut Reader) -> Result<Vec<ColumnMeta>> {
    // ARROW_MAGIC(6 bytes) + EOS(8 bytes) + meta_size(4 bytes) = 18 bytes
    //reader.seek(SeekFrom::End(-18))?;
    reader.seek(SeekFrom::End(-12))?;
    let meta_size = read_u32(reader)? as usize;
    //reader.seek(SeekFrom::End(-22 - meta_size as i64))?;
    reader.seek(SeekFrom::End(-16 - meta_size as i64))?;

    let mut buf = vec![0u8; meta_size];
    reader.read_exact(&mut buf)?;

    let mut buf_reader = std::io::Cursor::new(buf);
    let meta_len = read_u64(&mut buf_reader)?;
    let mut metas = Vec::with_capacity(meta_len as usize);
    for _i in 0..meta_len {
        let offset = read_u64(&mut buf_reader)?;
        let page_num = read_u64(&mut buf_reader)?;
        let mut pages = Vec::with_capacity(page_num as usize);
        for _p in 0..page_num {
            let length = read_u64(&mut buf_reader)?;
            let num_values = read_u64(&mut buf_reader)?;

            pages.push(PageMeta {
                length,
                num_values,
            });
        }
        metas.push(ColumnMeta { offset, pages })
    }
    Ok(metas)
}

pub fn infer_schema<Reader: Read + Seek>(reader: &mut Reader) -> Result<Schema> {
    // ARROW_MAGIC(6 bytes) + EOS(8 bytes) + meta_size(4 bytes) + schema_size(4bytes) = 22 bytes
    //reader.seek(SeekFrom::End(-22))?;
    reader.seek(SeekFrom::End(-16))?;
    let schema_size = read_u32(reader)? as usize;
    let column_meta_size = read_u32(reader)? as usize;
    println!("schema_size={:?}", schema_size);
    println!("column_meta_size={:?}", column_meta_size);


    reader.seek(SeekFrom::Current(
        (-(column_meta_size as i64) - (schema_size as i64) - 8) as i64,
    ))?;
    let mut schema_bytes = vec![0u8; schema_size];
    reader.read_exact(&mut schema_bytes)?;
    let (schema, _) = deserialize_schema(&schema_bytes).expect("deserialize schema error");
    Ok(schema)
}
