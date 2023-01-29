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
use arrow::io::parquet::write::ParquetType;


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

pub struct NnativeReader<R: NativeReadBuf> {
    page_readers: Vec<R>,
    field: Field,
    parquet_type: ParquetType,
    leaves: Vec<ColumnDescriptor>,
    column_metas: Vec<ColumnMeta>,
    current_page: usize,
    scratchs: Vec<Vec<u8>>,
}

impl<R: NativeReadBuf> NnativeReader<R> {
    pub fn new(
        page_readers: Vec<R>,
        field: Field,
        parquet_type: ParquetType,
        leaves: Vec<ColumnDescriptor>,
        column_metas: Vec<ColumnMeta>,
        scratchs: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            page_readers,
            field,
            parquet_type,
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


        //let page_metas = &self.column_metas.iter()
        //            .map(|meta| meta.pages[self.current_page].clone())
        //            .collect::<Vec<_>>();
        //println!("page_metas={:?}", page_metas);


        self.current_page += 1;

/**
        let page = &self.page_metas[self.current_page];
        let result = deserialize::read(
            &mut self.reader,
            self.data_type.clone(),
            page.num_values as usize,
            &mut self.scratch,
        )?;
*/
        Ok(result)
    }

/**
    fn read_columns(
        reader: &mut R,
        page_metas: Vec<PageMeta>,
    ) -> Result<Vec<Vec<u8>>> {

    }

pub struct ColumnMeta {
    pub offset: u64,
    pub pages: Vec<PageMeta>,
}

pub struct PageMeta {
    // compressed size of this page
    pub length: u64,
    // num values(rows) of this page
    pub num_values: u64,
}



pub fn read<R: NativeReadBuf>(
    reader: &mut R,
    data_type: DataType,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<Box<dyn Array>> {
    use PhysicalType::*;



/// Reads all columns that are part of the parquet field `field_name`
/// # Implementation
/// This operation is IO-bounded `O(C)` where C is the number of columns associated to
/// the field (one for non-nested types)
pub fn read_columns<'a, R: Read + Seek>(
    reader: &mut R,
    columns: &'a [ColumnChunkMetaData],
    field_name: &str,
) -> Result<Vec<(&'a ColumnChunkMetaData, Vec<u8>)>> {
    get_field_columns(columns, field_name)
        .into_iter()
        .map(|meta| _read_single_column(reader, meta))
        .collect()
}
*/



    pub fn has_next(&self) -> bool {
        self.current_page < self.column_metas[0].pages.len()
    }

    pub fn current_page(&self) -> usize {
        self.current_page
    }
}

/**
    pub fn has_next(&self) -> bool {
        self.current_column < self.fields.len()
    }

    // must call after has_next
    pub fn next_array(&mut self) -> Result<Box<dyn Array>> {
        let parquet_type = &self.parquet_types[self.current_page];



        self.current_column += 1;

        let page = &self.page_metas[self.current_page];
        let result = deserialize::read(
            &mut self.reader,
            self.data_type.clone(),
            page.num_values as usize,
            &mut self.scratch,
        )?;
        self.current_page += 1;
        Ok(result)
    }
*/

pub struct NativeReader<R: NativeReadBuf> {
    reader: R,
    data_type: DataType,
    current_page: usize,
    page_metas: Vec<PageMeta>,
    scratch: Vec<u8>,
}

impl<R: NativeReadBuf> NativeReader<R> {
    pub fn new(
        reader: R,
        data_type: DataType,
        page_metas: Vec<PageMeta>,
        scratch: Vec<u8>,
    ) -> Self {
        Self {
            reader,
            data_type,
            current_page: 0,
            page_metas,
            scratch,
        }
    }

    /// must call after has_next
    pub fn next_array(&mut self) -> Result<Box<dyn Array>> {
        let page = &self.page_metas[self.current_page];
        let result = deserialize::read_simple(
            &mut self.reader,
            self.data_type.clone(),
            page.num_values as usize,
            &mut self.scratch,
        )?;
        self.current_page += 1;
        Ok(result)
    }

    pub fn has_next(&self) -> bool {
        self.current_page < self.page_metas.len()
    }

    pub fn current_page(&self) -> usize {
        self.current_page
    }
}

impl<R: NativeReadBuf + std::io::Seek> NativeReader<R> {
    pub fn skip_page(&mut self) -> Result<()> {
        self.reader.seek(SeekFrom::Current(
            self.page_metas[self.current_page].length as i64,
        ))?;
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
