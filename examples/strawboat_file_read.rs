use std::fs::File;
use std::io::{BufReader, Read, Seek};
use std::time::Instant;

use arrow::chunk::Chunk;
use arrow::io::parquet::write::to_parquet_schema;
use arrow::io::parquet::read::n_columns;

use arrow::error::Result;

use strawboat::read::reader::{infer_schema, read_meta, NativeReader};
use strawboat::ColumnMeta;

/// Simplest way: read all record batches from the file. This can be used e.g. for random access.
// cargo run --example strawboat_file_read  --release /tmp/input.st
fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = "/tmp/input.str";

    let t = Instant::now();
    {
        let mut reader = File::open(file_path).unwrap();
        // we can read its metadata:
        // and infer a [`Schema`] from the `metadata`.
        let schema = infer_schema(&mut reader).unwrap();
        println!("schema={:?}", schema);

        let mut metas: Vec<ColumnMeta> = read_meta(&mut reader)?;
        //println!("metas={:?}", metas);
        println!("metas.len()={:?}", metas.len());

        let schema_descriptor = to_parquet_schema(&schema)?;

        println!("schema_descriptor.fields().len()={:?}", schema_descriptor.fields().len());
        println!("schema_descriptor.columns().len()={:?}", schema_descriptor.columns().len());

/**
pub struct SchemaDescriptor {
    name: String,
    // The top-level schema (the "message" type).
    fields: Vec<ParquetType>,

    // All the descriptors for primitive columns in this schema, constructed from
    // `schema` in DFS order.
    leaves: Vec<ColumnDescriptor>,
}

    /// The [`ColumnDescriptor`] (leafs) of this schema.
    ///
    /// Note that, for nested fields, this may contain more entries than the number of fields
    /// in the file - e.g. a struct field may have two columns.
    pub fn columns(&self) -> &[ColumnDescriptor] {
        &self.leaves
    }
    /// The schemas' fields.
    pub fn fields(&self) -> &[ParquetType] {
        &self.fields
    }

*/
        let mut leaves = schema_descriptor.columns().to_vec().clone();


        let mut readers = vec![];
        for (field, parquet_type) in schema.fields.iter().zip(schema_descriptor.fields().iter()) {
            //let mut reader = File::open(file_path).unwrap();
            //reader.seek(std::io::SeekFrom::Start(meta.offset)).unwrap();
            // todo total meta.len();
            //let reader = reader.take(meta.total_len());

            //let buffer_size = meta.total_len().min(8192) as usize;
            //let buffer_size = 8192;
            //let reader = BufReader::with_capacity(buffer_size, reader);
            //let scratch = Vec::with_capacity(8 * 1024);

            // let n = 1;
            let n = n_columns(&field.data_type);

            let curr_metas: Vec<ColumnMeta> = metas.drain(..n).collect();
            let curr_leaves = leaves.drain(..n).collect();
            //println!("curr_metas={:?}", curr_metas);
            //println!("curr_leaves={:?}", curr_leaves);
            println!("metas.len()={:?}", metas.len());
            println!("leaves.len()={:?}", leaves.len());

            let mut page_readers = Vec::with_capacity(n);
            let mut scratchs = Vec::with_capacity(n);
            for curr_meta in curr_metas.iter() {
                let mut reader = File::open(file_path).unwrap();
                reader.seek(std::io::SeekFrom::Start(curr_meta.offset)).unwrap();
                let reader = reader.take(curr_meta.total_len());
                //let buffer_size = meta.total_len().min(8192) as usize;
                let buffer_size = 8192;
                let reader = BufReader::with_capacity(buffer_size, reader);
                page_readers.push(reader);

                let scratch = Vec::with_capacity(8 * 1024);
                scratchs.push(scratch);
            }

            let pa_reader = NativeReader::new(
                page_readers,
                field.clone(),
                curr_leaves,
                curr_metas,
                scratchs,
            );
            readers.push(pa_reader);
        }

        'FOR: loop {
            let mut arrays = Vec::new();
            for reader in readers.iter_mut() {
                if !reader.has_next() {
                    break 'FOR;
                }
                arrays.push(reader.next_array().unwrap());
            }

            //let chunk = Chunk::new(arrays);
            //println!("READ -> {:?} rows", chunk.len());
            println!("READ -> {:?} rows", arrays.len());
        }
    }
/**
        let mut readers = vec![];
        for (meta, field) in metas.iter().zip(schema.fields.iter()) {
            let mut reader = File::open(file_path).unwrap();
            reader.seek(std::io::SeekFrom::Start(meta.offset)).unwrap();
            let reader = reader.take(meta.total_len());

            let buffer_size = meta.total_len().min(8192) as usize;
            let reader = BufReader::with_capacity(buffer_size, reader);
            let scratch = Vec::with_capacity(8 * 1024);

            let pa_reader = NativeReader::new(
                reader,
                field.data_type().clone(),
                meta.pages.clone(),
                scratch,
            );

            readers.push(pa_reader);
        }

        'FOR: loop {
            let mut arrays = Vec::new();
            for reader in readers.iter_mut() {
                if !reader.has_next() {
                    break 'FOR;
                }
                arrays.push(reader.next_array().unwrap());
            }

            let chunk = Chunk::new(arrays);
            println!("READ -> {:?} rows", chunk.len());
        }
    }
*/
    println!("cost {:?} ms", t.elapsed().as_millis());
    // println!("{}", print::write(&[chunks], &["names", "tt", "3", "44"]));
    Ok(())
}
