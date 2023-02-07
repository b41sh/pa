use std::fs::File;
use std::io::{BufReader, Read, Seek};
use std::time::Instant;

use arrow::chunk::Chunk;
use arrow::error::Result;
use arrow::io::parquet::read::{n_columns, ColumnDescriptor};
use arrow::io::parquet::write::to_parquet_schema;

use strawboat::read::deserialize;
use strawboat::read::reader::{infer_schema, is_primitive, read_meta, NativeReader, NnativeReader};
use strawboat::ColumnMeta;

/// Simplest way: read all record batches from the file. This can be used e.g. for random access.
// cargo run --example strawboat_file_read  --release /tmp/input.st
fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let t = Instant::now();
    {
        let mut reader = File::open(file_path).unwrap();
        // we can read its metadata:
        // and infer a [`Schema`] from the `metadata`.
        let schema = infer_schema(&mut reader).unwrap();

        let mut metas: Vec<ColumnMeta> = read_meta(&mut reader)?;
        let schema_descriptor = to_parquet_schema(&schema)?;
        let mut leaves = schema_descriptor.columns().to_vec();

        let mut readers = vec![];
        for field in schema.fields.iter() {
            let n = n_columns(&field.data_type);

            let curr_metas: Vec<ColumnMeta> = metas.drain(..n).collect();
            let mut curr_leaves: Vec<ColumnDescriptor> = leaves.drain(..n).collect();

            let mut native_readers = Vec::with_capacity(n);
            for curr_meta in curr_metas.iter() {
                let mut reader = File::open(file_path).unwrap();
                reader
                    .seek(std::io::SeekFrom::Start(curr_meta.offset))
                    .unwrap();
                let reader = reader.take(curr_meta.total_len());
                let buffer_size = curr_meta.total_len().min(8192) as usize;
                let reader = BufReader::with_capacity(buffer_size, reader);

                let native_reader = NnativeReader::new(reader, curr_meta.pages.clone(), vec![]);
                native_readers.push(native_reader);
            }
            let is_nested = !is_primitive(field.data_type());

            let array_iter = deserialize::column_iter_to_arrays(
                native_readers,
                &mut curr_leaves,
                field.clone(),
                is_nested,
            )?;
            readers.push(array_iter);
        }

        'FOR: loop {
            let mut arrays = Vec::new();
            //for reader in readers.iter_mut() {
            for array_iter in readers.iter_mut() {
                //if !reader.has_next() {
                //    break 'FOR;
                //}
                //arrays.push(reader.next_array().unwrap());
                match array_iter.next() {
                    Some(array) => {
                        arrays.push(array.unwrap().to_boxed());
                    }
                    None => {
                        break 'FOR;
                    }
                }
            }

            let chunk = Chunk::new(arrays);
            println!("READ -> {:?} rows", chunk.len());
        }
    }
    println!("cost {:?} ms", t.elapsed().as_millis());
    // println!("{}", print::write(&[chunks], &["names", "tt", "3", "44"]));
    Ok(())
}
