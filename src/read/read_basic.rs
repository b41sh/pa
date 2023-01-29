use std::convert::TryInto;
use std::io::Read;

use arrow::buffer::Buffer;
use arrow::error::Result;

use arrow::{bitmap::Bitmap, types::NativeType};

use super::super::endianess::is_native_little_endian;
use super::NativeReadBuf;
use crate::{compression, Compression};

use arrow::bitmap::MutableBitmap;
use arrow::io::parquet::read::{InitNested, NestedState, init_nested};


use parquet2::{
    encoding::hybrid_rle::{Decoder, HybridEncoded, BitmapIter, HybridRleDecoder},
    read::levels::get_bit_width,
    metadata::ColumnDescriptor,
};




fn read_swapped<T: NativeType, R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    buffer: &mut Vec<T>,
) -> Result<()> {
    // slow case where we must reverse bits
    let mut slice = vec![0u8; length * std::mem::size_of::<T>()];
    reader.read_exact(&mut slice)?;

    let chunks = slice.chunks_exact(std::mem::size_of::<T>());
    // machine is little endian, file is big endian
    buffer
        .as_mut_slice()
        .iter_mut()
        .zip(chunks)
        .try_for_each(|(slot, chunk)| {
            let a: T::Bytes = match chunk.try_into() {
                Ok(a) => a,
                Err(_) => unreachable!(),
            };
            *slot = T::from_le_bytes(a);
            Result::Ok(())
        })?;
    Ok(())
}

fn read_uncompressed_buffer<T: NativeType, R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
) -> Result<Vec<T>> {
    // it is undefined behavior to call read_exact on un-initialized, https://doc.rust-lang.org/std/io/trait.Read.html#tymethod.read
    // see also https://github.com/MaikKlein/ash/issues/354#issue-781730580
    let mut buffer = vec![T::default(); length];

    if is_native_little_endian() {
        // fast case where we can just copy the contents
        let slice = bytemuck::cast_slice_mut(&mut buffer);
        reader.read_exact(slice)?;
    } else {
        read_swapped(reader, length, &mut buffer)?;
    }
    Ok(buffer)
}

pub fn read_buffer<T: NativeType, R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<Buffer<T>> {
    let compression = Compression::from_codec(read_u8(reader)?)?;
    let compressed_size = read_u32(reader)? as usize;
    let uncompressed_size = read_u32(reader)? as usize;

    println!("length={:?}", length);
    println!("compression={:?}", compression);
    println!("compressed_size={:?}", compressed_size);
    println!("uncompressed_size={:?}", uncompressed_size);


    if compression.is_none() {
        return Ok(read_uncompressed_buffer(reader, length)?.into());
    }
    let mut buffer = vec![T::default(); length];
    let out_slice = bytemuck::cast_slice_mut(&mut buffer);

    assert_eq!(uncompressed_size, out_slice.len());

    // already fit in buffer
    let mut use_inner = false;
    let input = if reader.buffer_bytes().len() > compressed_size {
        use_inner = true;
        reader.buffer_bytes()
    } else {
        scratch.resize(compressed_size, 0);
        reader.read_exact(scratch.as_mut_slice())?;
        scratch.as_slice()
    };

    match compression {
        Compression::LZ4 => {
            compression::decompress_lz4(&input[..compressed_size], out_slice)?;
        }
        Compression::ZSTD => {
            compression::decompress_zstd(&input[..compressed_size], out_slice)?;
        }
        Compression::SNAPPY => {
            compression::decompress_snappy(&input[..compressed_size], out_slice)?;
        }
        Compression::None => unreachable!(),
    }

    if use_inner {
        reader.consume(compressed_size);
    }

    Ok(buffer.into())
}

pub fn read_bitmap<R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<Bitmap> {
    let bytes = (length + 7) / 8;
    let mut buffer = vec![0u8; bytes];

    let compression = Compression::from_codec(read_u8(reader)?)?;
    let compressed_size = read_u32(reader)? as usize;
    let uncompressed_size = read_u32(reader)? as usize;

    assert_eq!(uncompressed_size, bytes);

    if compression.is_none() {
        reader
            .by_ref()
            .take(bytes as u64)
            .read_exact(buffer.as_mut_slice())?;
        return Bitmap::try_new(buffer, length);
    }

    // already fit in buffer
    let mut use_inner = false;
    let input = if reader.buffer_bytes().len() > compressed_size as usize {
        use_inner = true;
        reader.buffer_bytes()
    } else {
        scratch.resize(compressed_size, 0);
        reader.read_exact(scratch.as_mut_slice())?;
        scratch.as_slice()
    };

    match compression {
        Compression::LZ4 => {
            compression::decompress_lz4(&input[..compressed_size], &mut buffer)?;
        }
        Compression::ZSTD => {
            compression::decompress_zstd(&input[..compressed_size], &mut buffer)?;
        }
        Compression::SNAPPY => {
            compression::decompress_snappy(&input[..compressed_size], &mut buffer)?;
        }
        Compression::None => unreachable!(),
    }

    if use_inner {
        reader.consume(compressed_size);
    }

    Bitmap::try_new(buffer, length)
}

/**
+-------------------+
|  rep levels len   |
+-------------------+
|  def levels len   |
+-------------------+
|  def/def values   |
+-------------------+
|    codec type     |
+-------------------+
|  compressed size  |
+-------------------+
| uncompressed size |
+-------------------+
|     values        |
+-------------------+

    w.write_all(&(rep_levels_len as u32).to_le_bytes())?;
    w.write_all(&(def_levels_len as u32).to_le_bytes())?;
    w.write_all(&scratch[..def_levels_len])?;
*/

#[allow(clippy::too_many_arguments)]
pub fn read_validity<R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<Option<Bitmap>> {

    let _rep_levels_len = read_u32(reader)?;
    let def_levels_len = read_u32(reader)?;
    println!("def_levels_len={:?}", def_levels_len);

    if def_levels_len == 0 {
        return Ok(None);
    }
    scratch.resize(def_levels_len as usize, 0);
    reader.read_exact(scratch.as_mut_slice())?;
    let buf = scratch.as_slice();
    println!("buf={:?}", buf);

    let mut builder = MutableBitmap::with_capacity(length);
    let mut iter = Decoder::new(scratch.as_slice(), 1);
    while let Some(encoded) = iter.next() {
        let encoded = encoded.unwrap();
        match encoded {
            HybridEncoded::Bitpacked(r) => {
                let mut bitmap_iter = BitmapIter::new(r, 0, length);
                while let Some(v) = bitmap_iter.next() {
                    unsafe { builder.push_unchecked(v) };
                }
            }
            HybridEncoded::Rle(_, _) => unreachable!(),
        }
    }
    Ok(Some(std::mem::take(&mut builder).into()))
}







/**
impl<'a> NestedPage<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self> {
        let (rep_levels, def_levels, _) = split_buffer(page)?;

        let max_rep_level = page.descriptor.max_rep_level;
        let max_def_level = page.descriptor.max_def_level;

        let reps =
            HybridRleDecoder::try_new(rep_levels, get_bit_width(max_rep_level), page.num_values())?;
        let defs =
            HybridRleDecoder::try_new(def_levels, get_bit_width(max_def_level), page.num_values())?;

        let iter = reps.zip(defs).peekable();

        Ok(Self { iter })
    }
*/

/**
pub struct NestedPage<'a> {
    iter: std::iter::Peekable<std::iter::Zip<HybridRleDecoder<'a>, HybridRleDecoder<'a>>>,
}

impl<'a> NestedPage<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self> {
        let (rep_levels, def_levels, _) = split_buffer(page)?;

        let max_rep_level = page.descriptor.max_rep_level;
        let max_def_level = page.descriptor.max_def_level;

        let reps =
            HybridRleDecoder::try_new(rep_levels, get_bit_width(max_rep_level), page.num_values())?;
        let defs =
            HybridRleDecoder::try_new(def_levels, get_bit_width(max_def_level), page.num_values())?;

        let iter = reps.zip(defs).peekable();

        Ok(Self { iter })
    }

        let max_rep_level = leaf.descriptor.max_rep_level;
        let max_def_level = leaf.descriptor.max_def_level;
*/







pub fn read_validity_nested<R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    leaf: &ColumnDescriptor,
    mut init: Vec<InitNested>,
    scratch: &mut Vec<u8>,
) -> Result<(NestedState, Option<Bitmap>)> {

    let offset_length = read_u32(reader)?;
    let rep_levels_len = read_u32(reader)?;
    let def_levels_len = read_u32(reader)?;
    println!("\n\nrep_levels_len={:?}", rep_levels_len);
    println!("def_levels_len={:?}", def_levels_len);

    let max_rep_level = leaf.descriptor.max_rep_level;
    let max_def_level = leaf.descriptor.max_def_level;

    scratch.resize(rep_levels_len as usize, 0);
    reader.read_exact(scratch.as_mut_slice())?;
    let rep_levels = scratch.clone();

    scratch.resize(def_levels_len as usize, 0);
    reader.read_exact(scratch.as_mut_slice())?;
    let def_levels = scratch.clone();

    // 05d6 0105 ff03
    //scratch=[5, 214, 1, 5, 255, 3]
    //nested=[List(ListNested { is_optional: false, offsets: [0, 3, 5, 9, 10], validity: None }), Primitive(None, false, 10)]

    //nested=NestedState { nested: [NestedValid { offsets: [] }] }
    //nested=NestedState { nested: [NestedValid { offsets: [1, 1] }] }
    println!("max_rep_level={:?}", max_rep_level);
    println!("max_def_level={:?}", max_def_level);

    println!("rep_levels={:?}", rep_levels);
    println!("def_levels={:?}", def_levels);

    //let length = 10;
    println!("length={:?}", length);

    let reps =
        HybridRleDecoder::try_new(&rep_levels, get_bit_width(max_rep_level), length)?;
    let defs =
        HybridRleDecoder::try_new(&def_levels, get_bit_width(max_def_level), length)?;

    let mut page_iter = reps.zip(defs).peekable();

    let mut nested = init_nested(&init, length);

    println!("nested={:?}", nested);

//nested=NestedState { nested: [NestedValid { offsets: [] }] }
//00nested=NestedState { nested: [NestedValid { offsets: [] }, NestedPrimitive { is_nullable: false, length: 0 }] }
//existing=0 additional=4


    let additional = offset_length;
    println!("additional={:?}", additional);

    let max_depth = nested.nested.len();

    let mut cum_sum = vec![0u32; max_depth + 1];
    for (i, nest) in nested.nested.iter().enumerate() {
        let delta = nest.is_nullable() as u32 + nest.is_repeated() as u32;
        cum_sum[i + 1] = cum_sum[i] + delta;
    }

    let mut cum_rep = vec![0u32; max_depth + 1];
    for (i, nest) in nested.nested.iter().enumerate() {
        let delta = nest.is_repeated() as u32;
        cum_rep[i + 1] = cum_rep[i] + delta;
    }
    println!("max_depth={:?}", max_depth);
    println!("cum_sum={:?}", cum_sum);
    println!("cum_rep={:?}", cum_rep);

/**
rep=0, def=1
rep=1, def=1
rep=1, def=1
rep=0, def=1

right_level=true
is_valid=true
additional=4 next_rep=1 rows=1
additional=4 next_rep=1 rows=1
additional=4 next_rep=0 rows=1
right_level=true
is_valid=true
additional=4 next_rep=0 rows=2
*/

    let mut is_nullable = false;
    let mut builder = MutableBitmap::with_capacity(length);

    let mut rows = 0;
    while let Some((rep, def)) = page_iter.next() {
        let rep = rep?;
        let def = def?;
        // rep=0, def=1
        // rep=1, def=1
        // rep=1, def=1
        // rep=0, def=1
        println!("rep={:?}, def={:?}", rep, def);
        if rep == 0 {
            rows += 1;
        }

        let mut is_required = false;
        for depth in 0..max_depth {
            let right_level = rep <= cum_rep[depth] && def >= cum_sum[depth];
            if is_required || right_level {
                let length = nested.nested
                    .get(depth + 1)
                    .map(|x| x.len() as i64)
                    // the last depth is the leaf, which is always increased by 1
                    .unwrap_or(1);
                println!("-----11112222nested.nested={:?}", nested.nested);

                let nest = &mut nested.nested[depth];

                let is_valid = nest.is_nullable() && def > cum_sum[depth];
                nest.push(length, is_valid);
                if nest.is_required() && !is_valid {
                    is_required = true;
                } else {
                    is_required = false
                };

                if depth == max_depth - 1 {
                    // the leaf / primitive
                    println!("------------1111nest.is_nullable()={:?}", nest.is_nullable());
                    is_nullable = nest.is_nullable();
                    if is_nullable {
                        let is_valid = (def != cum_sum[depth]) || !nest.is_nullable();
                        println!("right_level={:?}", right_level);
                        println!("is_valid={:?}", is_valid);
                        if right_level && is_valid {
                            unsafe { builder.push_unchecked(true) };
                        } else {
                            unsafe { builder.push_unchecked(false) };
                        }
                    }
                }
            }
        }

        let next_rep = *page_iter
            .peek()
            .map(|x| x.0.as_ref())
            .transpose()
            .unwrap() // todo: fix this
            .unwrap_or(&0);
        println!("additional={:?} next_rep={:?} rows={:?}", additional, next_rep, rows);

        if next_rep == 0 && rows == additional {
            break;
        }
    }

//nested=NestedState { nested: [NestedValid { offsets: [0, 3, 5, 9] }, NestedPrimitive { is_nullable: false, length: 10 }] }
//nested=[NestedValid { offsets: [0, 3, 5, 9] }, NestedPrimitive { is_nullable: false, length: 9 }]

    println!("nested={:?}", nested);
    println!("base_type={:?}", leaf.base_type);

    let validity = if is_nullable {
        Some(std::mem::take(&mut builder).into())
    } else {
        None
    };

    Ok((nested, validity))
}















pub fn read_u8<R: Read>(r: &mut R) -> Result<u8> {
    let mut buf = [0; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub fn read_u32<R: Read>(r: &mut R) -> Result<u32> {
    let mut buf = [0; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

pub fn read_u64<R: Read>(r: &mut R) -> Result<u64> {
    let mut buf = [0; 8];
    r.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}
