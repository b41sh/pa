use crate::with_match_primitive_type;
use crate::PageMeta;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, PhysicalType};
use arrow::error::Result;
use arrow::io::parquet::read::{create_list, n_columns, ArrayIter, InitNested, NestedState};
use parquet2::metadata::ColumnDescriptor;

use super::reader::NnativeReader;
use super::{array::*, NativeReadBuf};

pub type NestedArrayIter<'a> =
    Box<dyn Iterator<Item = Result<(NestedState, Box<dyn Array>)>> + Send + Sync + 'a>;

pub fn read_simple<R: NativeReadBuf>(
    reader: &mut R,
    field: Field,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<Box<dyn Array>> {
    use PhysicalType::*;

    let is_nullable = field.is_nullable;
    let data_type = field.data_type().clone();

    match data_type.to_physical_type() {
        Null => read_null(data_type, length).map(|x| x.boxed()),
        Boolean => read_boolean(reader, is_nullable, data_type, length, scratch).map(|x| x.boxed()),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            read_primitive::<$T, _>(
                reader,
                is_nullable,
                data_type,
                length,
                scratch
            )
            .map(|x| x.boxed())
        }),
        Binary => read_binary::<i32, _>(reader, is_nullable, data_type, length, scratch)
            .map(|x| x.boxed()),
        LargeBinary => read_binary::<i64, _>(reader, is_nullable, data_type, length, scratch)
            .map(|x| x.boxed()),
        FixedSizeBinary => unimplemented!(),
        Utf8 => {
            read_utf8::<i32, _>(reader, is_nullable, data_type, length, scratch).map(|x| x.boxed())
        }
        LargeUtf8 => {
            read_utf8::<i64, _>(reader, is_nullable, data_type, length, scratch).map(|x| x.boxed())
        }
        _ => unreachable!(),
    }
}

pub fn read_nested<R: NativeReadBuf>(
    readers: &mut Vec<R>,
    field: Field,
    leaves: &mut Vec<ColumnDescriptor>,
    mut page_metas: Vec<PageMeta>,
    mut init: Vec<InitNested>,
    scratchs: &mut Vec<Vec<u8>>,
) -> Result<(NestedState, Box<dyn Array>)> {
    use PhysicalType::*;

    match field.data_type().to_physical_type() {
        Null => unimplemented!(),
        Boolean => {
            init.push(InitNested::Primitive(field.is_nullable));
            let (nested, array) = read_boolean_nested(
                &mut readers[0],
                field.data_type().clone(),
                &leaves[0],
                init,
                page_metas[0].num_values as usize,
                &mut scratchs[0],
            )?;
            Ok((nested, array.boxed()))
        }
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            init.push(InitNested::Primitive(field.is_nullable));
            let (nested, array) = read_primitive_nested::<$T, _>(
                &mut readers[0],
                field.data_type().clone(),
                &leaves[0],
                init,
                page_metas[0].num_values as usize,
                &mut scratchs[0]
            )?;
            Ok((nested, array.boxed()))
        }),
        Binary => {
            init.push(InitNested::Primitive(field.is_nullable));
            let (nested, array) = read_binary_nested::<i32, _>(
                &mut readers[0],
                field.data_type().clone(),
                &leaves[0],
                init,
                page_metas[0].num_values as usize,
                &mut scratchs[0],
            )?;
            Ok((nested, array.boxed()))
        }
        LargeBinary => {
            init.push(InitNested::Primitive(field.is_nullable));
            let (nested, array) = read_binary_nested::<i64, _>(
                &mut readers[0],
                field.data_type().clone(),
                &leaves[0],
                init,
                page_metas[0].num_values as usize,
                &mut scratchs[0],
            )?;
            Ok((nested, array.boxed()))
        }
        Utf8 => {
            init.push(InitNested::Primitive(field.is_nullable));
            let (nested, array) = read_utf8_nested::<i32, _>(
                &mut readers[0],
                field.data_type().clone(),
                &leaves[0],
                init,
                page_metas[0].num_values as usize,
                &mut scratchs[0],
            )?;
            Ok((nested, array.boxed()))
        }
        LargeUtf8 => {
            init.push(InitNested::Primitive(field.is_nullable));
            let (nested, array) = read_utf8_nested::<i64, _>(
                &mut readers[0],
                field.data_type().clone(),
                &leaves[0],
                init,
                page_metas[0].num_values as usize,
                &mut scratchs[0],
            )?;
            Ok((nested, array.boxed()))
        }
        _ => match field.data_type().to_logical_type() {
            DataType::List(inner)
            | DataType::LargeList(inner)
            | DataType::FixedSizeList(inner, _) => {
                init.push(InitNested::List(field.is_nullable));
                let (mut nested, values) =
                    read_nested(readers, *inner.clone(), leaves, page_metas, init, scratchs)?;
                let array = create_list(field.data_type().clone(), &mut nested, values);
                Ok((nested, array))
            }
            DataType::Struct(fields) => {
                let mut values = Vec::with_capacity(fields.len());
                for f in fields.iter() {
                    let mut init = init.clone();
                    init.push(InitNested::Struct(field.is_nullable));
                    let n = n_columns(f.data_type());

                    let mut inner_readers: Vec<_> = readers.drain(..n).collect();
                    let mut inner_scratchs: Vec<_> = scratchs.drain(..n).collect();
                    let mut inner_leaves: Vec<_> = leaves.drain(..n).collect();
                    let inner_page_metas: Vec<_> = page_metas.drain(..n).collect();

                    let (_, value) = read_nested(
                        &mut inner_readers,
                        f.clone(),
                        &mut inner_leaves,
                        inner_page_metas,
                        init,
                        &mut inner_scratchs,
                    )?;
                    values.push(value);
                }
                // TODO(b41sh): struct array validity
                let array = StructArray::new(field.data_type().clone(), values, None);
                Ok((NestedState::new(vec![]), array.boxed()))
            }
            _ => unreachable!(),
        },
    }
}

#[inline]
fn dyn_iter<'a, A, I>(iter: I) -> ArrayIter<'a>
where
    A: Array,
    I: Iterator<Item = Result<A>> + Send + Sync + 'a,
{
    Box::new(iter.map(|x| x.map(|x| Box::new(x) as Box<dyn Array>)))
}

pub fn page_iter_to_arrays<'a, I: 'a>(reader: I, field: Field) -> Result<ArrayIter<'a>>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + Send + Sync,
{
    use DataType::*;

    let is_nullable = field.is_nullable;
    let data_type = field.data_type().clone();

    Ok(match data_type.to_logical_type() {
        Boolean => dyn_iter(BooleanIter::new(reader, is_nullable, data_type)),
        //_ => todo!(),
        _ => unreachable!(),
    })
}

fn columns_to_iter_recursive<'a, I: 'a>(
    mut readers: Vec<I>,
    mut leaves: &mut Vec<ColumnDescriptor>,
    field: Field,
    init: Vec<InitNested>,
    is_nested: bool,
) -> Result<NestedArrayIter<'a>>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + Send + Sync,
{
    if !is_nested {
        Ok(Box::new(
            page_iter_to_arrays(readers.pop().unwrap(), field)?
                .map(|x| Ok((NestedState::new(vec![]), x?))),
        ))
    } else {
        todo!()
    }
}

pub fn column_iter_to_arrays<'a, I: 'a>(
    readers: Vec<I>,
    leaves: &mut Vec<ColumnDescriptor>,
    field: Field,
    is_nested: bool,
) -> Result<ArrayIter<'a>>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + Send + Sync,
{
    Ok(Box::new(
        columns_to_iter_recursive(readers, leaves, field, vec![], is_nested)?
            .map(|x| x.map(|x| x.1)),
    ))
}
