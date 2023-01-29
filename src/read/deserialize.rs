use crate::with_match_primitive_type;
use crate::PageMeta;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, PhysicalType};
use arrow::error::Result;
use arrow::offset::OffsetsBuffer;
use arrow::io::parquet::read::{InitNested, NestedState, n_columns, create_list};
use parquet2::metadata::ColumnDescriptor;

use super::{array::*, NativeReadBuf};

pub fn read_simple<R: NativeReadBuf>(
    reader: &mut R,
    data_type: DataType,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<Box<dyn Array>> {
    use PhysicalType::*;

    match data_type.to_physical_type() {
        Null => read_null(data_type, length).map(|x| x.boxed()),
        Boolean => read_boolean(reader, data_type, length, scratch).map(|x| x.boxed()),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            read_primitive::<$T, _>(
                reader,
                data_type,
                length,
                scratch
            )
            .map(|x| x.boxed())
        }),
        Binary => read_binary::<i32, _>(reader, data_type, length, scratch).map(|x| x.boxed()),
        LargeBinary => read_binary::<i64, _>(reader, data_type, length, scratch).map(|x| x.boxed()),

        FixedSizeBinary => unimplemented!(),

        Utf8 => read_utf8::<i32, _>(reader, data_type, length, scratch).map(|x| x.boxed()),

        LargeUtf8 => read_utf8::<i64, _>(reader, data_type, length, scratch).map(|x| x.boxed()),
/**
        List => {
            let sub_filed = if let DataType::List(field) = data_type.to_logical_type() {
                field
            } else {
                unreachable!()
            };
            let offset_size =
                read_primitive::<i32, _>(reader, DataType::Int32, 1, scratch)?.value(0);
            let offset =
                read_primitive::<i32, _>(reader, DataType::Int32, offset_size as usize, scratch)?;
            let value = read(
                reader,
                sub_filed.clone().data_type,
                length * (offset_size - 1) as usize,
                scratch,
            )?;
            ListArray::try_new(
                data_type,
                unsafe { OffsetsBuffer::new_unchecked(offset.values().to_owned()) },
                value,
                None,
            )
            .map(|x| x.boxed())
        }
        LargeList => unimplemented!(),
        FixedSizeList => unimplemented!(),
        Struct => {
            let children_fields = if let DataType::Struct(children) = data_type.to_logical_type() {
                children
            } else {
                unreachable!()
            };
            let mut value = vec![];
            for f in children_fields {
                value.push(read(reader, f.clone().data_type, length, scratch)?);
            }
            StructArray::try_new(data_type, value, None).map(|x| x.boxed())
        }
*/

        List => unimplemented!(),
        LargeList => unimplemented!(),
        FixedSizeList => unimplemented!(),
        Struct => unimplemented!(),

        Dictionary(_key_type) => unimplemented!(),
        Union => unimplemented!(),
        Map => unimplemented!(),
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
    println!("data_type={:?}", field.data_type());
    println!("page_metas={:?}", page_metas);
    use PhysicalType::*;

    match field.data_type().to_physical_type() {
        //Null => read_null(field.data_type().clone(), page_metas[0].num_values as usize).map(|x| x.boxed()),
        Boolean => {
            init.push(InitNested::Primitive(field.is_nullable));
            let (nested, array) = read_boolean_nested(
                &mut readers[0],
                field.data_type().clone(),
                &leaves[0],
                init,
                page_metas[0].num_values as usize,
                &mut scratchs[0]
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
                &mut scratchs[0]
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
                &mut scratchs[0]
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
                &mut scratchs[0]
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
                &mut scratchs[0]
            )?;
            Ok((nested, array.boxed()))
        }
        _ => match field.data_type().to_logical_type() {
            DataType::List(inner)
            | DataType::LargeList(inner)
            | DataType::FixedSizeList(inner, _) => {
                init.push(InitNested::List(field.is_nullable));
                let (mut nested, values) = read_nested(
                    readers,
                    *inner.clone(),
                    leaves,
                    page_metas,
                    init,
                    scratchs,
                )?;

                let array = create_list(field.data_type().clone(), &mut nested, values);
                println!("array={:?}", array);

                Ok((nested, array))
            }
            DataType::Struct(fields) => {
                let mut values = Vec::with_capacity(fields.len());
                for f in fields.iter() {
                    let mut init = init.clone();
                    //init.push(InitNested::Struct(false));
                    init.push(InitNested::Struct(field.is_nullable));
                    let n = n_columns(f.data_type());
                    println!("nnn={:?}", n);

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
                let array = StructArray::new(
                    field.data_type().clone(),
                    values,
                    None,
                );
                println!("array={:?}", array);

                Ok((NestedState::new(vec![]), array.boxed()))
            }
            _ => unimplemented!(),
        }
        _ => unimplemented!(),
    }
}
