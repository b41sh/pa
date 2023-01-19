use arrow::array::Array;
use arrow::array::StructArray;
use arrow::array::ListArray;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;


use arrow::io::parquet::write::Nested;
use arrow::io::parquet::write::ListNested;

use arrow::error::Result;


/// Constructs the necessary `Vec<Vec<Nested>>` to write the rep and def levels of `array` to parquet
pub fn to_nested<'a>(array: &'a dyn Array, field: &Field) -> Result<Vec<Vec<Nested<'a>>>> {
    let mut nested = vec![];

    to_nested_recursive(array, field, &mut nested, vec![])?;
    Ok(nested)
}

/**
/// Checks whether this schema is nullable.
pub(crate) fn is_nullable(field_info: &FieldInfo) -> bool {
    match field_info.repetition {
        Repetition::Optional => true,
        Repetition::Repeated => true,
        Repetition::Required => false,
    }
}
*/

fn to_nested_recursive<'a>(
    array: &'a dyn Array,
    field: &Field,
    nested: &mut Vec<Vec<Nested<'a>>>,
    mut parents: Vec<Nested<'a>>,
) -> Result<()> {
    //let is_optional = is_nullable(type_.get_field_info());
    let is_optional = field.is_nullable;

    match &field.data_type {
        DataType::Struct(inner_fields) => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            parents.push(Nested::Struct(array.validity(), is_optional, array.len()));

            for (inner_field, array) in inner_fields.iter().zip(array.values()) {
                to_nested_recursive(array.as_ref(), inner_field, nested, parents.clone())?;
            }
        }
        DataType::List(inner_field) => {
            let array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            parents.push(Nested::List(ListNested::new(
                array.offsets().buffer(),
                array.validity(),
                true,
            )));
            to_nested_recursive(array.values().as_ref(), &inner_field, nested, parents)?;
        }
        DataType::LargeList(inner_field) => {
            let array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            parents.push(Nested::LargeList(ListNested::new(
                array.offsets().buffer(),
                array.validity(),
                true,
            )));
            to_nested_recursive(array.values().as_ref(), &inner_field, nested, parents)?;
        }
        _ => {
            parents.push(Nested::Primitive(
                array.validity(),
                is_optional,
                array.len(),
            ));
            nested.push(parents)
        }
    }
    Ok(())
}


pub fn to_leaves(array: &dyn Array) -> Vec<&dyn Array> {
    let mut leaves = vec![];
    to_leaves_recursive(array, &mut leaves);
    leaves
}

fn to_leaves_recursive<'a>(array: &'a dyn Array, leaves: &mut Vec<&'a dyn Array>) {
    use arrow::datatypes::PhysicalType::*;

    match array.data_type().to_physical_type() {
        Struct => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            array
                .values()
                .iter()
                .for_each(|a| to_leaves_recursive(a.as_ref(), leaves));
        }
        List => {
            let array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            to_leaves_recursive(array.values().as_ref(), leaves);
        }
        LargeList => {
            let array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            to_leaves_recursive(array.values().as_ref(), leaves);
        }
        Null | Boolean | Primitive(_) | Binary | FixedSizeBinary | LargeBinary | Utf8
        | LargeUtf8 | Dictionary(_) => leaves.push(array),
        other => todo!("Writing {:?} to parquet not yet implemented", other),
    }
}


pub fn to_data_type_leaves(field: &Field) -> Vec<DataType> {
    let mut leaves = vec![];
    to_data_type_leaves_recursive(field, &mut leaves);
    leaves
}

fn to_data_type_leaves_recursive(field: &Field, leaves: &mut Vec<DataType>) {
    match &field.data_type {
        DataType::Struct(inner_fields) => {
            inner_fields
                .into_iter()
                .for_each(|inner_field| to_data_type_leaves_recursive(&inner_field, leaves));
        }
        DataType::List(inner_field) | DataType::LargeList(inner_field) => {
            to_data_type_leaves_recursive(&inner_field, leaves);
        }
        _ => leaves.push(field.data_type.clone()),
    }
}
