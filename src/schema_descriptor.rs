use arrow::datatypes::DataType;
use arrow::datatypes::Field;


/// A schema descriptor. This encapsulates the top-level schemas for all the columns,
/// as well as all descriptors for all the primitive columns.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde_types", derive(Deserialize, Serialize))]
pub struct SchemaDescriptor {
    //name: String,
    // The top-level schema (the "message" type).
    fields: Vec<Field>,

    // All the descriptors for primitive columns in this schema, constructed from
    // `schema` in DFS order.
    leaves: Vec<ColumnDescriptor>,
}

impl SchemaDescriptor {
    /// Creates new schema descriptor from Arrow schema.
    //pub fn new(name: String, fields: Vec<Field>) -> Self {
    pub fn new(fields: Vec<Field>) -> Self {
        let mut leaves = vec![];
        for f in &fields {
            let mut path = vec![];
            build_tree(f, f, 0, 0, &mut leaves, &mut path);
        }

        Self {
            //name,
            fields,
            leaves,
        }
    }

    /// The [`ColumnDescriptor`] (leafs) of this schema.
    ///
    /// Note that, for nested fields, this may contain more entries than the number of fields
    /// in the file - e.g. a struct field may have two columns.
    pub fn columns(&self) -> &[ColumnDescriptor] {
        &self.leaves
    }

    /// The schemas' name.
    //pub fn name(&self) -> &str {
    //    &self.name
    //}

    /// The schemas' fields.
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }
}

fn build_tree<'a>(
    tp: &'a Field,
    base_tp: &Field,
    mut max_rep_level: i16,
    mut max_def_level: i16,
    leaves: &mut Vec<ColumnDescriptor>,
    path_so_far: &mut Vec<&'a str>,
) {
    path_so_far.push(&tp.name);

/**
    // list 内部的那个 repetition 是 Repetition::Repeated
    let repetition = if tp.is_nullable {
        Repetition::Optional
    } else {
        Repetition::Required
    };

    //match tp.get_field_info().repetition {
    match repetition {
        Repetition::Optional => {
            max_def_level += 1;
        }
        Repetition::Repeated => {
            max_def_level += 1;
            max_rep_level += 1;
        }
        _ => {}
    }
*/

    if tp.is_nullable {
        max_def_level += 1;
    }

    println!("tp={:?}", tp);

    match tp.data_type() {
        DataType::Null | DataType::Boolean |
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
        DataType::Float32 | DataType::Float64 |
        DataType::Utf8 | DataType::LargeUtf8 => {
            let path_in_schema = path_so_far.iter().copied().map(String::from).collect();
            leaves.push(ColumnDescriptor::new(
                path_in_schema,
                tp.data_type().clone(),
                base_tp.data_type().clone(),
                max_def_level,
                max_rep_level,
            ));
        }
        DataType::List(f) | DataType::FixedSizeList(f, _) | DataType::LargeList(f) => {
            max_def_level += 1;
            max_rep_level += 1;

            build_tree(
                f,
                base_tp,
                max_rep_level,
                max_def_level,
                leaves,
                path_so_far,
            );
            path_so_far.pop();
        }
        DataType::Struct(ref fields) => {
            for f in fields {
                build_tree(
                    f,
                    base_tp,
                    max_rep_level,
                    max_def_level,
                    leaves,
                    path_so_far,
                );
                path_so_far.pop();
            }
        }
        _ => todo!(),
    }
}












/// A descriptor for leaf-level primitive columns.
/// This encapsulates information such as definition and repetition levels and is used to
/// re-assemble nested data.
#[derive(Debug, PartialEq, Clone)]
#[cfg_attr(feature = "serde_types", derive(Deserialize, Serialize))]
pub struct ColumnDescriptor {
    /// The path of this column. For instance, "a.b.c.d".
    pub path_in_schema: Vec<String>,

    /// The repetition ?
    //pub repetition: Repetition,

    /// The [`DataType`] this descriptor is a leaf of
    pub data_type: DataType,

    /// The [`DataType`] this descriptor is a leaf of
    pub base_type: DataType,

    /// The maximum definition level
    pub max_def_level: i16,

    /// The maximum repetition level
    pub max_rep_level: i16,
}

impl ColumnDescriptor {
    /// Creates new descriptor for leaf-level column.
    pub fn new(
        path_in_schema: Vec<String>,
        data_type: DataType,
        base_type: DataType,
        max_def_level: i16,
        max_rep_level: i16,
    ) -> Self {
        Self {
            path_in_schema,
            data_type,
            base_type,
            max_def_level,
            max_rep_level,
        }
    }
}



