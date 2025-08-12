use pyo3::prelude::*;
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use std::sync::Arc;
use crate::*;

/// Utilities for schema conversion between PyArrow, Arrow, and Fluss
pub struct Utils;

impl Utils {
    /// Convert PyArrow schema to Rust Arrow schema
    pub fn pyarrow_to_arrow_schema(py_schema: &PyObject) -> PyResult<SchemaRef> {
        Python::with_gil(|py| {
            let schema_bound = py_schema.bind(py);
            
            let schema: ArrowSchema = arrow_pyarrow::FromPyArrow::from_pyarrow_bound(&schema_bound)
                .map_err(|e| FlussError::new_err(format!("Failed to convert PyArrow schema: {}", e)))?;
            Ok(Arc::new(schema))
        })
    }

    /// Convert Arrow DataType to Fluss DataType
    pub fn arrow_type_to_fluss_type(arrow_type: &arrow::datatypes::DataType) -> PyResult<fcore::metadata::DataType> {
        use arrow::datatypes::DataType as ArrowDataType;
        use fcore::metadata::DataTypes;

        let fluss_type = match arrow_type {
            ArrowDataType::Boolean => DataTypes::boolean(),
            ArrowDataType::Int8 => DataTypes::tinyint(),
            ArrowDataType::Int16 => DataTypes::smallint(),
            ArrowDataType::Int32 => DataTypes::int(),
            ArrowDataType::Int64 => DataTypes::bigint(),
            ArrowDataType::UInt8 => DataTypes::tinyint(),
            ArrowDataType::UInt16 => DataTypes::smallint(),
            ArrowDataType::UInt32 => DataTypes::int(),
            ArrowDataType::UInt64 => DataTypes::bigint(),
            ArrowDataType::Float32 => DataTypes::float(),
            ArrowDataType::Float64 => DataTypes::double(),
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataTypes::string(),
            ArrowDataType::Binary | ArrowDataType::LargeBinary => DataTypes::bytes(),
            ArrowDataType::Date32 => DataTypes::date(),
            ArrowDataType::Date64 => DataTypes::date(),
            ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => DataTypes::time(),
            ArrowDataType::Timestamp(_, _) => DataTypes::timestamp(),
            ArrowDataType::Decimal128(precision, scale) => DataTypes::decimal(*precision as u32, *scale as u32),
            _ => {
                return Err(FlussError::new_err(format!(
                    "Unsupported Arrow data type: {:?}", arrow_type
                )));
            }
        };

        Ok(fluss_type)
    }

    pub fn datatype_to_string(data_type: &fcore::metadata::DataType) -> String {
        match data_type {
            fcore::metadata::DataType::Boolean(_) => "boolean".to_string(),
            fcore::metadata::DataType::TinyInt(_) => "tinyint".to_string(),
            fcore::metadata::DataType::SmallInt(_) => "smallint".to_string(),
            fcore::metadata::DataType::Int(_) => "int".to_string(),
            fcore::metadata::DataType::BigInt(_) => "bigint".to_string(),
            fcore::metadata::DataType::Float(_) => "float".to_string(),
            fcore::metadata::DataType::Double(_) => "double".to_string(),
            fcore::metadata::DataType::String(_) => "string".to_string(),
            fcore::metadata::DataType::Bytes(_) => "bytes".to_string(),
            fcore::metadata::DataType::Date(_) => "date".to_string(),
            fcore::metadata::DataType::Time(t) => {
                if t.precision() == 0 {
                    "time".to_string()
                } else {
                    format!("time({})", t.precision())
                }
            },
            fcore::metadata::DataType::Timestamp(t) => {
                if t.precision() == 6 {
                    "timestamp".to_string()
                } else {
                    format!("timestamp({})", t.precision())
                }
            },
            fcore::metadata::DataType::TimestampLTz(t) => {
                if t.precision() == 6 {
                    "timestamp_ltz".to_string()
                } else {
                    format!("timestamp_ltz({})", t.precision())
                }
            },
            fcore::metadata::DataType::Char(c) => format!("char({})", c.length()),
            fcore::metadata::DataType::Decimal(d) => format!("decimal({},{})", d.precision(), d.scale()),
            fcore::metadata::DataType::Binary(b) => format!("binary({})", b.length()),
            fcore::metadata::DataType::Array(arr) => format!("array<{}>", Utils::datatype_to_string(arr.get_element_type())),
            fcore::metadata::DataType::Map(map) => format!("map<{},{}>", 
                                        Utils::datatype_to_string(map.key_type()), 
                                        Utils::datatype_to_string(map.value_type())),
            fcore::metadata::DataType::Row(row) => {
                let fields: Vec<String> = row.fields().iter()
                    .map(|field| format!("{}: {}", field.name(), Utils::datatype_to_string(field.data_type())))
                    .collect();
                format!("row<{}>", fields.join(", "))
            },
        }
    }

    /// Convert Fluss Schema to Arrow Schema (future use)
    pub fn fluss_schema_to_arrow_schema(_fluss_schema: &fcore::metadata::Schema) -> PyResult<SchemaRef> {
        // TODO: 实现 Fluss Schema 到 Arrow Schema 的转换
        // 这个在将来需要将 Fluss Schema 转换回 PyArrow 时会用到
        Err(FlussError::new_err("Not implemented yet".to_string()))
    }

    /// Convert Arrow Schema to PyArrow Schema (future use)
    pub fn arrow_schema_to_pyarrow(_arrow_schema: SchemaRef) -> PyResult<PyObject> {
        // TODO: 实现 Arrow Schema 到 PyArrow Schema 的转换
        // 这个在需要返回 PyArrow Schema 给 Python 时会用到
        Python::with_gil(|py| Ok(py.None()))
    }
}

/// Format parsing utilities
pub struct FormatUtils;

impl FormatUtils {
    /// Parse log format string to LogFormat enum
    pub fn parse_log_format(format_str: &str) -> PyResult<fcore::metadata::LogFormat> {
        fcore::metadata::LogFormat::parse(format_str)
            .map_err(|e| FlussError::new_err(format!("Invalid log format '{}': {}", format_str, e)))
    }

    /// Parse kv format string to KvFormat enum
    pub fn parse_kv_format(format_str: &str) -> PyResult<fcore::metadata::KvFormat> {
        fcore::metadata::KvFormat::parse(format_str)
            .map_err(|e| FlussError::new_err(format!("Invalid kv format '{}': {}", format_str, e)))
    }
}