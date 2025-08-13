use pyo3::prelude::*;
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use std::sync::Arc;
use crate::*;

/// Utilities for schema conversion between PyArrow, Arrow, and Fluss
pub struct Utils;

impl Utils {
    // Convert PyArrow schema to Rust Arrow schema
    pub fn pyarrow_to_arrow_schema(py_schema: &PyObject) -> PyResult<SchemaRef> {
        Python::with_gil(|py| {
            let schema_bound = py_schema.bind(py);
            
            let schema: ArrowSchema = arrow_pyarrow::FromPyArrow::from_pyarrow_bound(&schema_bound)
                .map_err(|e| FlussError::new_err(format!("Failed to convert PyArrow schema: {}", e)))?;
            Ok(Arc::new(schema))
        })
    }

    // Convert Arrow DataType to Fluss DataType
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

    // Parse log format string to LogFormat enum
    pub fn parse_log_format(format_str: &str) -> PyResult<fcore::metadata::LogFormat> {
        fcore::metadata::LogFormat::parse(format_str)
            .map_err(|e| FlussError::new_err(format!("Invalid log format '{}': {}", format_str, e)))
    }

    // Parse kv format string to KvFormat enum
    pub fn parse_kv_format(format_str: &str) -> PyResult<fcore::metadata::KvFormat> {
        fcore::metadata::KvFormat::parse(format_str)
            .map_err(|e| FlussError::new_err(format!("Invalid kv format '{}': {}", format_str, e)))
    }

    // Convert ScanRecords to Arrow RecordBatch
    pub fn convert_scan_records_to_arrow(
        py: Python, 
        _scan_records: fcore::record::ScanRecords,
        schema: &fcore::metadata::Schema,
    ) -> PyResult<PyObject> {
        let pyarrow = py.import("pyarrow")?;
        
        // Convert ScanRecords to Arrow format
        // For now, return empty batch - this needs actual implementation based on your schema
        let arrow_schema = Self::create_arrow_schema(py, schema)?;
        
        // Create empty arrays for each field in the schema
        let builtins = py.import("builtins")?;
        let empty_list = builtins.getattr("list")?.call0()?;
        let empty_batch = pyarrow
            .getattr("RecordBatch")?
            .call_method1("from_arrays", (empty_list, arrow_schema))?;
        
        Ok(empty_batch.into())
    }
    
    // Create Arrow schema from table schema
    pub fn create_arrow_schema(py: Python, _schema: &fcore::metadata::Schema) -> PyResult<PyObject> {
        let pyarrow = py.import("pyarrow")?;
        
        // Create a simple schema for now - this needs actual implementation based on your table schema
        let builtins = py.import("builtins")?;
        let fields = builtins.getattr("list")?.call0()?;
        let schema = pyarrow
            .getattr("schema")?
            .call1((fields,))?;
        
        Ok(schema.into())
    }
    
    // Combine multiple Arrow batches into a single Table
    pub fn combine_batches_to_table(py: Python, batches: Vec<PyObject>) -> PyResult<PyObject> {
        let pyarrow = py.import("pyarrow")?;
        
        // Use pyarrow.Table.from_batches to combine batches
        let table = pyarrow
            .getattr("Table")?
            .call_method1("from_batches", (batches,))?;
        
        Ok(table.into())
    }
}
