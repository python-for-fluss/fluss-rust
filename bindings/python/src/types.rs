use pyo3::prelude::*;
use crate::*;
use pyo3::types::PyDict;
use std::{collections::HashMap, time::Duration};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use fcore::row::InternalRow;

#[pyclass]
#[derive(Clone)]
pub struct TablePath {
    database_name: String,
    table_name: String,
}

#[pymethods]
impl TablePath {
    // Create a new TablePath
    #[new]
    pub fn new(database_name: String, table_name: String) -> Self {
        Self {
            database_name,
            table_name,
        }
    }
    
    // Get the database name
    #[getter]
    pub fn database_name(&self) -> String {
        self.database_name.clone()
    }
    
    // Get the table name  
    #[getter]
    pub fn table_name(&self) -> String {
        self.table_name.clone()
    }

    // Get table path as string
    pub fn table_path_str(&self) -> String {
        format!("{}.{}", self.database_name, self.table_name)
    }

    pub fn __str__(&self) -> String {
        format!("{}.{}", self.database_name, self.table_name)
    }
    
    fn __repr__(&self) -> String {
        format!("TablePath('{}', '{}')", self.database_name, self.table_name)
    }
}

impl TablePath {
    // Convert to core TablePath
    pub fn to_core(&self) -> fcore::metadata::TablePath {
        fcore::metadata::TablePath::new(self.database_name.clone(), self.table_name.clone())
    }

    pub fn from_core(core_path: fcore::metadata::TablePath) -> Self {
        Self {
            database_name: core_path.database().to_string(),
            table_name: core_path.table().to_string(),
        }
    }
}

#[pyclass]
pub struct Schema {
    __schema: fcore::metadata::Schema,
}

#[pymethods]
impl Schema {
    #[new]
    #[pyo3(signature = (schema, primary_keys=None, primary_key_name=None))]
    pub fn new(
        schema: PyObject, // PyArrow schema
        primary_keys: Option<Vec<String>>,
        primary_key_name: Option<String>,
    ) -> PyResult<Self> {
        let arrow_schema = crate::utils::Utils::pyarrow_to_arrow_schema(&schema)?;
        
        let mut builder = fcore::metadata::Schema::builder();
        
        for field in arrow_schema.fields() {
            let fluss_data_type = crate::utils::Utils::arrow_type_to_fluss_type(field.data_type())?;
            builder = builder.column(field.name(), fluss_data_type);
            
            if let Some(comment) = field.metadata().get("comment") {
                builder = builder.with_comment(comment);
            }
        }
        
        if let Some(pk_columns) = primary_keys {
            if !pk_columns.is_empty() {
                if let Some(pk_name) = primary_key_name {
                    builder = builder.primary_key_named(&pk_name, pk_columns);
                } else {
                    builder = builder.primary_key(pk_columns);
                }
            }
        }
        
        let fluss_schema = builder.build()
            .map_err(|e| FlussError::new_err(format!("Failed to build schema: {}", e)))?;
        
        Ok(Self {
            __schema: fluss_schema,
        })
    }

    fn get_column_names(&self) -> Vec<String> {
        self.__schema.columns().iter().map(|col| col.name().to_string()).collect()
    }

    fn get_column_types(&self) -> Vec<String> {
        self.__schema.columns().iter()
            .map(|col| Utils::datatype_to_string(col.data_type()))
            .collect()
    }

    fn get_columns(&self) -> Vec<(String, String)> {
        self.__schema.columns().iter()
            .map(|col| (col.name().to_string(), Utils::datatype_to_string(col.data_type())))
            .collect()
    }

    // TODO: support primaryKey

    fn __str__(&self) -> String {
        format!("Schema: columns={:?}", self.get_columns())
    }
}

impl Schema {
    // Convert to core Schema
    pub fn to_core(&self) -> &fcore::metadata::Schema {
        &self.__schema
    }
}

#[pyclass]
pub struct TableDistribution {
    inner: fcore::metadata::TableDistribution,
}

#[pymethods]
impl TableDistribution {
    fn bucket_keys(&self) -> Vec<String> {
        self.inner.bucket_keys().to_vec()
    }

    fn bucket_count(&self) -> Option<i32> {
        self.inner.bucket_count()
    }
}


#[pyclass]
#[derive(Clone)]
pub struct TableDescriptor {
    __tbl_desc: fcore::metadata::TableDescriptor,
}

#[pymethods]
impl TableDescriptor {
    // Create a new TableDescriptor
    #[new]
    #[pyo3(signature = (schema, **kwargs))]
    pub fn new(
        schema: &Schema,  // fluss schema
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let mut partition_keys = Vec::new();
        let mut bucket_count = None;
        let mut bucket_keys = Vec::new();
        let mut properties = std::collections::HashMap::new();
        let mut custom_properties = std::collections::HashMap::new();
        let mut comment: Option<String> = None;
        let mut log_format = None;
        let mut kv_format = None;

        if let Some(kwargs) = kwargs {
            if let Ok(Some(pkeys)) = kwargs.get_item("partition_keys") {
                partition_keys = pkeys.extract()?;
            }
            if let Ok(Some(bcount)) = kwargs.get_item("bucket_count") {
                bucket_count = Some(bcount.extract()?);
            }
            if let Ok(Some(bkeys)) = kwargs.get_item("bucket_keys") {
                bucket_keys = bkeys.extract()?;
            }
            if let Ok(Some(props)) = kwargs.get_item("properties") {
                properties = props.extract()?;
            }
            if let Ok(Some(cprops)) = kwargs.get_item("custom_properties") {
                custom_properties = cprops.extract()?;
            }
            if let Ok(Some(comm)) = kwargs.get_item("comment") {
                comment = Some(comm.extract()?);
            }
            if let Ok(Some(lformat)) = kwargs.get_item("log_format") {
                let format_str: String = lformat.extract()?;
                log_format = Some(fcore::metadata::LogFormat::parse(&format_str)
                    .map_err(|e| FlussError::new_err(e.to_string()))?);
            }
            if let Ok(Some(kformat)) = kwargs.get_item("kv_format") {
                let format_str: String = kformat.extract()?;
                kv_format = Some(fcore::metadata::KvFormat::parse(&format_str)
                    .map_err(|e| FlussError::new_err(e.to_string()))?);
            }
        }

        let fluss_schema = schema.to_core().clone();
        
        let mut builder = fcore::metadata::TableDescriptor::builder()
            .schema(fluss_schema)
            .properties(properties)
            .custom_properties(custom_properties)
            .partitioned_by(partition_keys)
            .distributed_by(bucket_count, bucket_keys);

        if let Some(comment) = comment {
            builder = builder.comment(&comment);
        }
        if let Some(log_format) = log_format {
            builder = builder.log_format(log_format);
        }
        if let Some(kv_format) = kv_format {
            builder = builder.kv_format(kv_format);
        }

        let core_descriptor = builder.build()
            .map_err(|e| FlussError::new_err(format!("Failed to build TableDescriptor: {}", e)))?;

        Ok(Self {
            __tbl_desc: core_descriptor,
        })
    }

    pub fn get_schema(&self) -> PyResult<Schema> {
        Ok(Schema {
            __schema: self.__tbl_desc.schema().clone(),
        })
    }
}

impl TableDescriptor {
    // Convert to core TableDescriptor
    pub fn to_core(&self) -> &fcore::metadata::TableDescriptor {
        &self.__tbl_desc
    }
}

// Information about a Fluss table
#[pyclass]
#[derive(Clone)]
pub struct TableInfo {
    __table_info: fcore::metadata::TableInfo,
}

#[pymethods]
impl TableInfo {
    // Get the table ID
    #[getter]
    pub fn table_id(&self) -> i64 {
        self.__table_info.get_table_id()
    }
    
    // Get the schema ID
    #[getter]
    pub fn schema_id(&self) -> i32 {
        self.__table_info.get_schema_id()
    }
    
    // Get the table path
    #[getter]
    pub fn table_path(&self) -> TablePath {
        TablePath::from_core(self.__table_info.get_table_path().clone())
    }

    // Get the created time
    #[getter]
    pub fn created_time(&self) -> i64 {
        self.__table_info.get_created_time()
    }
    
    // Get the modified time
    #[getter]
    pub fn modified_time(&self) -> i64 {
        self.__table_info.get_modified_time()
    }
    
    // Get the primary keys
    pub fn get_primary_keys(&self) -> Vec<String> {
        self.__table_info.get_primary_keys().clone()
    }

    // Get the bucket keys
    pub fn get_bucket_keys(&self) -> Vec<String> {
        self.__table_info.get_bucket_keys().to_vec()
    }

    // Get the partition keys
    pub fn get_partition_keys(&self) -> Vec<String> {
        self.__table_info.get_partition_keys().to_vec()
    }

    // Get number of buckets
    #[getter]
    pub fn num_buckets(&self) -> i32 {
        self.__table_info.get_num_buckets()
    }

    // Check if table has primary key
    pub fn has_primary_key(&self) -> bool {
        self.__table_info.has_primary_key()
    }

    // Check if table is partitioned
    pub fn is_partitioned(&self) -> bool {
        self.__table_info.is_partitioned()
    }

    // Get properties
    pub fn get_properties(&self) -> std::collections::HashMap<String, String> {
        self.__table_info.get_properties().clone()
    }

    // Get custom properties
    pub fn get_custom_properties(&self) -> std::collections::HashMap<String, String> {
        self.__table_info.get_custom_properties().clone()
    }

    // Get comment
    #[getter]
    pub fn comment(&self) -> Option<String> {
        self.__table_info.get_comment().map(|s| s.to_string())
    }

    // get the Schema
    pub fn get_schema(&self) -> Schema {
        Schema {
            __schema: self.__table_info.get_schema().clone(),
        }
    }

    // Get column names
    pub fn get_column_names(&self) -> Vec<String> {
        self.__table_info.get_schema().columns().iter()
            .map(|col| col.name().to_string())
            .collect()
    }

    // Get column count
    pub fn get_column_count(&self) -> usize {
        self.__table_info.get_schema().columns().len()
    }
}

impl TableInfo {
    // Create from core TableInfo (internal use)
    pub fn from_core(info: fcore::metadata::TableInfo) -> Self {
        Self {
            __table_info: info,
        }
    }
}

// Result of a table scan operation
#[pyclass]
pub struct ScanResult {
    scanner: Arc<fcore::client::LogScanner>,
    table_schema: fcore::metadata::Schema,
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
    cached_records: Option<HashMap<fcore::metadata::TableBucket, Vec<fcore::record::ScanRecord>>>,
}

#[pymethods]
impl ScanResult {
    // Convert to PyArrow Table -- This fetches all data
    pub fn to_arrow(&self) -> PyResult<PyObject> {
        let scanner = self.scanner.clone();
        let schema = self.table_schema.clone();
        let start_ts = self.start_timestamp;
        let end_ts = self.end_timestamp;

        future_into_py(py, async move{
            let all_records = Self::fetch_all_records_async(scanner, end_ts).await?;

            Python::with_gil(|py| {
                Self::records_to_arrow_table(py, all_records, schema)
            })
        })
    }

    pub fn to_arrow_batch(&self) -> PyResult<PyObject> {
        // TODO: Implement actual conversion to PyArrow RecordBatch (or RecordBatchReader)
        Python::with_gil(|py| {
            println!("Converting to PyArrow Table");
            Ok(py.None())
        })
    }

    // Convert to Pandas DataFrame
    pub fn to_pandas(&self) -> PyResult<PyObject> {
        // TODO: Implement actual conversion to Pandas DataFrame
        Python::with_gil(|py| {
            println!("Converting to Pandas DataFrame");
            Ok(py.None())
        })
    }

    // Register as DuckDB table
    pub fn to_duckdb(&self, table_name: String) -> PyResult<PyObject> {
        // TODO: Implement actual DuckDB registration
        Python::with_gil(|py| {
            println!("Registering as DuckDB table: {}", table_name);
            Ok(py.None())
        })
    }

    // Get the number of records
    fn len(&self) -> usize {
        self.records.len()
    }

    // Check empty
    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    // TODO: do we need to get a single record?

    // Iterator support
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    // Iterator next
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyObject> {
        None
    }

    fn __repr__(&self) -> String {
        format!("ScanResult(rows={})", self.records.len())
    }
}

impl ScanResult {
    pub fn new(
        scanner: Arc<fcore::client::LogScanner>,
        schema: fcore::metadata::Schema,
        start_timestamp: Option<i64>,
        end_timestamp: Option<i64>,
    ) -> Self {
        Self {
            scanner,
            table_schema: schema,
            start_timestamp,
            end_timestamp,
            cached_records: None,
        }
    }

    async fn fetch_all_records_async(
        scanner: Arc<fcore::client::LogScanner>,
        end_ts: Option<i64>,
    ) -> Result<HashMap<fcore::metadata::TableBucket, Vec<fcore::record::ScanRecord>>, FlussError> {
        let mut all_records: HashMap<fluss::metadata::TableBucket, Vec<fcore::record::ScanRecord>> = HashMap::new();
        let timeout = Duration::from_millis(1000);
        
        match end_ts {
            Some(_ts) => {
                loop {
                    let scan_records = scanner.poll(timeout).await?;

                    if scan_records.is_empty() {
                        break;
                    }

                    let mut reached_end = false;
                    for (bucket, records) in scan_records.into_records().into_iter() {
                        let mut filtered_records = Vec::new();

                        for record in records {
                            if record.timestamp() <= _ts {
                                filtered_records.push(record);
                            } else {
                                reached_end = true;
                                break;
                            }
                        }

                        if !filtered_records.is_empty() {
                            all_records.entry(bucket)
                                .or_insert_with(Vec::new)
                                .extend(filtered_records);
                        }

                        if reached_end {
                            break;
                        }
                    }
                }
            },
            None => {
                // TODO: implement unbounded scan
                return Err(FlussError {
                    message: "Unbounded scans are not yet implemented".to_string(),
                    error_code: None,
                });
            }
        }

        Ok(all_records)
    }

    fn records_to_arrow_table(
        py: Python, 
        records: &HashMap<fcore::metadata::TableBucket, Vec<fcore::record::ScanRecord>>,
        schema: &fcore::metadata::Schema,
    ) -> PyResult<PyObject> {
        // 展平所有记录
        let mut all_records = Vec::new();
        for record_vec in records.values() {
            all_records.extend(record_vec.iter());
        }

        if all_records.is_empty() {
            // 返回空的 PyArrow Table
            let pyarrow = py.import("pyarrow")?;
            let empty_schema = Self::create_pyarrow_schema(py, schema)?;
            let empty_table = pyarrow.call_method1("table", (Vec::<PyObject>::new(), empty_schema))?;
            return Ok(empty_table.into());
        }

        let python_records = Self::scan_records_to_python_data(py, &all_records, schema)?;
        
        // 创建 PyArrow Table
        let pyarrow = py.import("pyarrow")?;
        let table = pyarrow.call_method1("table", (python_records,))?;
        Ok(table.into())
    }

    /// 创建 PyArrow Schema
    fn create_pyarrow_schema(py: Python, schema: &fcore::metadata::Schema) -> PyResult<PyObject> {
        let pyarrow = py.import("pyarrow")?;
        
        let mut fields = Vec::new();
        
        // 数据列
        for column in schema.columns() {
            let field_args = (
                column.name(),
                Self::fluss_type_to_pyarrow_type(py, column.data_type())?,
                column.data_type().is_nullable(),
            );
            let field = pyarrow.call_method1("field", field_args)?;
            fields.push(field);
        }
        
        // 元数据列
        let int64_type = pyarrow.call_method0("int64")?;
        let string_type = pyarrow.call_method0("string")?;
        
        fields.push(pyarrow.call_method1("field", ("__offset__", &int64_type, false))?);
        fields.push(pyarrow.call_method1("field", ("__timestamp__", &int64_type, false))?);
        fields.push(pyarrow.call_method1("field", ("__change_type__", &string_type, false))?);
        
        let schema = pyarrow.call_method1("schema", (fields,))?;
        Ok(schema.into())
    }

    /// 转换 Fluss 类型为 PyArrow 类型
    fn fluss_type_to_pyarrow_type(py: Python, data_type: &fcore::metadata::DataType) -> PyResult<PyObject> {
        let pyarrow = py.import("pyarrow")?;
        
        let arrow_type = match data_type {
            fcore::metadata::DataType::Boolean(_) => pyarrow.call_method0("bool_")?,
            fcore::metadata::DataType::TinyInt(_) => pyarrow.call_method0("int8")?,
            fcore::metadata::DataType::SmallInt(_) => pyarrow.call_method0("int16")?,
            fcore::metadata::DataType::Int(_) => pyarrow.call_method0("int32")?,
            fcore::metadata::DataType::BigInt(_) => pyarrow.call_method0("int64")?,
            fcore::metadata::DataType::Float(_) => pyarrow.call_method0("float32")?,
            fcore::metadata::DataType::Double(_) => pyarrow.call_method0("float64")?,
            fcore::metadata::DataType::String(_) => pyarrow.call_method0("string")?,
            fcore::metadata::DataType::Bytes(_) => pyarrow.call_method0("binary")?,
            _ => pyarrow.call_method0("string")?, // 默认字符串类型
        };
        
        Ok(arrow_type.into())
    }

    fn scan_records_to_python_data(
        py: Python,
        records: &[&fcore::record::ScanRecord],
        schema: &fcore::metadata::Schema,
    ) -> PyResult<Vec<PyObject>> {
        let mut python_records = Vec::new();
        
        for record in records {
            let mut record_dict = std::collections::HashMap::new();
            let row = record.row();
            
            // 数据列
            for (idx, column) in schema.columns().iter().enumerate() {
                let value = if row.is_null_at(idx) {
                    py.None()
                } else {
                    Self::extract_column_value(py, row, idx, column.data_type())?
                };
                record_dict.insert(column.name().to_string(), value);
            }
            
            // 元数据列
            record_dict.insert("__offset__".to_string(), record.offset().into_py(py));
            record_dict.insert("__timestamp__".to_string(), record.timestamp().into_py(py));
            record_dict.insert("__change_type__".to_string(), record.change_type().to_string().into_py(py));
            
            python_records.push(record_dict.into_py(py));
        }
        
        Ok(python_records)
    }

    /// 提取列值
    fn extract_column_value(
        py: Python,
        row: &fcore::row::ColumnarRow,
        idx: usize,
        data_type: &fcore::metadata::DataType,
    ) -> PyResult<PyObject> {
        
        match data_type {
            fcore::metadata::DataType::Int(_) => Ok(row.get_int(idx).into_py(py)),
            fcore::metadata::DataType::BigInt(_) => Ok(row.get_long(idx).into_py(py)),
            fcore::metadata::DataType::Float(_) => Ok(row.get_float(idx).into_py(py)),
            fcore::metadata::DataType::Double(_) => Ok(row.get_double(idx).into_py(py)),
            fcore::metadata::DataType::Boolean(_) => Ok(row.get_boolean(idx).into_py(py)),
            fcore::metadata::DataType::String(_) => Ok(row.get_string(idx).into_py(py)),
            fcore::metadata::DataType::Bytes(_) => Ok(row.get_bytes(idx).into_py(py)),
            _ => Ok(format!("Unsupported: {:?}", data_type).into_py(py)),
        }
    }
}
