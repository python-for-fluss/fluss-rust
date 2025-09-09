use pyo3::prelude::*;
use crate::*;
use pyo3::types::PyDict;
use std::collections::HashMap;

/// Represents a table path with database and table name
#[pyclass]
#[derive(Clone)]
pub struct TablePath {
    database_name: String,
    table_name: String,
}

#[pymethods]
impl TablePath {
    /// Create a new TablePath
    #[new]
    pub fn new(database_name: String, table_name: String) -> Self {
        Self {
            database_name,
            table_name,
        }
    }
    
    /// Get the database name
    #[getter]
    pub fn database_name(&self) -> String {
        self.database_name.clone()
    }
    
    /// Get the table name  
    #[getter]
    pub fn table_name(&self) -> String {
        self.table_name.clone()
    }

    /// Get table path as string
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
    /// Convert to core TablePath
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

/// Schema wrapper for Fluss table schema
#[pyclass]
pub struct Schema {
    __schema: fcore::metadata::Schema,
}

#[pymethods]
impl Schema {
    /// Create a new Schema from PyArrow schema with optional primary keys
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

    /// Get column names
    fn get_column_names(&self) -> Vec<String> {
        self.__schema.columns().iter().map(|col| col.name().to_string()).collect()
    }

    /// Get column types
    fn get_column_types(&self) -> Vec<String> {
        self.__schema.columns().iter()
            .map(|col| Utils::datatype_to_string(col.data_type()))
            .collect()
    }

    /// Get columns as (name, type) pairs
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
    /// Convert to core Schema
    pub fn to_core(&self) -> &fcore::metadata::Schema {
        &self.__schema
    }
}

/// Table distribution configuration
#[pyclass]
pub struct TableDistribution {
    inner: fcore::metadata::TableDistribution,
}

#[pymethods]
impl TableDistribution {
    /// Get bucket keys
    fn bucket_keys(&self) -> Vec<String> {
        self.inner.bucket_keys().to_vec()
    }

    /// Get bucket count
    fn bucket_count(&self) -> Option<i32> {
        self.inner.bucket_count()
    }
}


/// Table descriptor containing schema and metadata
#[pyclass]
#[derive(Clone)]
pub struct TableDescriptor {
    __tbl_desc: fcore::metadata::TableDescriptor,
}

#[pymethods]
impl TableDescriptor {
    /// Create a new TableDescriptor
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

    /// Get the schema of this table descriptor
    pub fn get_schema(&self) -> PyResult<Schema> {
        Ok(Schema {
            __schema: self.__tbl_desc.schema().clone(),
        })
    }
}

impl TableDescriptor {
    /// Convert to core TableDescriptor
    pub fn to_core(&self) -> &fcore::metadata::TableDescriptor {
        &self.__tbl_desc
    }
}

/// Information about a Fluss table
#[pyclass]
#[derive(Clone)]
pub struct TableInfo {
    __table_info: fcore::metadata::TableInfo,
}

#[pymethods]
impl TableInfo {
    /// Get the table ID
    #[getter]
    pub fn table_id(&self) -> i64 {
        self.__table_info.get_table_id()
    }
    
    /// Get the schema ID
    #[getter]
    pub fn schema_id(&self) -> i32 {
        self.__table_info.get_schema_id()
    }
    
    /// Get the table path
    #[getter]
    pub fn table_path(&self) -> TablePath {
        TablePath::from_core(self.__table_info.get_table_path().clone())
    }

    /// Get the created time
    #[getter]
    pub fn created_time(&self) -> i64 {
        self.__table_info.get_created_time()
    }
    
    /// Get the modified time
    #[getter]
    pub fn modified_time(&self) -> i64 {
        self.__table_info.get_modified_time()
    }
    
    /// Get the primary keys
    pub fn get_primary_keys(&self) -> Vec<String> {
        self.__table_info.get_primary_keys().clone()
    }

    /// Get the bucket keys
    pub fn get_bucket_keys(&self) -> Vec<String> {
        self.__table_info.get_bucket_keys().to_vec()
    }

    /// Get the partition keys
    pub fn get_partition_keys(&self) -> Vec<String> {
        self.__table_info.get_partition_keys().to_vec()
    }

    /// Get number of buckets
    #[getter]
    pub fn num_buckets(&self) -> i32 {
        self.__table_info.get_num_buckets()
    }

    /// Check if table has primary key
    pub fn has_primary_key(&self) -> bool {
        self.__table_info.has_primary_key()
    }

    /// Check if table is partitioned
    pub fn is_partitioned(&self) -> bool {
        self.__table_info.is_partitioned()
    }

    /// Get properties
    pub fn get_properties(&self) -> std::collections::HashMap<String, String> {
        self.__table_info.get_properties().clone()
    }

    /// Get custom properties
    pub fn get_custom_properties(&self) -> std::collections::HashMap<String, String> {
        self.__table_info.get_custom_properties().clone()
    }

    /// Get comment
    #[getter]
    pub fn comment(&self) -> Option<String> {
        self.__table_info.get_comment().map(|s| s.to_string())
    }

    /// Get the Schema
    pub fn get_schema(&self) -> Schema {
        Schema {
            __schema: self.__table_info.get_schema().clone(),
        }
    }

    /// Get column names
    pub fn get_column_names(&self) -> Vec<String> {
        self.__table_info.get_schema().columns().iter()
            .map(|col| col.name().to_string())
            .collect()
    }

    /// Get column count
    pub fn get_column_count(&self) -> usize {
        self.__table_info.get_schema().columns().len()
    }
}

impl TableInfo {
    /// Create from core TableInfo (internal use)
    pub fn from_core(info: fcore::metadata::TableInfo) -> Self {
        Self {
            __table_info: info,
        }
    }
}

/// Represents a lake snapshot with snapshot ID and table bucket offsets
#[pyclass]
#[derive(Clone)]
pub struct LakeSnapshot {
    snapshot_id: i64,
    table_buckets_offset: HashMap<fcore::metadata::TableBucket, i64>,
}

/// Represents a table bucket with table ID, partition ID, and bucket ID
#[pyclass]
#[derive(Clone)]
pub struct TableBucket {
    table_id: i64,
    partition_id: Option<i64>,
    bucket: i32,
}

#[pymethods]
impl TableBucket {
    /// Create a new TableBucket
    #[new]
    pub fn new(table_id: i64, bucket: i32) -> Self {
        Self {
            table_id,
            partition_id: None,
            bucket,
        }
    }

    /// Create a new TableBucket with partition
    #[staticmethod]
    pub fn with_partition(table_id: i64, partition_id: i64, bucket: i32) -> Self {
        Self {
            table_id,
            partition_id: Some(partition_id),
            bucket,
        }
    }

    /// Get table ID
    #[getter]
    pub fn table_id(&self) -> i64 {
        self.table_id
    }

    /// Get bucket ID
    #[getter]
    pub fn bucket_id(&self) -> i32 {
        self.bucket
    }

    /// Get partition ID
    #[getter]
    pub fn partition_id(&self) -> Option<i64> {
        self.partition_id
    }

    /// String representation
    pub fn __str__(&self) -> String {
        if let Some(partition_id) = self.partition_id {
            format!("TableBucket(table_id={}, partition_id={}, bucket={})", 
                    self.table_id, partition_id, self.bucket)
        } else {
            format!("TableBucket(table_id={}, bucket={})", 
                    self.table_id, self.bucket)
        }
    }

    /// String representation
    pub fn __repr__(&self) -> String {
        self.__str__()
    }

    /// Hash implementation for Python
    pub fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        self.table_id.hash(&mut hasher);
        self.partition_id.hash(&mut hasher);
        self.bucket.hash(&mut hasher);
        hasher.finish()
    }

    /// Equality implementation for Python
    pub fn __eq__(&self, other: &TableBucket) -> bool {
        self.table_id == other.table_id 
            && self.partition_id == other.partition_id 
            && self.bucket == other.bucket
    }
}

impl TableBucket {
    /// Create from core TableBucket (internal use)
    pub fn from_core(bucket: fcore::metadata::TableBucket) -> Self {
        Self {
            table_id: bucket.table_id(),
            partition_id: bucket.partition_id(),
            bucket: bucket.bucket_id(),
        }
    }

    /// Convert to core TableBucket (internal use)
    pub fn to_core(&self) -> fcore::metadata::TableBucket {
        fcore::metadata::TableBucket::new(self.table_id, self.bucket)
    }
}

#[pymethods]
impl LakeSnapshot {
    /// Create a new LakeSnapshot
    #[new]
    pub fn new(snapshot_id: i64) -> Self {
        Self {
            snapshot_id,
            table_buckets_offset: HashMap::new(),
        }
    }

    /// Get snapshot ID
    #[getter]
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Get table bucket offsets as a Python dictionary with TableBucket keys
    #[getter]
    pub fn table_buckets_offset(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        for (bucket, offset) in &self.table_buckets_offset {
            let py_bucket = TableBucket::from_core(bucket.clone());
            dict.set_item(Py::new(py, py_bucket)?, *offset)?;
        }
        Ok(dict.into())
    }

    /// Get offset for a specific table bucket
    pub fn get_bucket_offset(&self, bucket: &TableBucket) -> Option<i64> {
        let core_bucket = bucket.to_core();
        self.table_buckets_offset.get(&core_bucket).copied()
    }

    /// Get all table buckets
    pub fn get_table_buckets(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let mut buckets = Vec::new();
        for bucket in self.table_buckets_offset.keys() {
            let py_bucket = TableBucket::from_core(bucket.clone());
            buckets.push(Py::new(py, py_bucket)?.into());
        }
        Ok(buckets)
    }

    /// String representation
    pub fn __str__(&self) -> String {
        format!("LakeSnapshot(snapshot_id={}, buckets_count={})", 
                self.snapshot_id, self.table_buckets_offset.len())
    }

    /// String representation
    pub fn __repr__(&self) -> String {
        self.__str__()
    }
}

impl LakeSnapshot {
    /// Create from core LakeSnapshot (internal use)
    pub fn from_core(snapshot: fcore::metadata::LakeSnapshot) -> Self {
        Self {
            snapshot_id: snapshot.snapshot_id,
            table_buckets_offset: snapshot.table_buckets_offset,
        }
    }
}

