use pyo3::prelude::*;
use crate::*;
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use std::sync::Arc;

/// Represents a table path in Fluss
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

// Information about a Fluss table
#[pyclass]
#[derive(Clone)]
pub struct TableInfo {
    __table_info: Option<fcore::metadata::TableInfo>,
}

#[pymethods]
impl TableInfo {
    /// Create a new TableInfo (internal use)
    #[new]
    pub fn new(table_id: u64, schema_id: u64, created_time: u64, primary_keys: Vec<String>) -> Self {
        let core_info = fcore::metadata::TableInfo::new(table_id, schema_id, created_time, primary_keys);
    }

    /// Get the table ID
    #[getter]
    pub fn table_id(&self) -> u64 {
        self.table_id
    }
    
    /// Get the schema ID
    #[getter]
    pub fn schema_id(&self) -> u64 {
        self.schema_id
    }
    
    /// Get the created time
    #[getter]
    pub fn created_time(&self) -> u64 {
        self.created_time
    }
    
    /// Get the primary keys
    pub fn get_primary_keys(&self) -> Vec<String> {
        self.primary_keys.clone()
    }
    
    pub fn __str__(&self) -> String {
        format!(
            "TableInfo(table_id={}, schema_id={}, created_time={}, primary_keys={:?})",
            self.table_id(), self.schema_id(), self.created_time(), self.get_primary_keys()
        )
    }
}

impl TableInfo {
    /// Create from core TableInfo (internal use)
    pub fn from_core(info: fcore::metadata::TableInfo) -> Self {
        Self {
            __table_info: Some(info),
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct TableDescriptor {
    __tbl_desc: Option<fcore::metadata::TableDescriptor>,
}

#[pymethods]
impl TableDescriptor {
    /// Create a new TableDescriptor
    #[new]
    #[pyo3(signature = (schema, primary_keys=None, properties=None))]
    pub fn new(
        schema: PyObject,  // PyArrow schema
        primary_keys: Option<Vec<String>>,
        properties: Option<std::collections::HashMap<String, String>>,
    ) -> PyResult<Self> {
        let arrow_schema = Self::pyarrow::from_pyarrow_schema(schema)?;
        // TODO: 从 PyArrow schema 创建 fcore::metadata::TableDescriptor
        // let core_descriptor = fcore::metadata::TableDescriptor::from_schema(...)?;
        let desc_builder = fcore::metadata::TableDescriptorBuilder::new();
        let core_descriptor = desc_builder
            .schema(schema)
            .with_primary_keys(primary_keys.unwrap_or_default())
            .with_properties(properties.unwrap_or_default())
            .build()?;
        let core_descriptor = fcore::metadata::TableDescriptor::new(
            schema, 
            primary_keys.unwrap_or_default(), 
            properties.unwrap_or_default()
        );
        
    }

    /// Get primary keys from the descriptor
    pub fn get_primary_keys(&self) -> PyResult<Vec<String>> {
        if let Some(ref desc) = self.__tbl_desc {
            // TODO: 从 core descriptor 获取主键
            // Ok(desc.primary_keys())
            Ok(vec!["id".to_string()]) // 临时返回
        } else {
            Ok(vec![])
        }
    }

    /// Get table properties
    pub fn get_properties(&self) -> PyResult<std::collections::HashMap<String, String>> {
        if let Some(ref desc) = self.__tbl_desc {
            // TODO: 从 core descriptor 获取属性
            // Ok(desc.properties())
            Ok(std::collections::HashMap::new()) // 临时返回
        } else {
            Ok(std::collections::HashMap::new())
        }
    }

    /// Get schema information
    pub fn get_schema(&self) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            if let Some(ref desc) = self.__tbl_desc {
                // TODO: 将 core schema 转换为 PyArrow schema
                // let arrow_schema = desc.to_arrow_schema()?;
                // Ok(arrow_schema.to_object(py))
                Ok(py.None()) // 临时返回
            } else {
                Ok(py.None())
            }
        })
    }

    /// Convert to string representation
    pub fn __str__(&self) -> String {
        if let Some(ref desc) = self.__tbl_desc {
            // TODO: 从 core descriptor 获取信息
            format!("TableDescriptor(...)")
        } else {
            "TableDescriptor(empty)".to_string()
        }
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }
}

impl TableDescriptor {
    /// Convert PyArrow schema to Rust Arrow schema
    fn pyarrow_to_arrow_schema(py_schema: &PyObject) -> PyResult<SchemaRef> {
        Python::with_gil(|py| {
            let schema_obj = py_schema.as_ref(py);
            use arrow_pyarrow::PyArrowType;
            let schema: ArrowSchema = schema_obj.extract()?;
            Ok(Arc::new(schema))
        })
    }

    /// Create from core TableDescriptor (internal use)
    pub fn from_core(desc: fcore::metadata::TableDescriptor) -> Self {
        Self {
            __tbl_desc: Some(desc),
        }
    }

    /// Get the internal core descriptor
    pub fn core_descriptor(&self) -> Option<&fcore::metadata::TableDescriptor> {
        self.__tbl_desc.as_ref()
    }

    /// Take the internal core descriptor
    pub fn take_core_descriptor(self) -> Option<fcore::metadata::TableDescriptor> {
        self.__tbl_desc
    }
}

// Result of a table scan operation
#[pyclass]
pub struct ScanResult {
    // TODO: Store actual scan results
    current_index: usize,
    records: Vec<fcore::record::ScanRecord>, // Sample data for testing
}

#[pymethods]
impl ScanResult {
    #[new]
    pub fn new() -> Self {
        Self {
            current_index: 0,
            records: vec![], // Initialize with empty data
        }
    }

    // Convert to PyArrow Table
    pub fn to_arrow(&self) -> PyResult<PyObject> {
        // TODO: Implement actual conversion to PyArrow Table
        Python::with_gil(|py| {
            println!("Converting to PyArrow Table");
            Ok(py.None())
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
        if slf.current_index < slf.records.len() {
            let current_index = slf.current_index;
            slf.current_index += 1;
            let item = &slf.records[current_index];
            
            Python::with_gil(|py| {
                let dict = pyo3::types::PyDict::new_bound(py);
                dict.set_item("id", item.0).ok()?;
                dict.set_item("name", &item.1).ok()?;
                dict.set_item("score", item.2).ok()?;
                Some(dict.into())
            })
        } else {
            None
        }
    }

    fn __repr__(&self) -> String {
        format!("ScanResult(rows={})", self.data.len())
    }
}
