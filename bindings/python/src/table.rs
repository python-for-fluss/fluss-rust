use pyo3::prelude::*;
use crate::*;
use std::{future, sync::Arc};
use pyo3_async_runtimes::tokio::future_into_py;

// Represents a Fluss table for data operations
#[pyclass]
pub struct FlussTable {
    connection: Arc<fcore::client::FlussConnection>,
    metadata: Arc<fcore::client::Metadata>,
    table_info: fcore::metadata::TableInfo,
    table_path: fcore::metadata::TablePath,
    has_primary_key: bool,
}

#[pymethods]
impl FlussTable {
    // Create a new append writer for the table
    fn new_append_writer<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.connection.clone();
        let metadata = self.metadata.clone();
        let table_info = self.table_info.clone();
        
        future_into_py(py, async move {
            let fluss_table = fcore::client::FlussTable::new(
                &conn,
                metadata,
                table_info,
            );

            let table_append = fluss_table.new_append()
                .map_err(|e| FlussError::new_err(e.to_string()))?;

            let rust_writer = table_append.create_writer();

            let py_writer = AppendWriter::from_core(rust_writer);

            Python::with_gil(|py| {
                Py::new(py, py_writer)
            })
        })
    }

    // Create a new log scanner for the table
    fn new_log_scanner<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.connection.clone();
        let metadata = self.metadata.clone();
        let table_info = self.table_info.clone();

        future_into_py(py, async move {
            let fluss_table = fcore::client::FlussTable::new(
                &conn,
                metadata,
                table_info,
            );

            let table_scan = fluss_table.new_scan();

            let rust_scanner = table_scan.create_log_scanner();

            let py_scanner = LogScanner::from_core(rust_scanner);

            Python::with_gil(|py| {
                Py::new(py, py_scanner)
            })
        })
    }

    // Get table information
    pub fn get_table_info(&self) -> PyResult<TableInfo> {
        Ok(TableInfo::from_core(self.table_info.clone()))
    }

    // Get table path
    pub fn get_table_path(&self) -> PyResult<TablePath> {
        Ok(TablePath::from_core(self.table_path.clone()))
    }

    // has primary key
    pub fn has_primary_key(&self) -> bool {
        self.has_primary_key
    }

    fn __repr__(&self) -> String {
        format!("FlussTable(path={}.{})", 
                self.table_path.database(), 
                self.table_path.table())
    }
}

impl FlussTable {
    // Create a FlussTable
    pub fn new_table(
        connection: Arc<fcore::client::FlussConnection>,
        metadata: Arc<fcore::client::Metadata>,
        table_info: fcore::metadata::TableInfo,
        table_path: fcore::metadata::TablePath,
        has_primary_key: bool,
    ) -> Self {
        Self {
            connection,
            metadata,
            table_info,
            table_path,
            has_primary_key,
        }
    }
}

// Writer for appending data to a Fluss table
#[pyclass]
pub struct AppendWriter {
    inner: fcore::client::AppendWriter,
}

#[pymethods]
impl AppendWriter {
    // Write Arrow table data
    pub fn write_arrow(&mut self, batch: PyObject) -> PyResult<()> {
        // TODO: Implement Arrow batch conversion
        println!("Writing Arrow Table data");
        Ok(())
    }

    // Write Arrow batch data
    pub fn write_arrow_batch(&mut self, batch: PyObject) -> PyResult<()> {
        // TODO: Implement Arrow batch conversion
        println!("Writing Arrow batch data");
        Ok(())
    }

    // Write Pandas DataFrame data
    pub fn write_pandas(&mut self, df: PyObject) -> PyResult<()> {
        // TODO: Implement Pandas DataFrame conversion
        println!("Writing Pandas DataFrame data");
        Ok(())
    }

    // Close the writer and flush any pending data
    pub fn close(&mut self) -> PyResult<()> {
        // TODO: Implement actual writer closing
        println!("Closing TableWriter");
        Ok(())
    }

    fn __repr__(&self) -> String {
        "TableWriter()".to_string()
    }
}

impl AppendWriter {
    // Create a TableWriter from a core append writer
    pub fn from_core(append: fcore::client::AppendWriter) -> Self {
        Self {
            inner: append,
        }
    }
}

// Scanner for reading log data from a Fluss table
#[pyclass]
pub struct LogScanner {
    inner: Arc<fcore::client::LogScanner>,
    table_info: fcore::metadata::TableInfo,
    table_schema: fcore::metadata::Schema,
}

#[pymethods]
impl LogScanner {
    fn scan<'py>(
        &self,
        py: Python<'py>,
        start_timestamp: Option<i64>,
        end_timestamp: Option<i64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let scanner = self.inner.clone();
        let table_info = self.table_info.clone();
        let table_schema = self.table_schema.clone();

        future_into_py(py, async move {
            let num_buckets = table_info.get_num_buckets();

            for bucket_id in 0..num_buckets {
                let start_offset = match start_timestamp {
                    Some(_ts) => {
                        // TODO: implement listOffset in Rust client.
                        0
                    },
                    None => 0, // earliest
                };

                scanner.subscribe(bucket_id, start_offset).await
                    .map_err(|e| FlussError::new_err(e.to_string()))?;
            }

            let scan_result = ScanResult::new(
                scanner,
                table_schema,
                start_timestamp,
                end_timestamp,
            );

            Python::with_gil(|py| {
                Py::new(py, scan_result)
            })
        })
    }

    fn __repr__(&self) -> String {
        format!("LogScanner(table={})", self.table_info.table_path)
    }
}

impl LogScanner {
    // Create a LogScanner from a core scan
    pub fn from_core(scanner: fcore::client::LogScanner, table_info: fcore::metadata::TableInfo) -> Self {
        let schema = table_info.get_schema().clone();
        Self {
            inner: Arc::new(scanner),
            table_info,
            table_schema: schema,
        }
    }
}
