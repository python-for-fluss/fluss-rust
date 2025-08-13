use pyo3::prelude::*;
use crate::*;
use std::sync::Arc;
use pyo3_async_runtimes::tokio::future_into_py;
use crate::TOKIO_RUNTIME;

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
                metadata.clone(),
                table_info.clone(),
            );

            let table_scan = fluss_table.new_scan();

            let rust_scanner = table_scan.create_log_scanner();

            let py_scanner = LogScanner::from_core(
                rust_scanner,
                table_info.clone(),
                table_info.schema.clone(),
            );

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
    pub fn write_arrow(&mut self, _batch: PyObject) -> PyResult<()> {
        // TODO: Implement Arrow batch conversion
        println!("Writing Arrow Table data");
        Ok(())
    }

    // Write Arrow batch data
    pub fn write_arrow_batch(&mut self, _batch: PyObject) -> PyResult<()> {
        // TODO: Implement Arrow batch conversion
        println!("Writing Arrow batch data");
        Ok(())
    }

    // Write Pandas DataFrame data
    pub fn write_pandas(&mut self, _df: PyObject) -> PyResult<()> {
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
#[pyclass(unsendable)]
pub struct LogScanner {
    inner: fcore::client::LogScanner,
    table_info: fcore::metadata::TableInfo,
    table_schema: fcore::metadata::Schema,
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
}

#[pymethods]
impl LogScanner {
    fn subscribe(
        &mut self,
        _start_timestamp: Option<i64>,
        _end_timestamp: Option<i64>,
    ) -> PyResult<()> {
        // TODO: support unbounded subscriptions
        let end_timestamp = _end_timestamp
            .ok_or_else(|| FlussError::new_err("Currently we only supports bounded operations. So the 'end_timestamp' must be specified"))?;
    
        self.start_timestamp = _start_timestamp;
        self.end_timestamp = Some(end_timestamp);

        let num_buckets = self.table_info.get_num_buckets();
        let start_timestamp = self.start_timestamp; 

        for bucket_id in 0..num_buckets {
            let start_offset = match start_timestamp {
                Some(_ts) => {
                    // TODO: implement listOffset in Rust client.
                    0
                },
                None => 0, // earliest
            };

            TOKIO_RUNTIME.block_on(async {
                self.inner.subscribe(bucket_id, start_offset).await
                    .map_err(|e| FlussError::new_err(e.to_string()))
            })?;
        }

        Ok(())
    }

    // Convert all data to Arrow Table
    fn to_arrow(&mut self, py: Python) -> PyResult<PyObject> {
        let mut all_batches = Vec::new();
        
        // Poll all data from the scanner
        loop {
            let batch_result = TOKIO_RUNTIME.block_on(async {
                use std::time::Duration;
                self.inner.poll(Duration::from_millis(1000)).await
            });
            
            match batch_result {
                Ok(scan_records) => {
                    if scan_records.is_empty() {
                        break; // No more data
                    }
                    
                    // Convert ScanRecords to Arrow RecordBatch
                    let arrow_batch = Utils::convert_scan_records_to_arrow(py, scan_records, &self.table_schema)?;
                    all_batches.push(arrow_batch);
                },
                Err(e) => return Err(FlussError::new_err(e.to_string())),
            }
        }
        
        if all_batches.is_empty() {
            return Err(FlussError::new_err("No data available"));
        }
        
        // Combine all batches into a single Arrow Table
        Utils::combine_batches_to_table(py, all_batches)
    }

    // Convert all data to Pandas DataFrame
    fn to_pandas(&mut self, py: Python) -> PyResult<PyObject> {
        let arrow_table = self.to_arrow(py)?;
        
        // Convert Arrow Table to Pandas DataFrame using pyarrow
        let df = arrow_table.call_method0(py, "to_pandas")?;
        Ok(df)
    }

    // Return an Arrow RecordBatchReader for streaming data
    fn to_arrow_batch_reader(&mut self, py: Python) -> PyResult<PyObject> {
        // Create a streaming iterator that wraps our LogScanner
        let iterator = LogScannerIterator::new(self, py)?;
        
        // Create Arrow schema for the reader
        let schema = Utils::create_arrow_schema(py, &self.table_schema)?;
        
        // Create a Python RecordBatchReader that uses our iterator
        let pyarrow = py.import("pyarrow")?;
        let reader = pyarrow
            .getattr("RecordBatchReader")?
            .call_method1("from_batches", (schema, iterator))?;
        
        Ok(reader.into())
    }

    fn __repr__(&self) -> String {
        format!("LogScanner(table={})", self.table_info.table_path)
    }
}

impl LogScanner {
    // Create LogScanner from core LogScanner
    pub fn from_core(
        inner: fcore::client::LogScanner,
        table_info: fcore::metadata::TableInfo,
        table_schema: fcore::metadata::Schema,
    ) -> Self {
        Self {
            inner,
            table_info,
            table_schema,
            start_timestamp: None,
            end_timestamp: None,
        }
    }
}

// Iterator for streaming Arrow RecordBatches from LogScanner
#[pyclass]
pub struct LogScannerIterator {
    scanner_ptr: *mut LogScanner,
    finished: bool,
}

#[pymethods]
impl LogScannerIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }
    
    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        if self.finished {
            return Ok(None); // StopIteration
        }
        
        unsafe {
            let scanner = &mut *self.scanner_ptr;
            
            let batch_result = TOKIO_RUNTIME.block_on(async {
                use std::time::Duration;
                scanner.inner.poll(Duration::from_millis(1000)).await
            });
            
            match batch_result {
                Ok(scan_records) => {
                    if scan_records.is_empty() {
                        self.finished = true;
                        Ok(None) // StopIteration
                    } else {
                        let arrow_batch = Utils::convert_scan_records_to_arrow(py, scan_records, &scanner.table_schema)?;
                        Ok(Some(arrow_batch))
                    }
                },
                Err(e) => {
                    self.finished = true;
                    Err(FlussError::new_err(e.to_string()))
                },
            }
        }
    }
}

// Make it unsendable to avoid thread safety issues
unsafe impl Send for LogScannerIterator {}
unsafe impl Sync for LogScannerIterator {}

impl LogScannerIterator {
    fn new(scanner: &mut LogScanner, _py: Python) -> PyResult<Self> {
        Ok(Self {
            scanner_ptr: scanner as *mut LogScanner,
            finished: false,
        })
    }
}
