use pyo3::prelude::*;
use crate::*;
use std::sync::Arc;
use pyo3_async_runtimes::tokio::future_into_py;
use crate::TOKIO_RUNTIME;

/// Represents a Fluss table for data operations
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
    /// Create a new append writer for the table
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

    /// Create a new log scanner for the table
    /// Note: LogScanner is not Send, so this may cause issues in async contexts
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
            );

            Python::with_gil(|py| {
                Py::new(py, py_scanner)
            })
        })
    }

    // current workaround - synchronous version of new_log_scanner
    fn new_log_scanner_sync(&self) -> PyResult<LogScanner> {
        let conn = self.connection.clone();
        let metadata = self.metadata.clone();
        let table_info = self.table_info.clone();

        let rust_scanner = TOKIO_RUNTIME.block_on(async {
            let fluss_table = fcore::client::FlussTable::new(
                &conn,
                metadata.clone(),
                table_info.clone(),
            );

            let table_scan = fluss_table.new_scan();
            table_scan.create_log_scanner()
        });

        let py_scanner = LogScanner::from_core(
            rust_scanner,
            table_info.clone(),
        );

        Ok(py_scanner)
    }

    /// Get table information
    pub fn get_table_info(&self) -> PyResult<TableInfo> {
        Ok(TableInfo::from_core(self.table_info.clone()))
    }

    /// Get table path
    pub fn get_table_path(&self) -> PyResult<TablePath> {
        Ok(TablePath::from_core(self.table_path.clone()))
    }

    /// Check if table has primary key
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
    /// Create a FlussTable
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

/// Writer for appending data to a Fluss table
#[pyclass]
pub struct AppendWriter {
    inner: fcore::client::AppendWriter,
}

#[pymethods]
impl AppendWriter {
    /// Write Arrow table data
    pub fn write_arrow(&mut self, py: Python, table: PyObject) -> PyResult<()> {
        // Convert Arrow Table to batches and write each batch
        let batches = table.call_method0(py, "to_batches")?;
        let batch_list: Vec<PyObject> = batches.extract(py)?;
        
        for batch in batch_list {
            self.write_arrow_batch(py, batch)?;
        }
        Ok(())
    }

    /// Write Arrow batch data
    pub fn write_arrow_batch(&mut self, py: Python, batch: PyObject) -> PyResult<()> {
        // Extract number of rows and columns from the Arrow batch
        let num_rows: usize = batch.getattr(py, "num_rows")?.extract(py)?;
        let num_columns: usize = batch.getattr(py, "num_columns")?.extract(py)?;
        
        // Process each row in the batch
        for row_idx in 0..num_rows {
            let mut generic_row = fcore::row::GenericRow::new();
            
            // Extract values for each column in this row
            for col_idx in 0..num_columns {
                let column = batch.call_method1(py, "column", (col_idx,))?;
                let value = column.call_method1(py, "__getitem__", (row_idx,))?;
                
                // Convert the Python value to a Datum and add to the row
                let datum = self.convert_python_value_to_datum(py, value)?;
                generic_row.set_field(col_idx, datum);
            }
            
            // Append this row using the async append method
            TOKIO_RUNTIME.block_on(async {
                self.inner.append(generic_row).await
                    .map_err(|e| FlussError::new_err(e.to_string()))
            })?;
        }
        
        Ok(())
    }

    /// Write Pandas DataFrame data
    pub fn write_pandas(&mut self, py: Python, df: PyObject) -> PyResult<()> {
        // Import pyarrow module
        let pyarrow = py.import("pyarrow")?;
        
        // Get the Table class from pyarrow module
        let table_class = pyarrow.getattr("Table")?;
        
        // Call Table.from_pandas(df) - from_pandas is a class method
        let pa_table = table_class.call_method1("from_pandas", (df,))?;
        
        // Then call write_arrow with the converted table
        self.write_arrow(py, pa_table.into_py(py))
    }

    /// Append a single row from a Python dictionary
    pub fn append_row(&mut self, py: Python, row_dict: PyObject) -> PyResult<()> {
        let mut generic_row = fcore::row::GenericRow::new();
        
        // Extract dictionary items as (key, value) pairs
        let dict_ref = row_dict.bind(py);
        let items = dict_ref.call_method0("items")?;
        
        let mut field_idx = 0;
        for item in items.try_iter()? {
            let (_, value) = item?.extract::<(String, PyObject)>()?;
            let datum = self.convert_python_value_to_datum(py, value)?;
            generic_row.set_field(field_idx, datum);
            field_idx += 1;
        }
        
        // Append the row
        TOKIO_RUNTIME.block_on(async {
            self.inner.append(generic_row).await
                .map_err(|e| FlussError::new_err(e.to_string()))
        })
    }

    /// Flush any pending data
    pub fn flush(&mut self) -> PyResult<()> {
        TOKIO_RUNTIME.block_on(async {
            self.inner.flush().await
                .map_err(|e| FlussError::new_err(e.to_string()))
        })
    }

    /// Close the writer and flush any pending data
    pub fn close(&mut self) -> PyResult<()> {
        self.flush()?;
        println!("AppendWriter closed");
        Ok(())
    }

    fn __repr__(&self) -> String {
        "AppendWriter()".to_string()
    }
}

impl AppendWriter {
    /// Create a TableWriter from a core append writer
    pub fn from_core(append: fcore::client::AppendWriter) -> Self {
        Self {
            inner: append,
        }
    }

    // Convert Python value to Datum
    fn convert_python_value_to_datum(&self, py: Python, value: PyObject) -> PyResult<fcore::row::Datum<'static>> {
        use fcore::row::{Datum, F32, F64};
        
        // First try to extract scalar values from Arrow types
        let obj_ref = value.bind(py);
        let type_name = obj_ref.get_type().name()?;
        
        // Handle Arrow scalar types
        let type_name_str = type_name.to_str().unwrap_or("");
        if type_name_str.contains("Scalar") {
            // Try to get the Python value from Arrow scalar
            if let Ok(py_value) = obj_ref.call_method0("as_py") {
                // Recursively convert the extracted Python value
                return self.convert_python_value_to_datum(py, py_value.to_object(py));
            }
        }
        
        // Check for None (null)
        if value.is_none(py) {
            return Ok(Datum::Null);
        }
        
        // Try to extract different types
        if let Ok(bool_val) = value.extract::<bool>(py) {
            return Ok(Datum::Bool(bool_val));
        }
        
        if let Ok(int_val) = value.extract::<i32>(py) {
            return Ok(Datum::Int32(int_val));
        }
        
        if let Ok(int_val) = value.extract::<i64>(py) {
            return Ok(Datum::Int64(int_val));
        }
        
        if let Ok(float_val) = value.extract::<f32>(py) {
            return Ok(Datum::Float32(F32::from(float_val)));
        }
        
        if let Ok(float_val) = value.extract::<f64>(py) {
            return Ok(Datum::Float64(F64::from(float_val)));
        }
        
        if let Ok(str_val) = value.extract::<String>(py) {
            // Convert String to &'static str by leaking memory
            // This is a simplified approach - in production, you might want better lifetime management
            let leaked_str: &'static str = Box::leak(str_val.into_boxed_str());
            return Ok(Datum::String(leaked_str));
        }
        
        // If we can't convert, return an error
        Err(FlussError::new_err(format!(
            "Cannot convert Python value to Datum: {:?}", 
            type_name
        )))
    }
}

/// Scanner for reading log data from a Fluss table
#[pyclass(unsendable)]
pub struct LogScanner {
    inner: fcore::client::LogScanner,
    table_info: fcore::metadata::TableInfo,
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
}

#[pymethods]
impl LogScanner {
    /// Subscribe to log data with timestamp range
    fn subscribe(
        &mut self,
        _start_timestamp: Option<i64>,
        _end_timestamp: Option<i64>,
    ) -> PyResult<()> {
        // Handle end_timestamp intelligently
        let end_timestamp = match _end_timestamp {
            Some(ts) => ts,
            None => {
                return Err(FlussError::new_err(
                    "end_timestamp must be specified for LogScanner".to_string()));
            }
        };
    
        self.start_timestamp = _start_timestamp;
        self.end_timestamp = Some(end_timestamp);

        let num_buckets = self.table_info.get_num_buckets();
        let start_timestamp = self.start_timestamp; 

        for bucket_id in 0..num_buckets {
            let start_offset = match start_timestamp {
                Some(_ts) => {
                    // TODO: implement timestampIntoOffset in Rust client.
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
        let end_timestamp = self.end_timestamp;
        // Poll all data from the scanner
        loop {
            let batch_result = TOKIO_RUNTIME.block_on(async {
                use std::time::Duration;
                self.inner.poll(Duration::from_millis(1000)).await
            });
            
            match batch_result {
                Ok(scan_records) => {
                    let records_map = scan_records.into_records();
                    
                    let mut total_records = 0;
                    for (bucket, records) in &records_map {
                        total_records += records.len();
                    }

                    if total_records == 0 {
                        break; // No more data
                    }

                    let scan_records = fcore::record::ScanRecords::new(records_map);

                    // Filter records by end_timestamp if specified
                    let (filtered_records, reached_end) = if let Some(end_ts) = end_timestamp {
                        Self::filter_records_by_timestamp(scan_records, end_ts)
                    } else {
                        (scan_records, false)
                    };
                    
                    let filtered_records_map = filtered_records.into_records();
                    let mut filtered_total_records = 0;
                    for (_bucket, records) in &filtered_records_map {
                        filtered_total_records += records.len();
                    }
                    
                    let filtered_records = fcore::record::ScanRecords::new(filtered_records_map);
                    
                    if !filtered_records.is_empty() {
                        // Convert ScanRecords to Arrow RecordBatch
                        let arrow_batch = Utils::convert_scan_records_to_arrow(filtered_records);
                        all_batches.extend(arrow_batch);
                    }
                    
                    // If we reached the end timestamp, stop polling
                    if reached_end {
                        break;
                    }
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
    /// TODO: Support this for streaming reads
    fn to_arrow_batch_reader(&mut self, py: Python) -> PyResult<()> {
        // Create a streaming iterator that wraps our LogScanner
        
        // let iterator = LogScannerIterator::new(self, py)?;
        
        // // Create Arrow schema for the reader
        // let schema = Utils::create_arrow_schema(py, &self.table_schema)?;
        
        // // Create a Python RecordBatchReader that uses our iterator
        // let pyarrow = py.import("pyarrow")?;
        // let reader = pyarrow
        //     .getattr("RecordBatchReader")?
        //     .call_method1("from_batches", (schema, iterator))?;
        
        // Ok(reader.into())
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!("LogScanner(table={})", self.table_info.table_path)
    }
}

impl LogScanner {
    /// Create LogScanner from core LogScanner
    pub fn from_core(
        inner: fcore::client::LogScanner,
        table_info: fcore::metadata::TableInfo,
    ) -> Self {
        Self {
            inner,
            table_info,
            start_timestamp: None,
            end_timestamp: None,
        }
    }

    /// Filter scan records by timestamp, returns (filtered_records, reached_end)
    fn filter_records_by_timestamp(
        scan_records: fcore::record::ScanRecords, 
        end_timestamp: i64
    ) -> (fcore::record::ScanRecords, bool) {
        use std::collections::HashMap;
        
        let records_map = scan_records.into_records();
        let mut filtered_map = HashMap::new();
        let mut reached_end = false;
        
        for (table_bucket, records) in records_map {
            let mut filtered_records = Vec::new();
            
            for record in records {
                // Check if this record's timestamp exceeds end_timestamp
                if record.timestamp() > end_timestamp {
                    reached_end = true;
                    break; // Stop processing records for this bucket
                }
                
                // Include this record since it's within the time range
                filtered_records.push(record);
            }
            
            if !filtered_records.is_empty() {
                filtered_map.insert(table_bucket, filtered_records);
            }
        }
        
        (fcore::record::ScanRecords::new(filtered_map), reached_end)
    }
}

// Iterator for streaming Arrow RecordBatches from LogScanner
#[pyclass]
pub struct LogScannerIterator {
    scanner_ptr: *mut LogScanner,
    finished: bool,
    batch_cache: Vec<PyObject>, // Cache for Arrow batches
}

#[pymethods]
impl LogScannerIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }
    
    // TODO: Implement this for streaming reads
    fn __next__(&mut self, py: Python) -> PyResult<()> {
        Err(FlussError::new_err("Not implemented"))
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
            batch_cache: Vec::new(),
        })
    }

    // TODO: Support this for streaming reads
    fn convert_to_batches() -> PyResult<()> {
        Err(FlussError::new_err("Not implemented"))
    }
}
