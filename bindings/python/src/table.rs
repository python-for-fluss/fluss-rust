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
        // Handle end_timestamp intelligently
        let end_timestamp = match _end_timestamp {
            Some(ts) => ts,
            None => {
                // If no end_timestamp provided, use current system time + some buffer
                // This prevents infinite polling when user doesn't specify an end time
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                now + 60000 // Add 1 minute buffer
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
                    if scan_records.is_empty() {
                        break; // No more data
                    }
                    
                    // Filter records by end_timestamp if specified
                    let (filtered_records, reached_end) = if let Some(end_ts) = end_timestamp {
                        Self::filter_records_by_timestamp(scan_records, end_ts)
                    } else {
                        (scan_records, false)
                    };
                    
                    // If no records left after filtering, add batch if we have records
                    if !filtered_records.is_empty() {
                        // Convert ScanRecords to Arrow RecordBatch
                        let arrow_batch = Utils::convert_scan_records_to_arrow(py, filtered_records, &self.table_schema)?;
                        all_batches.push(arrow_batch);
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

    // Get a recommended end_timestamp for scanning
    // This can help users avoid setting end_timestamp too far in the future
    fn get_recommended_end_timestamp(&self) -> PyResult<i64> {
        // Return current system timestamp
        // Users can use this as a reasonable end_timestamp for scanning
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| FlussError::new_err(format!("Failed to get current time: {}", e)))?
            .as_millis() as i64;
        Ok(now)
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

    // Filter scan records by timestamp, returns (filtered_records, reached_end)
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
            
            // Only add bucket if it has records
            if !filtered_records.is_empty() {
                filtered_map.insert(table_bucket, filtered_records);
            }
        }
        
        (fcore::record::ScanRecords::new(filtered_map), reached_end)
    }

    // Check if we've reached the end of available data based on high watermarks
    fn has_reached_end(&self) -> bool {
        // If we have an end_timestamp, check if we've scanned past all available data
        if let Some(end_ts) = self.end_timestamp {
            // Get the maximum timestamp from the log scanner's high watermarks
            let max_timestamp = self.get_max_available_timestamp();
            if max_timestamp > 0 && end_ts <= max_timestamp {
                // end_timestamp is within available data range, continue scanning
                return false;
            }
            // end_timestamp is beyond available data, we can stop
            return true;
        }
        false
    }

    // Get the maximum available timestamp from all buckets' high watermarks
    // This helps determine if we should continue polling or if we've reached the end
    fn get_max_available_timestamp(&self) -> i64 {
        // TODO: This should access the LogScanner's internal high watermark information
        // For now, return 0 to indicate we don't have this information
        // In a complete implementation, we would:
        // 1. Access log_scanner_status from the inner LogScanner
        // 2. Get all bucket statuses and their high watermarks
        // 3. Convert high watermark (offset) to timestamp if needed
        0
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
    
    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        if self.finished {
            return Ok(None); // StopIteration
        }
        
        // First, try to serve from cache
        if !self.batch_cache.is_empty() {
            // Return the first batch and remove it from cache
            let batch = self.batch_cache.remove(0);
            return Ok(Some(batch));
        }
        
        // Cache is empty, need to poll for more data
        unsafe {
            let scanner = &mut *self.scanner_ptr;
            let end_timestamp = scanner.end_timestamp;
            
            let batch_result = TOKIO_RUNTIME.block_on(async {
                use std::time::Duration;
                scanner.inner.poll(Duration::from_millis(1000)).await
            });
            
            match batch_result {
                Ok(scan_records) => {
                    if scan_records.is_empty() {
                        // Check if we should really finish or if this is just temporary
                        if scanner.has_reached_end() {
                            self.finished = true;
                            Ok(None) // StopIteration
                        } else {
                            // No data right now, but we haven't reached the logical end
                            // Try again after a short delay (handled by poll timeout)
                            self.__next__(py)
                        }
                    } else {
                        // Filter records by end_timestamp if specified
                        let (filtered_records, reached_end) = if let Some(end_ts) = end_timestamp {
                            LogScanner::filter_records_by_timestamp(scan_records, end_ts)
                        } else {
                            (scan_records, false)
                        };
                        
                        // If we reached the end timestamp, mark as finished
                        if reached_end {
                            self.finished = true;
                        }
                        
                        // If no records left after filtering
                        if filtered_records.is_empty() {
                            if self.finished {
                                Ok(None) // StopIteration
                            } else {
                                // Recursively call __next__ to get the next batch
                                self.__next__(py)
                            }
                        } else {
                            // Convert to Arrow batches 
                            // For now this returns a single batch, but could be enhanced to return multiple
                            let batches = Self::convert_to_batches(py, filtered_records, &scanner.table_schema)?;
                            
                            if batches.is_empty() {
                                if self.finished {
                                    Ok(None)
                                } else {
                                    self.__next__(py) // Try again
                                }
                            } else if batches.len() == 1 {
                                // Single batch, return directly
                                Ok(Some(batches.into_iter().next().unwrap()))
                            } else {
                                // Multiple batches, cache them and return the first one
                                self.batch_cache = batches;
                                let first_batch = self.batch_cache.remove(0);
                                Ok(Some(first_batch))
                            }
                        }
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
            batch_cache: Vec::new(),
        })
    }

    // Helper method to convert ScanRecords to multiple Arrow batches
    // Since each ScanRecord contains ColumnarRow with Arc<RecordBatch>, we collect unique batches
    // For now, we'll group records by bucket and convert each bucket to a batch
    fn convert_to_batches(
        py: Python,
        scan_records: fcore::record::ScanRecords,
        schema: &fcore::metadata::Schema,
    ) -> PyResult<Vec<PyObject>> {
        use std::collections::HashMap;
        
        let records_map = scan_records.into_records();
        let mut batches = Vec::new();
        
        // Convert each bucket's records to a separate batch
        for (bucket, records) in records_map {
            if !records.is_empty() {
                let bucket_scan_records = fcore::record::ScanRecords::new(
                    HashMap::from([(bucket, records)])
                );
                let arrow_batch = Utils::convert_scan_records_to_arrow(py, bucket_scan_records, schema)?;
                batches.push(arrow_batch);
            }
        }
        
        if batches.is_empty() {
            // This shouldn't happen if scan_records wasn't empty, but just in case
            return Err(crate::FlussError::new_err("No batches to convert"));
        }
        
        Ok(batches)
    }
}
