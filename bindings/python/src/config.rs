use pyo3::prelude::*;
use crate::*;

// Configuration for Fluss client
#[pyclass]
#[derive(Clone)]
pub struct Config {
    inner: fcore::config::Config,
}

#[pymethods]
impl Config {
    // Create a new Config with bootstrap server
    #[new]
    #[pyo3(signature = (bootstrap_server))]
    fn new(bootstrap_server: String) -> Self {
        let mut config = fluss::config::Config::default();
        config.bootstrap_server = Some(bootstrap_server);
        
        Self { inner: config }
    }
    
    // Get the bootstrap server
    #[getter]
    fn bootstrap_server(&self) -> Option<String> {
        self.inner.bootstrap_server.clone()
    }
    
    // Set the bootstrap server
    #[setter]
    fn set_bootstrap_server(&mut self, server: String) {
        self.inner.bootstrap_server = Some(server);
    }
    
    // Get the request max size
    #[getter]
    fn request_max_size(&self) -> i32 {
        self.inner.request_max_size
    }
    
    // Set the request max size
    #[setter]
    fn set_request_max_size(&mut self, size: i32) {
        self.inner.request_max_size = size;
    }
    
    // Get the writer batch size
    #[getter]
    fn writer_batch_size(&self) -> i32 {
        self.inner.writer_batch_size
    }
    
    // Set the writer batch size
    #[setter]
    fn set_writer_batch_size(&mut self, size: i32) {
        self.inner.writer_batch_size = size;
    }
}

impl Config {
    pub fn get_core_config(&self) -> fcore::config::Config {
        self.inner.clone()
    }
}
