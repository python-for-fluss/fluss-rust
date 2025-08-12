use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use crate::*;
use std::sync::Arc;

// Administrative client for managing Fluss tables
#[pyclass]
pub struct FlussAdmin {
    __admin: Arc<fcore::client::FlussAdmin>,
}

#[pymethods]
impl FlussAdmin {
    // Create a table with the given schema
    #[pyo3(signature = (table_path, table_descriptor, ignore_if_exists=None))]
    pub fn create_table<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
        ignore_if_exists: Option<bool>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ignore = ignore_if_exists.unwrap_or(false);
        
        let core_table_path = table_path.to_core().clone();
        let core_descriptor = table_descriptor.to_core().clone();
        let admin = self.__admin.clone();

        future_into_py(py, async move {
            admin.create_table(&core_table_path, &core_descriptor, ignore)
                .await
                .map_err(|e| FlussError::new_err(e.to_string()))?;
        
            Python::with_gil(|py| Ok(py.None()))
        })
    }

    // Get table information
    pub fn get_table<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core_table_path = table_path.to_core().clone();
        let admin = self.__admin.clone();
        
        future_into_py(py, async move {
            let core_table_info = admin.get_table(&core_table_path).await
                .map_err(|e| FlussError::new_err(format!("Failed to get table: {}", e)))?;

            Python::with_gil(|py| {
                let table_info = TableInfo::from_core(core_table_info);
                Py::new(py, table_info)
            })
        })
    }

    fn __repr__(&self) -> String {
        "FlussAdmin()".to_string()
    }
}

impl FlussAdmin {
    // Internal method to create FlussAdmin from core admin
    pub fn from_core(admin: fcore::client::FlussAdmin) -> Self {
        Self {
            __admin: Arc::new(admin),
        }
    }
}
