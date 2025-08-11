pub use ::fluss as fcore;
use pyo3::prelude::*;

mod config;
mod connection;
mod table;
mod admin;
mod types;
mod error;

pub use config::*;
pub use connection::*;
pub use table::*;
pub use admin::*;
pub use types::*;
pub use error::*;

/// A Python module implemented in Rust.
#[pymodule]
fn _fluss_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register types
    m.add_class::<Config>()?;
    m.add_class::<FlussConnection>()?;
    m.add_class::<TablePath>()?;
    m.add_class::<TableInfo>()?;
    m.add_class::<TableDescriptor>()?;
    m.add_class::<FlussAdmin>()?;
    m.add_class::<FlussTable>()?;
    m.add_class::<AppendWriter>()?;
    m.add_class::<LogScanner>()?;
    m.add_class::<ScanResult>()?;
    
    // Register exception types
    m.add("FlussError", m.py().get_type::<FlussError>())?;
    
    Ok(())
}
