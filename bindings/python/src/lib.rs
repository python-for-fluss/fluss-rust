pub use ::fluss as fcore;
use pyo3::prelude::*;
use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

mod config;
mod connection;
mod table;
mod admin;
mod types;
mod error;
mod utils;

pub use config::*;
pub use connection::*;
pub use table::*;
pub use admin::*;
pub use types::*;
pub use error::*;
pub use utils::*;

static TOKIO_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

#[pymodule]
fn fluss_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register all classes
    m.add_class::<Config>()?;
    m.add_class::<FlussConnection>()?;
    m.add_class::<TablePath>()?;
    m.add_class::<TableInfo>()?;
    m.add_class::<TableDescriptor>()?;
    m.add_class::<FlussAdmin>()?;
    m.add_class::<FlussTable>()?;
    m.add_class::<AppendWriter>()?;
    m.add_class::<Schema>()?;
    m.add_class::<LogScanner>()?;
    
    // Register exception types
    // TODO: implement a separate module for exceptions
    // Example implementation:
    // let exception_module = PyModule::new(py, "exceptions")?;
    // exception_module.add("Error", py.get_type::<Error>())?;
    // m.add_submodule(&exception_module)?;
    m.add("FlussError", m.py().get_type::<FlussError>())?;
    
    Ok(())
}
