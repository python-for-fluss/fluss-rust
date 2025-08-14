# Fluss Python Bindings

Python bindings for Fluss using PyO3 and Maturin.

## System Requirements

- Python 3.8+
- Rust 1.70+
- Maturin (`pip install maturin`)

## Development Environment Setup

### 1. Install Maturin

```bash
pip install maturin
```

### 2. Build Development Version

```bash
cd bindings/python
maturin develop
```

### 3. Build Release Version

```bash
maturin build --release
```

### 4. Run Examples

```bash
python example/example.py
```

## Project Structure

```
bindings/python/
├── Cargo.toml              # Rust dependency configuration
├── pyproject.toml          # Python project configuration
├── README.md              # This file
├── src/                   # Rust source code
│   ├── lib.rs            # Main entry module
│   ├── config.rs         # Configuration related
│   ├── connection.rs     # Connection management
│   ├── admin.rs          # Admin operations
│   ├── table.rs          # Table operations
│   ├── types.rs          # Data types
│   └── error.rs          # Error handling
├── python/               # Python package source
│   └── fluss_python/
│       ├── __init__.py   # Python package entry
│       └── py.typed      # Type declarations
└── example/              # Example code
    └── example.py
```

## API Overview

### Basic Usage

```python
import fluss_python as fluss
import pyarrow as pa
import pandas as pd
import asyncio

async def main():
    # Create connection configuration
    config_spec = {
        "bootstrap.servers": "127.0.0.1:9123",
        "request.max.size": "10485760",  # 10 MB
        "writer.acks": "all",  # Wait for all replicas to acknowledge
        "writer.retries": "3",  # Retry up to 3 times on failure
        "writer.batch.size": "1000",  # Batch size for writes
    }
    config = fluss.Config(config_spec)
    
    # Create connection using the static connect method
    conn = await fluss.FlussConnection.connect(config)

    # Define PyArrow schema
    fields = [
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("score", pa.float32()),
        pa.field("age", pa.int32())
    ]
    schema = pa.schema(fields)

    # Create Fluss Schema and TableDescriptor
    fluss_schema = fluss.Schema(schema)
    table_descriptor = fluss.TableDescriptor(fluss_schema)

    # Get admin client
    admin = await conn.get_admin()

    # Create table path
    table_path = fluss.TablePath("fluss", "sample_table")

    # Create table
    await admin.create_table(table_path, table_descriptor, True)

    # Get table
    table = await conn.get_table(table_path)

    # Write data
    append_writer = await table.new_append_writer()
    
    # Write PyArrow Table
    pa_table = pa.Table.from_arrays([
        pa.array([1, 2, 3], type=pa.int32()),
        pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
        pa.array([95.2, 87.2, 92.1], type=pa.float32()),
        pa.array([25, 30, 35], type=pa.int32())
    ], schema=schema)
    await append_writer.write_arrow(pa_table)
    
    # Write Pandas DataFrame
    df = pd.DataFrame({
        "id": [4, 5],
        "name": ["David", "Eve"],
        "score": [88.5, 91.0],
        "age": [28, 32]
    })
    await append_writer.write_pandas(df)
    
    # Close writer
    await append_writer.close()

    # Read data
    import time
    log_scanner = table.new_log_scanner_sync()
    cur_timestamp = time.time_ns() // 1_000  # current timestamp in microseconds
    await log_scanner.subscribe(None, cur_timestamp)  # Scan from earliest to current
    pa_table_result = log_scanner.to_arrow()
    
    # Close connection
    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
```

### Core Classes

#### `Config`

Configuration for Fluss connection parameters

#### `FlussConnection`

Main interface for connecting to Fluss cluster

#### `FlussAdmin`

Administrative operations for managing tables (create, delete, etc.)

#### `FlussTable`

Represents a Fluss table, providing read and write operations

#### `TableWriter`

Used for writing data to tables, supports PyArrow and Pandas

#### `LogScanner`

Used for scanning table log data

### Testing

```bash
# Build and install to development environment
maturin develop

# Run examples
python example/example.py
```

### Release

```bash
# Build wheel
maturin build --release

# Publish to PyPI
maturin publish
```

## License

Apache 2.0 License
