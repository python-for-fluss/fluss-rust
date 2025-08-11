import fluss_python as fluss
import pyarrow as pa
import pandas as pd
import time

config = fluss.Config("127.0.0.1:9123", 30)
with fluss.FlussConnection(config) as conn:

    # Sample data to insert
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "score": [95.2, 87.2, 92.1]
    }

    # Define fields for PyArrow
    fields = [
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("score", pa.float32())
    ]

    # Create a PyArrow schema
    schema = pa.schema(fields)

	# Create a Fluss TableDescriptor
    table_descriptor = fluss.TableDescriptor(schema)

    # Get the admin for Fluss
    admin = conn.get_admin()

    # Create a Fluss table
    table_path = fluss.TablePath("fluss", "my_table")
    admin.create_table(table_path, table_descriptor, True)

    # Get table information via admin
    table_info = admin.get_table(table_path)
    print(f"Table info: {table_info}")
    print(f"Table ID: {table_info.table_id}")
    print(f"Schema ID: {table_info.schema_id}")
    print(f"Created time: {table_info.created_time}")
    print(f"Primary keys: {table_info.get_primary_keys()}")

    # Get the table instance
    table = conn.get_table(table_path)

    # Create a writer for the table
    table_write = table.new_append()

    # Append the PyArrow Table to the Fluss table
    pa_table = pa.Table.from_arrays(data, schema)
    table_write.write_arrow(pa_table)

    # Append a pandas DataFrame to the Fluss table
    dataframe = pd.DataFrame(data)
    table_write.write_pandas(dataframe)

    # Create a PyArrow RecordBatch
    pa_record_batch = pa.RecordBatch.from_arrays(data, schema)

    # Append the RecordBatch to the Fluss table
    table_write.write_arrow_batch(pa_record_batch)

    table_write.close()

    # Now we can scan the table
    # scan the whole table
    log_scanner = table.new_log_scanner()
    cur_timestamp = time.time_ns() // 1_000  # current timestamp in microseconds
    # scan from the earliest timestamp to the current timestamp
    result = log_scanner.scan_earliest(cur_timestamp)

    # we can iterate over the result
    for row in result:
        print(row)

    # or return a pyarrow Table (all data in memory)
    pa_table = result.to_arrow()
    print(pa_table)

    # or get a pandas DataFrame
    df = result.to_pandas()
    print(df)

    # or resgister it into an in-memory DuckDB table
    duckdb_conn = result.to_duckdb("duck_table")
    # Now we can query the DuckDB table
    print(duckdb_conn.query("SELECT * FROM duck_table WHERE id = 1").fetchdf())
    # Expected output:
    #    id  name  score
    # 0   1  Alice   95.2

# FlussConnection is automatically closed here