import fluss_python as fluss
import pyarrow as pa
import pandas as pd
import time
import asyncio

async def main():
    # Create connection configuration
    config = fluss.Config("127.0.0.1:9123")
    
    # Create connection 
    conn = fluss.FlussConnection(config)

    # Sample data to insert
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "score": [95.2, 87.2, 92.1, 88.5, 91.0],
        "age": [25, 30, 35, 28, 32]
    }

    # Define fields for PyArrow
    fields = [
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("score", pa.float32()),
        pa.field("age", pa.int32())
    ]

    # Create a PyArrow schema
    schema = pa.schema(fields)

    # Create a Fluss TableDescriptor
    table_descriptor = fluss.TableDescriptor(schema)

    # Get the admin for Fluss
    admin = await conn.get_admin()

    # Create a Fluss table
    table_path = fluss.TablePath("test_db", "users_table")
    
    try:
        await admin.create_table(table_path, table_descriptor, True)
        print(f"Created table: {table_path}")
    except Exception as e:
        print(f"Table creation failed or table already exists: {e}")

    # Get table information via admin
    try:
        table_info = await admin.get_table(table_path)
        print(f"Table info: {table_info}")
        print(f"Table ID: {table_info.table_id}")
        print(f"Schema ID: {table_info.schema_id}")
        print(f"Created time: {table_info.created_time}")
        print(f"Primary keys: {table_info.get_primary_keys()}")
    except Exception as e:
        print(f"Failed to get table info: {e}")

    # Get the table instance
    table = await conn.get_table(table_path)
    print(f"Got table: {table}")

    # Create a writer for the table using our implemented interface
    append_writer = await table.new_append_writer()
    print(f"Created append writer: {append_writer}")

    try:
        # Test 1: Write PyArrow Table
        print("\n--- Testing PyArrow Table write ---")
        pa_table = pa.Table.from_arrays([
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
            pa.array([95.2, 87.2, 92.1], type=pa.float32()),
            pa.array([25, 30, 35], type=pa.int32())
        ], schema=schema)
        
        append_writer.write_arrow(pa_table)
        print("Successfully wrote PyArrow Table")

        # Test 2: Write PyArrow RecordBatch
        print("\n--- Testing PyArrow RecordBatch write ---")
        pa_record_batch = pa.RecordBatch.from_arrays([
            pa.array([4, 5], type=pa.int32()),
            pa.array(["David", "Eve"], type=pa.string()),
            pa.array([88.5, 91.0], type=pa.float32()),
            pa.array([28, 32], type=pa.int32())
        ], schema=schema)
        
        append_writer.write_arrow_batch(pa_record_batch)
        print("Successfully wrote PyArrow RecordBatch")

        # Test 3: Write Pandas DataFrame
        print("\n--- Testing Pandas DataFrame write ---")
        df = pd.DataFrame({
            "id": [6, 7],
            "name": ["Frank", "Grace"],
            "score": [89.3, 94.7],
            "age": [29, 27]
        })
        
        append_writer.write_pandas(df)
        print("Successfully wrote Pandas DataFrame")

        # Test 4: Write individual rows using dictionaries
        print("\n--- Testing individual row writes ---")
        rows = [
            {"id": 8, "name": "Henry", "score": 92.8, "age": 31},
            {"id": 9, "name": "Ivy", "score": 87.9, "age": 26},
            {"id": 10, "name": "Jack", "score": 90.5, "age": 33}
        ]
        
        for row in rows:
            append_writer.append_row(row)
        print("Successfully wrote individual rows")

        # Flush all pending data
        print("\n--- Flushing data ---")
        append_writer.flush()
        print("Successfully flushed data")

    except Exception as e:
        print(f"Error during writing: {e}")
    finally:
        # Close the writer
        append_writer.close()
        print("Closed append writer")

    # Now scan the table to verify data was written
    print("\n--- Scanning table ---")
    try:
        log_scanner = await table.new_log_scanner()
        cur_timestamp = time.time_ns() // 1_000  # current timestamp in microseconds
        
        # Scan from the earliest timestamp to the current timestamp
        result = log_scanner.scan_earliest(cur_timestamp)
        
        print("Scanning results:")
        row_count = 0
        for row in result:
            print(f"Row {row_count + 1}: {row}")
            row_count += 1
            if row_count >= 5:  # Limit output to first 5 rows
                print("... (showing first 5 rows)")
                break
        
        # Try to get as PyArrow Table
        try:
            pa_table_result = result.to_arrow()
            print(f"\nAs PyArrow Table: {pa_table_result}")
        except Exception as e:
            print(f"Could not convert to PyArrow: {e}")
        
        # Try to get as Pandas DataFrame
        try:
            df_result = result.to_pandas()
            print(f"\nAs Pandas DataFrame:\n{df_result}")
        except Exception as e:
            print(f"Could not convert to Pandas: {e}")

    except Exception as e:
        print(f"Error during scanning: {e}")

    # Close connection
    conn.close()
    print("\nConnection closed")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
