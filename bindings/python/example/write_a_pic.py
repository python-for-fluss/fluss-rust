import fluss_python as fluss
import pyarrow as pa


async def main():
    config_spec = {
        "bootstrap.servers": "127.0.0.1:9123",
    }

    config = fluss.Config(config_spec)
    conn = await fluss.FlussConnection.connect(config)
    image_path = "/Users/jim/Downloads/colored_logo.png"
    filename = "colored_logo"
    category = "logo"
    data_type = "image/png"
    pa_schema = pa.schema([
        pa.field("image", pa.binary()),
        pa.field("filename", pa.string()),
        pa.field("category", pa.string()),
        pa.field("data_type", pa.string())
    ])
    # Create a Fluss Schema
    fluss_schema = fluss.Schema(pa_schema)
    # Create a Fluss TableDescriptor
    table_descriptor = fluss.TableDescriptor(
        fluss_schema,
        properties={
            "table.datalake.enabled": "true",
        }
    )
    # Get the admin for Fluss
    admin = await conn.get_admin()
    # Create a Fluss table
    table_path = fluss.TablePath("fluss", "images_lance")
    try:
        await admin.create_table(table_path, table_descriptor, True)
        print(f"Created table: {table_path}")
    except Exception as e:
        print(f"Table creation failed: {e}")
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
    # Write the image data to the Fluss table
    table = await conn.get_table(table_path)
    print(f"Got table: {table}")
    # Create a writer for the table
    append_writer = await table.new_append_writer()
    print(f"Created append writer: {append_writer}")
    try:
        print("\n--- Writing image data ---")
        for record_batch in read_image_as_record_batch(image_path, filename, category, data_type):
            append_writer.write_arrow_batch(record_batch)
            print("Successfully wrote image data RecordBatch")
    except Exception as e:
        print(f"Failed to write image data: {e}")
    append_writer.close()
    print("Closed append writer")
    conn.close()
    print("Closed connection")

def read_image_as_record_batch(image_path, filename, category, data_type):
    # Read and convert the image to a binary format
    with open(image_path, 'rb') as f:
        binary_data = f.read()

        image_array = pa.array([binary_data], type=pa.binary())
        filename_array = pa.array([filename], type=pa.string())
        category_array = pa.array([category], type=pa.string())
        data_type_array = pa.array([data_type], type=pa.string())

        # Yield RecordBatch for each image
        yield pa.RecordBatch.from_arrays(
            [image_array, filename_array, category_array, data_type_array],
            schema=pa.schema([
                pa.field("image", pa.binary()),
                pa.field("filename", pa.string()),
                pa.field("category", pa.string()),
                pa.field("data_type", pa.string())
            ])
        )

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())