import os
import pyarrow as pa
from tqdm import tqdm
import fluss_python as fluss

async def main():
	# Configuration
	config_spec = {
		"bootstrap.servers": "127.0.0.1:9123",
	}

	try:
		# Connect to Fluss
		config = fluss.Config(config_spec)
		conn = await fluss.FlussConnection.connect(config)
		
		# Create schema and table descriptor
		pa_schema = pa.schema([('image', pa.binary())])
		fluss_schema = fluss.Schema(pa_schema)
		table_descriptor = fluss.TableDescriptor(
			fluss_schema,
			properties={
				"table.datalake.enabled": "true",
			}
		)

		# Create table
		admin = await conn.get_admin()
		table_path = fluss.TablePath("fluss", "images_minio_2")
		
		try:
			await admin.create_table(table_path, table_descriptor, True)
			print(f"Created table: {table_path}")
		except Exception as e:
			print(f"Table creation failed: {e}")
		
		# Get table and create writer
		table = await conn.get_table(table_path)
		append_writer = await table.new_append_writer()

		try:
			print("\n--- Writing image data ---")
			for record_batch in process_images(pa_schema):
				append_writer.write_arrow_batch(record_batch)
		except Exception as e:
			print(f"Failed to write image data: {e}")
		finally:
			append_writer.close()
	except Exception as e:
		print(f"Connection error: {e}")
	finally:
		conn.close()

def process_images(schema: pa.Schema):
	# Get the current directory path
	current_dir = os.getcwd()
	images_folder = os.path.join(current_dir, "image")

	# Get the list of image files
	image_files = [filename for filename in os.listdir(images_folder)
		  		 if filename.endswith((".png", ".jpg", ".jpeg"))]

	# Iterate over all images in the folder with tqdm
	for filename in tqdm(image_files, desc="Processing Images"):
		# Construct the full path to the image
		image_path = os.path.join(images_folder, filename)

		# Read and convert the image to a binary format
		with open(image_path, 'rb') as f:
			binary_data = f.read()

		image_array = pa.array([binary_data], type=pa.binary())

		# Yield RecordBatch for each image
		yield pa.RecordBatch.from_arrays([image_array], schema=schema)

if __name__ == "__main__":
	import asyncio
	asyncio.run(main())